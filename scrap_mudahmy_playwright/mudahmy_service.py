import os
import time
import random
import logging
import re
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from .database import get_connection
from pathlib import Path
import requests

load_dotenv()


START_DATE = datetime.now().strftime('%Y%m%d')


# ================== Konfigurasi ENV
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP_MUDAH", "url")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY_MUDAH", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE_MUDAH", "price_history_scrap")
DB_TABLE_HISTORY_PRICE_COMBINED = os.getenv("DB_TABLE_HISTORY_PRICE_COMBINED_MUDAH", "price_history_combined")
MUDAHMY_LISTING_URL = os.getenv("MUDAHMY_LISTING_URL", "https://www.mudah.my/malaysia/cars-for-sale")


# ================== Konfigurasi PATH Logging
base_dir = Path(__file__).resolve().parents[1]
log_dir = base_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"scrape_mudahmy_{START_DATE}.log"

# ================== Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def take_screenshot(page, name):
    try:
        # Folder error sesuai TANGGAL sekarang (bisa beda dari START_DATE)
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_mudahmy"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"

        page.screenshot(path=str(screenshot_path))
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")


def should_use_proxy():
    return (
        os.getenv("USE_PROXY_OXYLABS", "false").lower() == "true" and
        os.getenv("PROXY_SERVER") and
        os.getenv("PROXY_USERNAME") and
        os.getenv("PROXY_PASSWORD")
    )


def get_custom_proxy_list():
    raw = os.getenv("CUSTOM_PROXIES_MUDAH", "")
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    parsed = []
    for p in proxies:
        try:
            ip, port, user, pw = p.split(":")
            parsed.append({
                "server": f"{ip}:{port}",
                "username": user,
                "password": pw
            })
        except ValueError:
            continue
    return parsed

class MudahMyService:
    def __init__(self):
        self.stop_flag = False
        self.batch_size = 40
        self.listing_count = 0

        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0

    def init_browser(self):
        self.playwright = sync_playwright().start()
        launch_kwargs = {
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security"
            ]
        }

        proxy_mode = os.getenv("PROXY_MODE_MUDAH", "none").lower()
        if proxy_mode == "oxylabs":
            launch_kwargs["proxy"] = {
                "server": os.getenv("PROXY_SERVER"),
                "username": os.getenv("PROXY_USERNAME"),
                "password": os.getenv("PROXY_PASSWORD")
            }
            logging.info("🌐 Proxy aktif (Oxylabs digunakan)")
        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy custom digunakan (random): {proxy['server']}")
        else:
            logging.info("⚡ Menjalankan browser tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},  # Set to full page size
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur"
        )
        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception as e:
            logging.error(e)
        self.playwright.stop()
        logging.info("🛑 Browser Playwright ditutup.")

    def get_current_ip(self, page, retries=3):
        """Contoh memanggil ip.oxylabs.io untuk cek IP."""
        for attempt in range(1, retries + 1):
            try:
                page.goto('https://ip.oxylabs.io/', timeout=10000)
                ip_text = page.inner_text('body')
                ip = ip_text.strip()
                logging.info(f"IP Saat Ini: {ip}")
                return
            except Exception as e:
                logging.warning(f"Attempt {attempt} gagal mendapatkan IP: {e}")
                # Jika gagal screenshot pun
                take_screenshot(page, "failed_get_ip")
                if attempt == retries:
                    logging.error("Gagal mendapatkan IP setelah beberapa percoaan")
                else:
                    time.sleep(7)

    def scrape_page(self, page, url):
        try:
            self.get_current_ip(page)
            delay = random.uniform(5, 10)
            logging.info(f"Menuju {url} (delay {delay:.1f}s)")
            time.sleep(delay)
            page.goto(url, timeout=60000)

            # Deteksi blokir
            if page.locator("text='Access Denied'").is_visible(timeout=3000):
                raise Exception("Akses ditolak")
            if page.locator("text='Please verify you are human'").is_visible(timeout=3000):
                take_screenshot(page, "captcha_detected")
                raise Exception("Deteksi CAPTCHA")

            page.wait_for_load_state('networkidle', timeout=15000)

            # Ambil semua card container
            card_selector = "div[data-testid^='listing-ad-item-']"
            cards = page.query_selector_all(card_selector)

            urls = []
            for card in cards:
                # Cek apakah ada span 'Today'
                try:
                    today_span = card.query_selector("span:has-text('Today')")
                    if today_span:
                        # Ambil <a href> dari dalam card
                        a_tag = card.query_selector("a[href*='mudah.my']")
                        if a_tag:
                            href = a_tag.get_attribute('href')
                            if href:
                                urls.append(href)
                except Exception as e:
                    logging.warning(f"❌ Error memproses card: {e}")
                    continue

            total_listing = len(set(urls))
            logging.info(f"📄 Ditemukan {total_listing} listing 'Today' di halaman {url}")
            return list(set(urls))

        except Exception as e:
            logging.error(f"Error saat scraping halaman: {e}")
            take_screenshot(page, "error_scrape_page")
            return []

    def download_image(self, url, file_path):
        """Download single image to file_path."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(resp.content)
                logging.info(f"Downloaded: {file_path}")
            else:
                logging.warning(f"Gagal download: {url} - Status: {resp.status_code}")
        except Exception as e:
            logging.error(f"Error download {url}: {str(e)}")

    def download_listing_images(self, listing_url, image_urls):
        """Download all images for a listing into images/{listing_id}/image_{n}.jpg"""
        try:
            path = urlparse(listing_url).path
            listing_id = path.split("-")[-1].replace(".htm", "")
            folder_path = os.path.join("images", listing_id)
            for idx, img_url in enumerate(image_urls):
                clean_url = img_url.split('?')[0]
                if not clean_url.startswith('http'):
                    clean_url = f"https:{clean_url}"
                file_path = os.path.join(folder_path, f"image_{idx+1}.jpg")
                self.download_image(clean_url, file_path)
        except Exception as e:
            logging.error(f"Error download images for {listing_url}: {str(e)}")

    def scrape_listing_detail(self, context, url):
        """Scrape detail listing di tab baru. Kembalikan dict data, atau None kalau gagal."""
        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            page = context.new_page()
            try:
                logging.info(f"Navigating to detail page: {url} (Attempt {attempt+1})")
                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Tunggu galeri muncul sebelum klik apapun
                try:
                    page.wait_for_selector('#ad_view_gallery', timeout=15000)
                    logging.info("Galeri ditemukan, siap klik tombol Show All/gambar utama.")
                except Exception as e:
                    logging.warning(f"Galeri tidak ditemukan dalam 15 detik: {e}")
                    take_screenshot(page, "gallery_not_found")
                    page.close()
                    return None

                # Deteksi blokir/captcha setelah load
                if (
                    "Access Denied" in page.title() or
                    "block" in page.url or
                    page.locator("text='Access Denied'").count() > 0 or
                    page.locator("text='verify you are human'").count() > 0
                ):
                    logging.warning("Blokir atau captcha terdeteksi di halaman detail!")
                    take_screenshot(page, "blocked_or_captcha")
                    page.close()
                    return None

                show_all_clicked = False
                try:
                    # Tunggu dan klik Show All dengan wait_for_selector
                    show_all_button = page.wait_for_selector(
                        "#ad_view_gallery a[data-action-step='17']",
                        timeout=5000
                    )
                    if show_all_button:
                        show_all_button.click()
                        logging.info("Tombol 'Show All' diklik (metode 1)")
                        show_all_clicked = True
                        time.sleep(random.uniform(6, 9)) 
                except Exception as e:
                    logging.info(f"Gagal klik tombol 'Show All' metode 1: {e}")

                if not show_all_clicked:
                    try:
                        show_all_button = page.query_selector("button:has-text('Show All'), a:has-text('Show All')")
                        if show_all_button:
                            show_all_button.scroll_into_view_if_needed()
                            show_all_button.click()
                            logging.info("Tombol 'Show All' diklik (metode 2)")
                            show_all_clicked = True
                            time.sleep(random.uniform(6, 9)) 
                    except Exception as e:
                        logging.info(f"Gagal klik tombol 'Show All' metode 2: {e}")

                if not show_all_clicked:
                    try:
                        main_image_div = page.query_selector("#ad_view_gallery div[data-action-step='1']")
                        if main_image_div:
                            main_image_div.click()
                            logging.info("Gambar utama galeri diklik (metode 3)")
                            time.sleep(random.uniform(6, 9)) 
                    except Exception as e:
                        logging.info(f"Tidak bisa klik gambar utama sebagai fallback: {e}")

                # Scroll ke galeri dan tunggu konten muncul
                try:
                    for _ in range(3):
                        page.mouse.wheel(0, random.randint(500, 1000))
                        time.sleep(1)

                    # Coba beberapa selector yang mungkin untuk galeri gambar
                    gallery_selectors = [
                        "#tabpanel-0",
                        "#ad_view_gallery div[role='tabpanel']",
                        "#ad_view_gallery div.gallery",
                        "div[data-index]"
                    ]
                    
                    gallery_found = False
                    for selector in gallery_selectors:
                        try:
                            page.wait_for_selector(selector, timeout=5000)
                            logging.info(f"Gallery terdeteksi dengan selector: {selector}")
                            gallery_found = True
                            break
                        except Exception:
                            continue

                    if not gallery_found:
                        raise Exception("Tidak ada selector galeri yang cocok")

                    time.sleep(3)

                    # Ambil semua gambar dengan mencari div[data-index]
                    image_urls = set()
                    image_divs = page.query_selector_all("div[data-index]")
                    logging.info(f"Ditemukan {len(image_divs)} div dengan data-index")

                    for div in image_divs:
                        try:
                            img = div.query_selector("img")
                            if img:
                                src = img.get_attribute("src")
                                if src and src.startswith(('http', '//')):
                                    clean_url = src.split('?')[0]
                                    if not clean_url.startswith('http'):
                                        clean_url = f"https:{clean_url}"
                                    image_urls.add(clean_url)
                        except Exception as e:
                            continue

                    if not image_urls:
                        raise Exception("Tidak ada gambar yang ditemukan di galeri")

                    logging.info(f"Total {len(image_urls)} gambar unik ditemukan")

                except Exception as e:
                    logging.warning(f"Gallery tidak dapat diproses: {e}")
                    take_screenshot(page, "gallery_not_found")
                    page.close()
                    return None

                def safe_extract(selectors, selector_type="css", fallback="N/A"):
                    for selector in selectors:
                        try:
                            if selector_type == "css":
                                if page.locator(selector).count() > 0:
                                    return page.locator(selector).first.inner_text().strip()
                            elif selector_type == "xpath":
                                xp = f"xpath={selector}"
                                if page.locator(xp).count() > 0:
                                    return page.locator(xp).first.inner_text().strip()
                        except Exception as e:
                            logging.warning(f"Selector failed: {selector} - {e}")
                    return fallback

                data = {}
                data["listing_url"] = url
                data["brand"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(1) > div:nth-child(3)",
                    "div:has-text('Brand') + div",
                    "//div[contains(text(),'Brand')]/following-sibling::div"
                ])
                data["model"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(2) > div:nth-child(3)",
                    "div:has-text('Model') + div",
                    "//div[contains(text(),'Model')]/following-sibling::div"
                ])
                data["variant"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(4) > div:nth-child(3)",
                    "div:has-text('Variant') + div",
                    "//span[contains(text(),'Variant')]/following-sibling::span"
                ])
                data["information_ads"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div > div",
                    "div.ad-highlight:first-child",
                    "//div[contains(@class,'ad-highlight')][1]"
                ])
                data["location"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(4) > div",
                    "div:has-text('Location') + div",
                    "//div[contains(text(),'Location')]/following-sibling::div"
                ])
                data["price"] = safe_extract([
                    "div.flex.gap-1.md\\:items-end > div"
                ])
                data["year"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Year') + div",
                    "//div[contains(text(),'Year')]/following-sibling::div"
                ])
                data["mileage"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(3) > div",
                    "div:has-text('Mileage') + div",
                    "//div[contains(text(),'Mileage')]"
                ])
                data["transmission"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(2) > div",
                    "div:has-text('Transmission') + div",
                    "//div[contains(text(),'Transmission')]"
                ])
                data["seat_capacity"] = safe_extract([
                    "#ad_view_car_specifications > div > div > div > div > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Seat Capacity') + div",
                    "//div[contains(text(),'Seat') and contains(text(),'Capacity')]"
                ])
                
                # Gunakan image_urls yang sudah berisi semua gambar
                data["gambar"] = list(image_urls)
                data["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Tambahan: download gambar
                if image_urls:  # Gunakan image_urls langsung
                    self.download_listing_images(url, image_urls)

                page.close()
                return data

            except Exception as e:
                logging.error(f"Scraping detail failed: {e}")
                take_screenshot(page, f"error_scrape_detail")
                attempt += 1
                page.close()
                if attempt < max_retries:
                    logging.warning(f"Mencoba ulang detail scraping untuk {url} (Attempt {attempt+1})...")
                    time.sleep(random.uniform(15, 20))
                else:
                    logging.warning(f"Gagal mengambil detail untuk URL: {url}")
                    return None

    def scrape_listings_for_brand(self, base_url, brand_name, model_name, start_page=1, descending=False):
        total_scraped = 0
        current_page = start_page
        self.init_browser()
        try:
            while current_page > 0:  # Ubah kondisi untuk mendukung descending
                if self.stop_flag:
                    logging.info("Stop flag terdeteksi, menghentikan scraping brand ini.")
                    break

                current_url = f"{base_url}?o={current_page}"
                logging.info(f"Scraping halaman {current_page}: {current_url}")
                listing_urls = self.scrape_page(self.page, current_url)

                if not listing_urls:
                    logging.info("Tidak ada listing URL ditemukan, pindah ke brand/model berikutnya.")
                    break

                for url in listing_urls:
                    if self.stop_flag:
                        break

                    # Ganti: kirim self.context, bukan self.page
                    detail_data = self.scrape_listing_detail(self.context, url)
                    if detail_data:
                        max_db_retries = 3
                        for attempt in range(1, max_db_retries + 1):
                            try:
                                self.save_to_db(detail_data)
                                break
                            except Exception as e:
                                logging.warning(f"⚠️ Attempt {attempt} gagal simpan data untuk {url}: {e}")
                                if attempt == max_db_retries:
                                    logging.error(f"❌ Gagal simpan data setelah {max_db_retries} percobaan: {url}")
                                else:
                                    time.sleep(20)
                        total_scraped += 1
                    else:
                        logging.warning(f"Gagal mengambil detail untuk URL: {url}")

                    delay = random.uniform(15, 35)
                    logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                    time.sleep(delay)

                # Re-init browser sebelum halaman berikutnya
                self.quit_browser()
                time.sleep(3)
                self.init_browser()

                # Update current_page berdasarkan mode descending
                if descending:
                    current_page -= 1
                else:
                    current_page += 1

                delay = random.uniform(300, 600)  # 5-10 menit
                logging.info(f"Menunggu {delay:.1f} detik sebelum halaman {'sebelumnya' if descending else 'berikutnya'}...")
                time.sleep(delay)

            logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")
        finally:
            self.quit_browser()
        return total_scraped, False

    def scrape_all_brands(self, brand=None, model=None, start_page=1):
        """
        Baca CSV:
          - Jika brand dan model diberikan, mulai scraping dari baris yang cocok,
            lalu lanjut ke seluruh baris berikutnya.
          - Jika tidak diberikan, scraping semua brand+model (dari baris pertama).
        """
        self.reset_scraping()
        df = pd.read_csv(INPUT_FILE)

        if brand and model:
            df = df.reset_index(drop=True)
            matching_rows = df[
                (df['brand'].str.lower() == brand.lower()) &
                (df['model'].str.lower() == model.lower())
            ]
            if matching_rows.empty:
                logging.warning("Brand dan model tidak ditemukan dalam CSV.")
                return

            start_index = matching_rows.index[0]
            logging.info(f"Mulai scraping dari baris {start_index} untuk brand={brand}, model={model} (start_page={start_page}).")

            for i in range(start_index, len(df)):
                row = df.iloc[i]
                brand_name = row['brand']
                model_name = row['model']
                base_url = row['url']

                if i == start_index:
                    current_page = start_page
                else:
                    current_page = 1

                logging.info(f"Mulai scraping brand: {brand_name}, model: {model_name}, start_page={current_page}")
                total_scraped, _ = self.scrape_listings_for_brand(base_url, brand_name, model_name, current_page)
                logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")
        else:
            logging.info("Mulai scraping dari baris pertama (tidak ada filter brand/model).")
            df = df.reset_index(drop=True)
            for i, row in df.iterrows():
                brand_name = row['brand']
                model_name = row['model']
                base_url = row['url']

                logging.info(f"Mulai scraping brand: {brand_name}, model: {model_name}, start_page=1")
                total_scraped, _ = self.scrape_listings_for_brand(base_url, brand_name, model_name, 1)
                logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")

        logging.info("Proses scraping selesai untuk filter brand/model.")

    def scrape_all_from_main(self, start_page=1, descending=False):
        """
        Scrape semua listing dari halaman utama mudah.my (tanpa filter brand/model, langsung dari ENV MUDAHMY_LISTING_URL)
        
        Args:
            start_page (int): Halaman awal untuk memulai scraping
            descending (bool): Jika True, scraping dilakukan dari nomor halaman besar ke kecil
        """
        self.reset_scraping()
        self.init_browser()
        try:
            current_page = start_page
            total_scraped = 0
            while current_page > 0:  # Ubah kondisi untuk mendukung descending
                if self.stop_flag:
                    logging.info("Stop flag terdeteksi, menghentikan scraping.")
                    break

                current_url = f"{MUDAHMY_LISTING_URL}?o={current_page}"
                logging.info(f"Scraping halaman {current_page}: {current_url}")
                listing_urls = self.scrape_page(self.page, current_url)

                if not listing_urls:
                    logging.info("Tidak ada listing URL ditemukan, selesai.")
                    break

                for url in listing_urls:
                    if self.stop_flag:
                        break
                    detail_data = self.scrape_listing_detail(self.context, url)
                    if detail_data:
                        max_db_retries = 3
                        for attempt in range(1, max_db_retries + 1):
                            try:
                                self.save_to_db(detail_data)
                                break
                            except Exception as e:
                                logging.warning(f"⚠️ Attempt {attempt} gagal simpan data untuk {url}: {e}")
                                if attempt == max_db_retries:
                                    logging.error(f"❌ Gagal simpan data setelah {max_db_retries} percobaan: {url}")
                                else:
                                    time.sleep(20)
                        total_scraped += 1
                    delay = random.uniform(15, 35)
                    logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                    time.sleep(delay)

                # Re-init browser sebelum halaman berikutnya
                self.quit_browser()
                time.sleep(3)
                self.init_browser()

                # Update current_page berdasarkan mode descending
                if descending:
                    current_page -= 1
                else:
                    current_page += 1

                delay = random.uniform(300, 600)  # 5-10 menit
                logging.info(f"Menunggu {delay:.1f} detik sebelum halaman {'sebelumnya' if descending else 'berikutnya'}...")
                time.sleep(delay)

            logging.info(f"Selesai scraping semua listing dari halaman utama. Total data: {total_scraped}")
        finally:
            self.quit_browser()
        return total_scraped

    def stop_scraping(self):
        logging.info("Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("Scraping direset.")

    def save_to_db(self, car_data):
        """
        Simpan atau update data mobil ke database.
        """
        try:
            self.cursor.execute(
                f"SELECT id, price, version FROM {DB_TABLE_SCRAP} WHERE listing_url = %s",
                (car_data["listing_url"],)
            )
            row = self.cursor.fetchone()

            # Normalisasi price -> integer
            price_int = 0
            if car_data.get("price"):
                match_price = re.sub(r"[^\d]", "", car_data["price"])  # buang non-digit
                price_int = int(match_price) if match_price else 0

            # Normalisasi year -> integer
            year_int = 0
            if car_data.get("year"):
                match_year = re.search(r"(\d{4})", car_data["year"])
                if match_year:
                    year_int = int(match_year.group(1))

            # Pisahkan information_ads menjadi condition dan information_ads
            condition = "N/A"
            info_ads = car_data.get("information_ads", "")
            if info_ads:
                parts = info_ads.split(",", 1)
                if len(parts) > 1:
                    condition = parts[0].strip()
                    info_ads = parts[1].strip()
                else:
                    info_ads = parts[0].strip()

            if row:
                car_id, old_price, current_version = row
                old_price = old_price if old_price else 0
                current_version = current_version if current_version else 0
                new_price = price_int

                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model=%s, variant=%s,
                        information_ads=%s, location=%s,
                        price=%s, year=%s, mileage=%s,
                        transmission=%s, seat_capacity=%s,
                        gambar=%s, last_scraped_at=%s,
                        version=%s, condition=%s
                    WHERE id=%s
                """

                new_version = current_version + 1
                self.cursor.execute(update_query, (
                    car_data.get("brand"),
                    car_data.get("model"),
                    car_data.get("variant"),
                    info_ads,
                    car_data.get("location"),
                    new_price,
                    year_int,
                    car_data.get("mileage"),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("gambar"),
                    datetime.now(),
                    new_version,
                    condition,
                    car_id
                ))

                # Jika harga berubah, catat di history
                if new_price != old_price and old_price != 0:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_id, old_price, new_price))

            else:
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                        (listing_url, brand, model, variant, information_ads, location,
                         price, year, mileage, transmission, seat_capacity, gambar, version, condition)
                    VALUES
                        (%s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, (
                    car_data["listing_url"],
                    car_data.get("brand"),
                    car_data.get("model"),
                    car_data.get("variant"),
                    info_ads,
                    car_data.get("location"),
                    price_int,
                    year_int,
                    car_data.get("mileage"),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("gambar"),
                    1,
                    condition
                ))

            self.conn.commit()
            logging.info(f"✅ Data untuk listing_url={car_data['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def sync_to_cars(self):
        """
        Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}, dan sinkronisasi data perubahan harga dari price_history_scrap ke price_history_combined.
        """
        logging.info(f"Memulai sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}...")
        try:
            fetch_query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(fetch_query)
            rows = self.cursor.fetchall()
            col_names = [desc[0] for desc in self.cursor.description]
            idx_url = col_names.index("listing_url")

            for row in rows:
                listing_url = row[idx_url]
                check_query = f"SELECT id FROM {DB_TABLE_PRIMARY} WHERE listing_url = %s"
                self.cursor.execute(check_query, (listing_url,))
                result = self.cursor.fetchone()

                if result:
                    update_query = f"""
                        UPDATE {DB_TABLE_PRIMARY}
                        SET brand=%s, model=%s, variant=%s, information_ads=%s,
                            location=%s, price=%s, year=%s, mileage=%s, transmission=%s,
                            seat_capacity=%s, gambar=%s, last_scraped_at=%s, condition=%s
                        WHERE listing_url=%s
                    """
                    self.cursor.execute(update_query, (
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("information_ads")],
                        row[col_names.index("location")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("mileage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("gambar")],
                        row[col_names.index("last_scraped_at")],
                        row[col_names.index("condition")],
                        listing_url
                    ))
                else:
                    insert_query = f"""
                        INSERT INTO {DB_TABLE_PRIMARY}
                            (listing_url, brand, model, variant, information_ads, location,
                             price, year, mileage, transmission, seat_capacity, gambar, 
                             last_scraped_at, condition)
                        VALUES
                            (%s, %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        listing_url,
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("information_ads")],
                        row[col_names.index("location")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("mileage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("gambar")],
                        row[col_names.index("last_scraped_at")],
                        row[col_names.index("condition")]
                    ))

            # Sinkronisasi perubahan harga dari price_history_scrap ke price_history_combined
            sync_price_history_query = f"""
                INSERT INTO {DB_TABLE_HISTORY_PRICE_COMBINED} (car_id, car_scrap_id, old_price, new_price, changed_at)
                SELECT c.id, cs.id, phs.old_price, phs.new_price, phs.changed_at
                FROM {DB_TABLE_HISTORY_PRICE} phs
                JOIN {DB_TABLE_SCRAP} cs ON phs.car_id = cs.id
                JOIN {DB_TABLE_PRIMARY} c ON cs.listing_url = c.listing_url
                WHERE phs.car_id IS NOT NULL;
            """
            self.cursor.execute(sync_price_history_query)

            # Commit perubahan ke database
            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
            logging.info("Sinkronisasi perubahan harga dari price_history_scrap ke price_history_combined selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error saat sinkronisasi data: {e}")

    def export_data(self):
        """
        Mengambil data dari DB_TABLE_SCRAP dalam bentuk list of dict
        """
        try:
            query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            return data
        except Exception as e:
            logging.error(f"Error export data: {e}")
            return []

    def close(self):
        """Tutup browser dan koneksi database."""
        try:
            self.quit_browser()
        except Exception:
            pass
        try:
            self.cursor.close()
            self.conn.close()
            logging.info("Koneksi database ditutup, browser ditutup.")
        except Exception as e:
            logging.error(e)