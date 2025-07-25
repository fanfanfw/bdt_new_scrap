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
import json

load_dotenv(override=True)

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

log_file = log_dir / f"null_scrape_mudahmy_{START_DATE}.log"

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

class MudahMyNullService:
    def __init__(self, download_images_locally=True):
        self.stop_flag = False
        self.batch_size = 40
        self.listing_count = 0
        self.last_scraped_data = {} 
        self.download_images_locally = download_images_locally
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0
        
        # Setup image storage path
        self.image_base_path = os.path.join(base_dir, "images_mudah")
        os.makedirs(self.image_base_path, exist_ok=True)
        logging.info(f"Image base path: {self.image_base_path}")

    def init_browser(self):
        self.playwright = sync_playwright().start()
        launch_kwargs = {
            "headless": True,
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
            if hasattr(self, "browser"):
                self.browser.close()
        except Exception as e:
            logging.error(e)
        if hasattr(self, "playwright"):
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

    def normalize_model_variant(self, text):
        """
        Membersihkan string model/variant:
        - Menghilangkan karakter aneh: -, (, ), _
        - Mengganti underscore menjadi spasi
        - Merapikan spasi
        - Ubah ke UPPERCASE
        - Jika kosong atau "-" atau "N/A", return "NO VARIANT"
        """
        if not text or str(text).strip() in ["N/A", "-", ""]:
            return "NO VARIANT"

        cleaned = re.sub(r'[\-\(\)_]', ' ', text)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.upper()

        # Jika hasil akhirnya kosong, return NO VARIANT
        if not cleaned:
            return "NO VARIANT"
        return cleaned


    def insert_new_listing(self, listing_url, price):
        """Insert listing_url baru ke database dengan status active dan price dari halaman utama."""
        try:
            insert_query = f"""
                INSERT INTO {DB_TABLE_SCRAP} 
                (listing_url, price, status, created_at) 
                VALUES (%s, %s, 'active', NOW())
                ON CONFLICT (listing_url) DO NOTHING
                RETURNING id
            """
            self.cursor.execute(insert_query, (listing_url, price))
            self.conn.commit()
            result = self.cursor.fetchone()
            if result:
                logging.info(f"✅ Listing baru {listing_url} berhasil ditambahkan ke database dengan price {price}")
                return True
            return False
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error saat menambahkan listing baru: {e}")
            return False
        
    def scrape_null_entries(self, id_min=None, id_max=None, include_urgent=False):
        """
        Scrape ulang listing yang kolom brand/model/variant/information_ads/location masih NULL
        ATAU field condition=URGENT, bisa difilter dengan range id.
        """
        filter_null = "(brand IS NULL OR model IS NULL OR variant IS NULL OR information_ads IS NULL OR location IS NULL)"
        if include_urgent:
            filter_main = f"({filter_null} OR condition = 'URGENT')"
        else:
            filter_main = filter_null

        # Tambahkan filter id
        filter_params = []
        if id_min is not None:
            filter_main += " AND id >= %s"
            filter_params.append(id_min)
        if id_max is not None:
            filter_main += " AND id <= %s"
            filter_params.append(id_max)

        query = f"SELECT id, listing_url, status FROM {DB_TABLE_SCRAP} WHERE {filter_main}"
        self.cursor.execute(query, tuple(filter_params))
        rows = self.cursor.fetchall()
        urls = [(r[0], r[1], r[2]) for r in rows if r[1]]  # Tambahkan status listing
        logging.info(f"Total filtered listings found: {len(urls)} (filters: id_min={id_min}, id_max={id_max}, urgent={include_urgent})")

        for idx, (listing_id, url, status) in enumerate(urls):  # Mengambil status di sini
            if status == "sold":  # Jika sudah sold, skip
                logging.info(f"✅ Listing ID={listing_id} sudah SOLD, melewatkan pengecekan.")
                continue 

            attempt = 0
            success = False
            last_error = None
            while attempt < 3 and not success:
                try:
                    self.init_browser()
                    self.page.goto(url, wait_until="domcontentloaded", timeout=30000)  # Navigasi ke halaman
                    # Cek apakah URL di-redirect ke halaman daftar kendaraan (sold)
                    current_url = self.page.url
                    if "/malaysia/cars-for-sale" in current_url:
                        logging.info(f"🔴 Listing {url} terdeteksi SOLD (redirect ke /malaysia/cars-for-sale)")
                        # Update status menjadi "sold"
                        self.cursor.execute(f"""
                            UPDATE {DB_TABLE_SCRAP}
                            SET status = 'sold', sold_at = NOW()
                            WHERE id = %s
                        """, (listing_id,))
                        self.conn.commit()
                        self.quit_browser()
                        break  # Skip ke listing berikutnya``

                    # Jika halaman valid, lanjutkan scraping
                    detail_data = self.scrape_listing_detail(self.context, url)
                    if detail_data:
                        _, car_id = self.save_to_db(detail_data)
                        if self.download_images_locally and car_id is not None:
                            self.download_listing_images(url, detail_data.get('images', []), car_id)
                        success = True
                    self.quit_browser()
                except Exception as e:
                    last_error = e
                    logging.error(f"Failed scraping {url} (Attempt {attempt+1}/3): {e}")
                    take_screenshot(self.page, f"scrape_error_{idx}_{attempt+1}")
                    self.quit_browser()
                    attempt += 1
                    time.sleep(random.uniform(5, 15))
            if not success:
                logging.error(f"❌ Gagal scraping {url} setelah 3 percobaan. Error terakhir: {last_error}")
            time.sleep(random.uniform(15, 25))

    def download_image(self, url, file_path):
        """Download single image to file_path."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True, mode=0o755)
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(resp.content)
                # Set file permissions
                os.chmod(file_path, 0o644)
                logging.info(f"Downloaded: {file_path}")
            else:
                logging.warning(f"Gagal download: {url} - Status: {resp.status_code}")
        except PermissionError as e:
            logging.error(f"Permission error saat menyimpan file: {e}")
        except Exception as e:
            logging.error(f"Error download {url}: {str(e)}")

    def download_listing_images(self, listing_url, image_urls, car_id):
        """Download all images for a listing into images_mudah/brand/model/variant/db_id/image_{n}.jpg"""
        try:
            # Clean brand, model and variant names untuk nama folder yang aman
            def clean_filename(name):
                # Hapus karakter yang tidak diinginkan, ganti dengan underscore
                return re.sub(r'[<>:"/\\|?*]', '_', str(name).strip())
            
            brand = clean_filename(self.last_scraped_data.get("brand", "unknown"))
            model = clean_filename(self.last_scraped_data.get("model", "unknown"))
            variant = clean_filename(self.last_scraped_data.get("variant", "unknown"))
            
            # Gunakan path absolut dari self.image_base_path
            folder_path = os.path.join(self.image_base_path, brand, model, variant, str(car_id))
            # Buat folder dengan permission yang benar
            os.makedirs(folder_path, exist_ok=True, mode=0o755)
            
            # Download setiap gambar
            for idx, img_url in enumerate(image_urls):
                clean_url = img_url.split('?')[0]
                if not clean_url.startswith('http'):
                    clean_url = f"https:{clean_url}"
                file_path = os.path.join(folder_path, f"image_{idx+1}.jpg")
                self.download_image(clean_url, file_path)
                
            logging.info(f"Gambar disimpan di folder: {folder_path}")
        except Exception as e:
            logging.error(f"Error download images for listing ID {car_id}: {str(e)}")
            
    def get_highlight_info(self, page):
        try:
            parent = page.query_selector('#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div')
            if not parent:
                return None
            children = parent.query_selector_all('div')
            if len(children) == 2:
                return children[1].inner_text().strip()
            elif len(children) == 1:
                return children[0].inner_text().strip()
            else:
                return parent.inner_text().strip()
        except Exception as e:
            logging.warning(f"❌ Gagal ekstrak highlight info: {e}")
            return None

    def scrape_listing_detail(self, context, url):
        """Scrape detail listing di tab baru. Kembalikan dict data, atau None kalau gagal."""
        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            page = context.new_page()
            try:
                logging.info(f"Navigating to detail page: {url} (Attempt {attempt+1})")
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # Check for blocks/captcha
                if (
                    "Access Denied" in page.title() or
                    "block" in page.url or
                    page.locator("text='Access Denied'").count() > 0 or
                    page.locator("text='verify you are human'").count() > 0
                ):
                    logging.warning("Blokir atau captcha terdeteksi di halaman detail!")
                    attempt += 1
                    page.close()
                    return None

                try:
                    page.wait_for_selector('#ad_view_car_specifications', timeout=15000)
                    time.sleep(3)  # Wait for animations
                except Exception as e:
                    logging.warning(f"Specifications section tidak ditemukan: {e}")
                    attempt += 1
                    page.close()
                    continue

                show_more_clicked = False
                
                try:
                    show_more_btn = page.wait_for_selector(
                        "#ad_view_car_specifications button:has-text('SHOW MORE')", 
                        timeout=5000,
                        state="visible"
                    )
                    if show_more_btn:
                        show_more_btn.scroll_into_view_if_needed()
                        show_more_btn.click()
                        time.sleep(3)  
                        
                        if page.locator("button:has-text('SHOW LESS')").count() > 0:
                            show_more_clicked = True
                            logging.info("Tombol 'SHOW MORE' specifications diklik (metode 1)")
                            time.sleep(2)
                except Exception as e:
                    logging.info("Metode 1 gagal: mencoba metode berikutnya")

                if not show_more_clicked:
                    try:
                        page.evaluate("""
                            const btn = document.querySelector('#ad_view_car_specifications button');
                            if (btn && btn.innerText.includes('SHOW MORE')) {
                                btn.click();
                            }
                        """)
                        time.sleep(3)
                        if page.locator("button:has-text('SHOW LESS')").count() > 0:
                            show_more_clicked = True
                            logging.info("Tombol 'SHOW MORE' specifications diklik via JavaScript")
                    except Exception as e:
                        logging.info("Metode 2 gagal: mencoba metode final")

                if not show_more_clicked:
                    try:
                        page.evaluate("""
                            const specDiv = document.querySelector('#ad_view_car_specifications');
                            if (btn && btn.innerText.includes('SHOW MORE')) {
                                btn.click();
                            }
                        """)
                        time.sleep(3)
                        if page.locator("button:has-text('SHOW LESS')").count() > 0:
                            show_more_clicked = True
                            logging.info("Tombol 'SHOW MORE' specifications diklik via JavaScript")
                    except Exception as e:
                        logging.info("Metode 2 gagal: mencoba metode final")

                if not show_more_clicked:
                    try:
                        page.evaluate("""
                            const specDiv = document.querySelector('#ad_view_car_specifications');
                            if (specDiv) {
                                const btn = specDiv.querySelector('button');
                                if (btn) {
                                    btn.setAttribute('data-expanded', 'true');
                                    btn.innerHTML = 'SHOW LESS<svg viewBox="0 0 24 24" style="width:1.25rem;height:1.25rem" role="presentation"><path d="M7.41,15.41L12,10.83L16.59,15.41L18,14L12,8L6,14L7.41,15.41Z" style="fill:currentColor"></path></svg>';
                                }
                                const contentDivs = specDiv.querySelectorAll('div[style*="display: none"]');
                                contentDivs.forEach(div => div.style.display = 'block');
                            }
                        """)
                        show_more_clicked = True
                        logging.info("Specifications diperluas via DOM manipulation")
                    except Exception as e:
                        logging.info("Semua metode gagal expand specifications")

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
                            logging.error(f"Error extracting selector: {e}")
                    return fallback

                data = {}
                data["listing_url"] = url
                data["brand"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(1) > div:nth-child(3)",
                    "div:has-text('Brand') + div",
                ])
                data["model"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(2) > div:nth-child(3)",
                    "div:has-text('Model') + div",
                ])
                data["variant"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(4) > div:nth-child(3)",
                    "div:has-text('Variant') + div",
                ])
                data["engine_cc"] = safe_extract([
                    "#ad_view_car_specifications > div > div > div:nth-child(2) > div > div > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)",
                    "div:has-text('Engine CC') + div",
                ])
                # Ambil informasi lengkap dari highlight (hanya dari elemen yang benar)
                full_info = self.get_highlight_info(page)
                if not full_info:
                    full_info = safe_extract([
                        "#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div > div",
                        "div.text-\[\#666666\].text-xs.lg\\:text-base",
                        "//*[@id='ad_view_ad_highlights']/div/div/div[1]/div/div/div"
                    ])
                if full_info and full_info != "N/A":
                    parts = full_info.split(",", 1)
                    data["condition"] = parts[0].strip()
                    data["information_ads"] = parts[1].strip() if len(parts) > 1 else ""
                else:
                    data["condition"] = "N/A"
                    data["information_ads"] = ""
                
                logging.info(f"Extracted condition: {data['condition']}")
                logging.info(f"Extracted information_ads: {data['information_ads']}")

                data["location"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(4) > div",
                    "div.font-bold.truncate.text-sm.md\\:text-base",
                    "//*[@id='ad_view_ad_highlights']/div/div/div[3]/div[4]/div",
                    "#ad_view_ad_highlights div.font-bold.truncate",
                ], selector_type="css")
                data["price"] = safe_extract([
                    "div.flex.gap-1.md\\:items-end > div"
                ])
                data["year"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Year') + div",
                ])
                data["mileage"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(3) > div",
                    "div:has-text('Mileage') + div",
                ])
                data["transmission"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(2) > div",
                    "div:has-text('Transmission') + div",
                ])
                data["seat_capacity"] = safe_extract([
                    "#ad_view_car_specifications > div > div > div > div > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Seat Capacity') + div",
                ])
                data["series"] = safe_extract([
                    "#ad_view_car_specifications div.flex.flex-col.gap-4 div:has-text('Series') + div",
                    "div:has-text('Series') + div",
                ])
                data["type"] = safe_extract([
                    "#ad_view_car_specifications div.flex.flex-col.gap-4 div:has-text('Type') + div",
                    "div:has-text('Type') + div",
                ])
                data["fuel_type"] = safe_extract([
                    "#ad_view_car_specifications > div > div > div:nth-child(1) > div > div > div:nth-child(2) > div:nth-child(4) > div:nth-child(3)",
                    "#ad_view_car_specifications div.flex.flex-col.gap-4 div:has-text('Fuel Type') + div",
                    "div:has-text('Fuel Type') + div",
                ])

                data["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Simpan ke last_scraped_data untuk digunakan saat download gambar
                self.last_scraped_data = data

                success, car_id = self.save_to_db(data)
                if car_id is None:
                    logging.error("Gagal menyimpan data ke database")
                    page.close()
                    return None

                try:
                    page.wait_for_selector('#ad_view_gallery', timeout=15000)
                    logging.info("Galeri ditemukan, siap proses gambar")

                    # Proses Show All gallery
                    show_all_clicked = False
                    try:
                        show_all_button = page.wait_for_selector(
                            "#ad_view_gallery a[data-action-step='17']",
                            timeout=5000
                        )
                        if show_all_button:
                            show_all_button.click()
                            logging.info("Tombol 'Show All' gallery diklik (metode 1)")
                            show_all_clicked = True
                            time.sleep(random.uniform(6, 9))
                    except Exception as e:
                        logging.info(f"Gagal klik tombol 'Show All' gallery metode 1: {e}")

                    if not show_all_clicked:
                        try:
                            show_all_button = page.query_selector("button:has-text('Show All'), a:has-text('Show All')")
                            if show_all_button:
                                show_all_button.scroll_into_view_if_needed()
                                show_all_button.click()
                                logging.info("Tombol 'Show All' gallery diklik (metode 2)")
                                show_all_clicked = True
                                time.sleep(random.uniform(6, 9))
                        except Exception as e:
                            logging.info(f"Gagal klik tombol 'Show All' gallery metode 2: {e}")

                    if not show_all_clicked:
                        try:
                            main_image_div = page.query_selector("#ad_view_gallery div[data-action-step='1']")
                            if main_image_div:
                                main_image_div.click()
                                logging.info("Gambar utama galeri diklik (metode 3)")
                                time.sleep(random.uniform(6, 9))
                        except Exception as e:
                            logging.info(f"Tidak bisa klik gambar utama sebagai fallback: {e}")

                    # Proses ambil URL gambar
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
                        except Exception:
                            continue

                    if image_urls:
                        # Update data dengan URL gambar dan download
                        data["images"] = list(image_urls)
                        
                        # Update images di database
                        update_images_query = f"""
                            UPDATE {DB_TABLE_SCRAP}
                            SET images = %s
                            WHERE id = %s
                        """
                        self.cursor.execute(update_images_query, (json.dumps(list(image_urls)), car_id))
                        self.conn.commit()
                        logging.info(f"✅ URL gambar berhasil diupdate untuk listing ID: {car_id}")
                    else:
                        logging.warning(f"Tidak ada gambar ditemukan untuk listing {url}")

                except Exception as e:
                    logging.warning(f"Gagal memproses galeri: {e}")
                    take_screenshot(page, "gallery_error")

                page.close()
                return data

            except Exception as e:
                logging.error(f"Scraping detail failed: {e}")
                attempt += 1
                page.close()
                if attempt < max_retries:
                    logging.warning(f"Mencoba ulang detail scraping untuk {url} (Attempt {attempt+1})...")
                    time.sleep(random.uniform(15, 20))
                else:
                    logging.warning(f"Gagal mengambil detail untuk URL: {url}")
                    return None

    def stop_scraping(self):
        logging.info("Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("Scraping direset.")

    def normalize_brand_name(self, brand_str):
        """
        Normalisasi nama brand dengan mengganti tanda '-' menjadi spasi
        dan membersihkan format lainnya.
        """
        if not brand_str or brand_str == "N/A":
            return brand_str
        
        # Ganti tanda '-' dengan spasi
        normalized = brand_str.replace('-', ' ')
        
        # Bersihkan spasi berlebih dan uppercase
        normalized = ' '.join(normalized.split()).upper()
        
        return normalized

    def save_to_db(self, car_data):
        try:
            # Normalisasi brand name sebelum menyimpan
            original_brand = car_data.get("brand")
            normalized_brand = self.normalize_brand_name(original_brand)
            normalized_model = self.normalize_model_variant(car_data.get("model"))
            normalized_variant = self.normalize_model_variant(car_data.get("variant"))
            
            if original_brand != normalized_brand:
                logging.info(f"Brand dinormalisasi dari '{original_brand}' menjadi '{normalized_brand}'")
            
            # Cek apakah listing_url sudah ada di database
            self.cursor.execute(
                f"SELECT id, price FROM {DB_TABLE_SCRAP} WHERE listing_url = %s",
                (car_data["listing_url"],)
            )
            row = self.cursor.fetchone()

            # Jika data sudah ada, cek harga
            price_int = 0
            if car_data.get("price"):
                match_price = re.sub(r"[^\d]", "", car_data["price"])
                price_int = int(match_price) if match_price else 0

            if row:
                car_id, old_price = row
                old_price = old_price if old_price else 0

                # Cek apakah ada field penting yang masih NULL di database
                self.cursor.execute(
                    f"SELECT brand, model, variant, information_ads, location, year, mileage, transmission, seat_capacity, condition, engine_cc, fuel_type FROM {DB_TABLE_SCRAP} WHERE id = %s",
                    (car_id,)
                )
                db_fields = self.cursor.fetchone()
                needs_update = any(x is None for x in db_fields)
                self.cursor.execute(
                    f"SELECT condition FROM {DB_TABLE_SCRAP} WHERE id = %s",
                    (car_id,)
                )
                cur_condition = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else None

                if old_price == price_int and not needs_update and cur_condition != "URGENT":
                    logging.info(f"✅ Harga dan data sudah lengkap dan bukan URGENT, melewatkan scraping.")
                    return False, car_id
                
                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model=%s, variant=%s,
                        information_ads=%s, location=%s,
                        price=%s, year=%s, mileage=%s,
                        transmission=%s, seat_capacity=%s,
                        last_scraped_at=%s, last_status_check=%s, condition=%s, engine_cc=%s,
                        fuel_type=%s, images=%s
                    WHERE id=%s
                """
                now_dt = datetime.now()
                self.cursor.execute(update_query, (
                    normalized_brand,
                    normalized_model,
                    normalized_variant,
                    car_data.get("information_ads"),
                    car_data.get("location"),
                    price_int,
                    self.convert_year_to_int(car_data.get("year")),
                    parse_mileage_mudah(car_data.get("mileage")),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    now_dt,
                    now_dt,
                    car_data.get("condition", "N/A"),
                    car_data.get("engine_cc"),
                    car_data.get("fuel_type"),
                    json.dumps(car_data.get("images", [])),
                    car_id
                ))

                # Insert history jika harga berubah
                if old_price != price_int and old_price != 0:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (listing_url, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_data["listing_url"], old_price, price_int))

            else:
                # Jika listing_url belum ada, insert data baru
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                        (listing_url, brand, model, variant, information_ads, location,
                        price, year, mileage, transmission, seat_capacity,
                        condition, engine_cc, fuel_type, images, last_scraped_at, last_status_check)
                    VALUES
                        (%s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                now_dt = datetime.now()
                self.cursor.execute(insert_query, (
                    car_data["listing_url"],
                    normalized_brand,
                    normalized_model,
                    normalized_variant,
                    car_data.get("information_ads"),
                    car_data.get("location"),
                    price_int,
                    self.convert_year_to_int(car_data.get("year")),
                    parse_mileage_mudah(car_data.get("mileage")),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("condition", "N/A"),
                    car_data.get("engine_cc"),
                    car_data.get("fuel_type"),
                    json.dumps(car_data.get("images", [])),
                    now_dt,
                    now_dt
                ))
                car_id = self.cursor.fetchone()[0]

            self.conn.commit()
            logging.info(f"✅ Data untuk {car_data['listing_url']} berhasil disimpan/diupdate dengan ID: {car_id}")
            return True, car_id

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")
            return False, None

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

    def get_price_from_listing(self, card):
        """Extract price from listing card element with proper handling of multiple price formats."""
        try:
            # ambil harga penuh
            full_price = card.query_selector('div.text-sm.text-black.font-normal')
            if full_price:
                price_text = full_price.inner_text()
            else:
                # Jika tidak ada harga penuh, cek harga lainnya (bulanan atau lainnya)
                other_price = card.query_selector('span.text-sm.font-bold, div.text-sm.font-bold')
                if other_price:
                    price_text = other_price.inner_text()
                else:
                    logging.warning("Tidak ditemukan elemen harga pada card")
                    return 0
            
            # Bersihkan teks harga
            price_clean = (
                price_text.replace('RM', '')
                .strip()
                .replace(',', '')
                .replace(' ', '')
                .split('/')[0]  
                .split()[0]    
            )
            
            try:
                return int(price_clean)
            except ValueError as e:
                logging.warning(f"Gagal mengkonversi harga '{price_text}' ke integer: {e}")
                return 0
                
        except Exception as e:
            logging.warning(f"Error extracting price from card: {e}")
            return 0

    def convert_year_to_int(self, year_str):
        """Convert year string to integer, handling special cases like '1995 or older'"""
        if not year_str or year_str == "N/A":
            return None
            
        # Extract first number from string
        year_match = re.search(r'\d{4}', year_str)
        if year_match:
            year_int = int(year_match.group(0))
            logging.info(f"Converting year from '{year_str}' to {year_int}")
            return year_int
        return None

def parse_mileage_mudah(mileage_str):
    if not mileage_str:
        return 0
    try:
        # Ambil angka terbesar dari rentang, atau satu angka
        if "-" in mileage_str:
            right = mileage_str.split("-")[-1].strip()
        else:
            right = mileage_str.strip("<> ").strip()
        # Hilangkan 'km', spasi, dan lowercase
        right = right.lower().replace("km", "").replace(" ", "")
        # Ganti k dengan 000
        right = right.replace("k", "000")
        # Ambil angka saja
        mileage_int = int(re.sub(r"[^\d]", "", right))
        return mileage_int
    except Exception:
        return 0