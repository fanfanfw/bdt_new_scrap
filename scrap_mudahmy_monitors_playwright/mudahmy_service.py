import os
import time
import random
import logging
import re
from datetime import datetime
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
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE_MUDAH", "price_history_scrap")
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
    def __init__(self, download_images_locally=True):
        self.stop_flag = False
        self.batch_size = 40
        self.listing_count = 0
        self.last_scraped_data = {} 
        self.download_images_locally = download_images_locally
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.custom_proxies = get_custom_proxy_list()
        self.last_used_proxy = None
        
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
        proxy_used = None

        if proxy_mode == "oxylabs":
            launch_kwargs["proxy"] = {
                "server": os.getenv("PROXY_SERVER"),
                "username": os.getenv("PROXY_USERNAME"),
                "password": os.getenv("PROXY_PASSWORD")
            }
            proxy_used = launch_kwargs["proxy"]["server"]
            logging.info("🌐 Proxy aktif (Oxylabs digunakan)")
        elif proxy_mode == "custom" and self.custom_proxies:
            proxies = [p for p in self.custom_proxies if p["server"] != self.last_used_proxy]
            if not proxies:
                proxies = self.custom_proxies
            proxy = random.choice(proxies)
            proxy_used = proxy["server"]
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy custom digunakan (random): {proxy['server']}")
        else:
            logging.info("⚡ Menjalankan browser tanpa proxy")

        self.last_used_proxy = proxy_used

        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur"
        )
        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

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

    def quit_browser(self):
        try:
            if hasattr(self, "browser"):
                self.browser.close()
        except Exception as e:
            logging.error(e)
        if hasattr(self, "playwright"):
            self.playwright.stop()
        logging.info("🛑 Browser Playwright ditutup.")

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
                # Dapatkan ID dari listing yang baru saja dimasukkan
                new_id = result[0]
                
                # Menambahkan pengisian tanggal untuk kolom information_ads_date
                today_date = datetime.now().strftime('%Y-%m-%d')
                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET information_ads_date = %s
                    WHERE id = %s
                """
                self.cursor.execute(update_query, (today_date, new_id))
                self.conn.commit()

                logging.info(f"✅ Listing baru {listing_url} berhasil ditambahkan ke database dengan price {price}, dan information_ads_date diupdate.")
                return True
            return False
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error saat menambahkan listing baru: {e}")
            return False

    def scrape_page(self, page, url):
        """Scrape hanya dari halaman utama (MUDAHMY_LISTING_URL), ambil price dari halaman utama, insert listing_url dan price, lalu scrape detail."""
        try:
            delay = random.uniform(5, 10)
            logging.info(f"Menuju {url} (delay {delay:.1f}s)")
            time.sleep(delay)
            page.goto(url, timeout=60000)

            # Check for blocks
            if page.locator("text='Access Denied'").is_visible(timeout=3000):
                raise Exception("Akses ditolak")
            if page.locator("text='Please verify you are human'").is_visible(timeout=3000):
                raise Exception("Deteksi CAPTCHA")

            page.wait_for_load_state('networkidle', timeout=15000)

            # Get all card containers ordered from top to bottom
            card_selector = "div[data-testid^='listing-ad-item-']"
            cards = page.query_selector_all(card_selector)

            urls_to_scrape = []
            for card in cards:
                try:
                    a_tag = card.query_selector("a[href*='mudah.my']")
                    if a_tag:
                        href = a_tag.get_attribute('href')
                        if href:
                            # Dapatkan harga dari card
                            current_price = self.get_price_from_listing(card)
                            
                            # Cek listing di database
                            self.cursor.execute(
                                f"""SELECT id, price, 
                                    (brand IS NULL OR brand = '') AS brand_null,
                                    (model IS NULL OR model = '') AS model_null,
                                    (variant IS NULL OR variant = '') AS variant_null,
                                    (information_ads IS NULL OR information_ads = '') AS info_null,
                                    (location IS NULL OR location = '') AS location_null,
                                    images
                                FROM {DB_TABLE_SCRAP} 
                                WHERE listing_url = %s""",
                                (href,)
                            )
                            existing = self.cursor.fetchone()
                            
                            if not existing:
                                # Listing baru, masukkan ke database dengan status active dan price
                                if self.insert_new_listing(href, current_price):
                                    urls_to_scrape.append(href)
                                    logging.info(f"Listing baru ditemukan dan ditambahkan: {href} dengan price {current_price}")
                            else:
                                # Listing sudah ada, cek harga, field penting, dan images
                                db_price = existing[1] if existing[1] else 0
                                has_null_fields = any(existing[2:7])  # Cek field brand_null sampai location_null
                                images_field = existing[7]
                                images_empty = False
                                try:
                                    if images_field is None:
                                        images_empty = True
                                    elif isinstance(images_field, str):
                                        images_empty = images_field.strip() in ('[]', '', 'null')
                                    elif isinstance(images_field, (list, tuple)):
                                        images_empty = len(images_field) == 0
                                except Exception:
                                    images_empty = False
                                
                                if current_price != db_price or has_null_fields or images_empty:
                                    # Harga berbeda, ada field penting null, atau images kosong, perlu update
                                    urls_to_scrape.append(href)
                                    if has_null_fields:
                                        logging.info(f"Listing {href} perlu diupdate karena ada field penting yang masih kosong")
                                    elif images_empty:
                                        logging.info(f"Listing {href} perlu diupdate karena field images kosong")
                                    else:
                                        logging.info(f"Harga berubah untuk {href}: {db_price} -> {current_price}")
                                else:
                                    logging.info(f"Skip listing {href}: harga sama ({current_price}), data lengkap, dan images sudah ada")
                except Exception as e:
                    logging.warning(f"❌ Error memproses card: {e}")
                    continue

            total_listing = len(set(urls_to_scrape))
            logging.info(f"📄 Ditemukan {total_listing} listing yang perlu di-scrape di halaman {url}")
            return list(set(urls_to_scrape))

        except Exception as e:
            logging.error(f"Error saat scraping halaman: {e}")
            return []

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
        if not self.download_images_locally:
            logging.info("Lewati download gambar sesuai parameter --image-download=no")
            return

        try:
            # Fungsi membersihkan nama folder dari karakter aneh
            def clean_filename(name):
                return re.sub(r'[<>:"/\\|?*]', '_', str(name).strip())

            brand = clean_filename(self.last_scraped_data.get("brand", "unknown"))
            model = clean_filename(self.last_scraped_data.get("model", "unknown"))
            variant = clean_filename(self.last_scraped_data.get("variant", "unknown"))

            # Path penyimpanan gambar
            folder_path = os.path.join(self.image_base_path, brand, model, variant, str(car_id))
            os.makedirs(folder_path, exist_ok=True, mode=0o755)

            for idx, img_url in enumerate(image_urls):
                clean_url = img_url.split('?')[0]
                if not clean_url.startswith('http'):
                    clean_url = f"https:{clean_url}"
                file_path = os.path.join(folder_path, f"image_{idx+1}.jpg")
                self.download_image(clean_url, file_path)

            logging.info(f"Gambar disimpan di folder: {folder_path}")
        except Exception as e:
            logging.error(f"Error download images for listing ID {car_id}: {str(e)}")

    def scrape_listing_detail(self, context, url):
        """Scrape detail listing di tab baru. Kembalikan dict data, atau None kalau gagal."""
        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            page = context.new_page()
            try:
                logging.info(f"Navigating to detail page: {url} (Attempt {attempt+1})")
                page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Check if blocked
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
                    time.sleep(3)
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

                # Perbaiki selector location agar lebih robust
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
                        self.download_listing_images(url, image_urls, car_id)
                        
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
                page.close()

                if "ERR_TUNNEL_CONNECTION_FAILED" in str(e) or "net::" in str(e):
                    logging.warning("🚨 Proxy mungkin gagal/tidak stabil. Re-inisialisasi browser dengan proxy baru...")
                    self.quit_browser()
                    time.sleep(random.uniform(5, 10))
                    self.init_browser()
                    context = self.context 
                    continue 

                attempt += 1
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
            while current_page > 0: 
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

    def scrape_all_from_main(self):
        self.reset_scraping()
        self.init_browser()
        try:
            url = MUDAHMY_LISTING_URL
            logging.info(f"Scraping halaman utama: {url}")
            listing_urls = self.scrape_page(self.page, url)
            for href in listing_urls:
                if self.stop_flag:
                    break
                detail_data = self.scrape_listing_detail(self.context, href)
                if detail_data:
                    max_db_retries = 3
                    for attempt in range(1, max_db_retries + 1):
                        try:
                            self.save_to_db(detail_data)
                            break
                        except Exception as e:
                            logging.warning(f"⚠️ Attempt {attempt} gagal simpan data untuk {href}: {e}")
                            if attempt == max_db_retries:
                                logging.error(f"❌ Gagal simpan data setelah {max_db_retries} percobaan: {href}")
                            else:
                                time.sleep(20)
                else:
                    logging.warning(f"Gagal mengambil detail untuk URL: {href}")
                delay = random.uniform(15, 35)
                logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                time.sleep(delay)
        finally:
            self.quit_browser()

    def convert_mileage(self, mileage_str):
        """Convert mileage string to integer in km"""
        if not mileage_str or mileage_str == "N/A":
            return None
        try:
            # Handle cases like "<4k"
            if mileage_str.startswith("<"):
                return int(mileage_str[1:-1]) * 1000 if "k" in mileage_str else int(mileage_str[1:])
            
            # Handle ranges like "10k - 20k"
            if " - " in mileage_str:
                parts = mileage_str.split(" - ")
                max_part = parts[-1]
                if "k" in max_part:
                    return int(float(max_part.replace("k", "")) * 1000)
                return int(max_part)
                    
            # Handle ">500k"
            if mileage_str.startswith(">"):
                return int(mileage_str[1:-1]) * 1000 if "k" in mileage_str else int(mileage_str[1:])
                    
            # Handle normal cases with "k"
            if "k" in mileage_str:
                return int(float(mileage_str.replace("k", "")) * 1000)
                    
            # Handle pure numbers
            return int(mileage_str.replace(",", "").replace("km", "").strip())
            
        except Exception as e:
            logging.warning(f"Gagal mengkonversi mileage '{mileage_str}': {e}")
            return None
    
    def stop_scraping(self):
        logging.info("Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("Scraping direset.")

    def normalize_brand_name(self, brand_str):
        """Normalize brand name by replacing dashes with spaces and cleaning up format."""
        if not brand_str or brand_str == "N/A":
            return brand_str
        
        try:
            # Replace dash with space and clean up
            normalized = (
                brand_str.replace('-', ' ')  # Replace dash with space
                .replace('_', ' ')           # Replace underscore with space (jika ada)
                .strip()                     # Remove leading/trailing spaces
                .upper()                     # Convert to uppercase for consistency
            )
            
            # Remove multiple spaces and replace with single space
            normalized = ' '.join(normalized.split())
            
            logging.info(f"Brand normalized: '{brand_str}' -> '{normalized}'")
            return normalized
            
        except Exception as e:
            logging.warning(f"Error normalizing brand '{brand_str}': {e}")
            return brand_str

    def save_to_db(self, car_data):
        try:
            # Cek apakah listing_url sudah ada di database
            self.cursor.execute(
                f"""SELECT id, price, 
                    (brand IS NULL OR brand = '') AS brand_null,
                    (model IS NULL OR model = '') AS model_null,
                    (variant IS NULL OR variant = '') AS variant_null,
                    (information_ads IS NULL OR information_ads = '') AS info_null,
                    (location IS NULL OR location = '') AS location_null
                FROM {DB_TABLE_SCRAP} 
                WHERE listing_url = %s""",
                (car_data["listing_url"],)
            )
            row = self.cursor.fetchone()

            # Konversi harga
            price_int = 0
            if car_data.get("price"):
                match_price = re.sub(r"[^\d]", "", car_data["price"])
                price_int = int(match_price) if match_price else 0

            # Konversi mileage sebelum menyimpan
            mileage_str = car_data.get("mileage", "")
            mileage_conv = self.convert_mileage(mileage_str)

            # Pastikan mileage telah terkonversi dengan benar sebelum disimpan
            if mileage_conv is None:
                logging.warning(f"Mileage '{mileage_str}' tidak valid, set ke 0 km.")
                mileage_conv = 0  # Jika mileage tidak valid, set ke 0

            # ===== TAMBAHAN: Normalize brand name =====
            normalized_brand = self.normalize_brand_name(car_data.get("brand"))
            normalized_model = self.normalize_model_variant(car_data.get("model"))
            normalized_variant = self.normalize_model_variant(car_data.get("variant"))
            
            if row:
                car_id, old_price, *null_fields = row
                old_price = old_price if old_price else 0
                has_null_fields = any(null_fields)

                # Selalu update data jika ada field yang null atau data baru lebih lengkap
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
                    mileage_conv,
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

                # Update information_ads_date dengan tanggal hari ini
                today_date = datetime.now().strftime('%Y-%m-%d')
                update_date_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET information_ads_date = %s
                    WHERE id = %s
                """
                self.cursor.execute(update_date_query, (today_date, car_id))
                self.conn.commit()

                # Insert history jika harga berubah
                if old_price != price_int and old_price != 0:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (listing_url, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_data["listing_url"], old_price, price_int))

                logging.info(f"✅ Data untuk {car_data['listing_url']} berhasil diupdate dengan ID: {car_id}")
                return True, car_id

            else:
                # Untuk insert baru
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
                    mileage_conv,
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("condition", "N/A"),
                    car_data.get("engine_cc"),
                    car_data.get("fuel_type"),
                    json.dumps(car_data.get("images", [])),
                    now_dt,
                    now_dt
                ))
                result = self.cursor.fetchone()
                if result is not None:
                    car_id = result[0]
                else:
                    logging.error("Gagal insert data: tidak ada ID yang dikembalikan dari database.")
                    return False, None

                # Update information_ads_date dengan tanggal hari ini
                today_date = datetime.now().strftime('%Y-%m-%d')
                update_date_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET information_ads_date = %s
                    WHERE id = %s
                """
                self.cursor.execute(update_date_query, (today_date, car_id))
                self.conn.commit()

                logging.info(f"✅ Data baru untuk {car_data['listing_url']} berhasil disimpan dengan ID: {car_id}")
                return True, car_id

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")
            return False, None

    def export_data(self):
        """
        Mengambil data dari DB_TABLE_SCRAP dalam bentuk list of dict
        """
        try:
            query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if self.cursor.description is not None:
                columns = [desc[0] for desc in self.cursor.description]
            else:
                logging.error("Gagal mengambil kolom: cursor.description bernilai None.")
                return []
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