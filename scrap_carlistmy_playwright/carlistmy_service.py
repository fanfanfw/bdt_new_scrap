import os
import re
import random
import time
import logging
import pandas as pd
import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from .database import get_connection

load_dotenv()

START_DATE = datetime.now().strftime('%Y%m%d')

# ===== Konfigurasi Env
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP_CARLIST", "cars_scrap_new")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY_CARLIST", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE_CARLIST", "price_history")
DB_TABLE_HISTORY_PRICE_COMBINED = os.getenv("DB_TABLE_HISTORY_PRICE_COMBINED_CARLIST", "price_history_combined")

USE_PROXY = os.getenv("USE_PROXY_OXYLABS", "false").lower() == "true"
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# ===== Konfigurasi Logging
log_dir = Path(__file__).resolve().parents[1] /  "logs"
log_dir.mkdir(parents=True, exist_ok=True)

# Nama file log menggunakan tanggal saat *pertama kali program dijalankan*
log_file = log_dir / f"scrape_carlistmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def take_screenshot(page, name: str):
    """
    Simpan screenshot ke dalam folder "scraping/logs/<YYYYMMDD>_error/"
    Di mana <YYYYMMDD> adalah tanggal screenshot diambil (bukan START_DATE).
    """
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_carlistmy"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"

        page.screenshot(path=str(screenshot_path))
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")

def get_custom_proxy_list():
    raw = os.getenv("CUSTOM_PROXIES_CARLIST", "")
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
            logging.warning(f"Format proxy tidak valid: {p}")
    return parsed

class CarlistMyService:
    def __init__(self):
        self.stop_flag = False
        self.batch_size = 25
        self.listing_count = 0
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy_config(self):
        proxy_mode = os.getenv("PROXY_MODE_CARLIST", "none").lower()

        if proxy_mode == "oxylabs":
            username_base = os.getenv("PROXY_USERNAME", "")
            proxy_config = {
                "server": os.getenv("PROXY_SERVER"),
                "username": f"{username_base}-sessid-{self.session_id}",
                "password": os.getenv("PROXY_PASSWORD")
            }
            logging.info(f"🌐 Proxy Oxylabs dengan session: {self.session_id}")
            return proxy_config

        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            logging.info(f"🌐 Proxy custom digunakan: {proxy['server']}")
            return proxy

        else:
            logging.info("⚡ Menjalankan tanpa proxy")
            return None

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

        proxy_config = self.build_proxy_config()
        if proxy_config:
            launch_kwargs["proxy"] = proxy_config

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur",
            geolocation={"longitude": 101.68627540160966, "latitude": 3.1504925396418315},
            permissions=["geolocation"],
            viewport={"width": 1920, "height": 1080},  # Set to full page size
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi dengan stealth.")


    def detect_anti_bot(self):
        content = self.page.content()
        if "Checking your browser before accessing" in content or "cf-browser-verification" in content:
            take_screenshot(self.page, "cloudflare_block")
            logging.warning("⚠️ Terkena anti-bot Cloudflare. Akan ganti proxy dan retry...")
            return True
        return False

    def retry_with_new_proxy(self):
        logging.info("🔁 Mengganti session proxy dan reinit browser...")
        self.session_id = self.generate_session_id()
        self.quit_browser()
        self.init_browser()
        try:
            self.get_current_ip()
        except Exception as e:
            logging.warning(f"Gagal get IP: {e}")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception as e:
            logging.error(e)
        self.playwright.stop()
        logging.info("🛑 Browser Playwright ditutup.")

    def get_current_ip(self, retries=3):
        for attempt in range(retries):
            try:
                self.page.goto("https://ip.oxylabs.io/", timeout=10000)
                ip = self.page.inner_text("body").strip()
                logging.info(f"🌐 IP yang digunakan: {ip}")
                return ip
            except Exception as e:
                logging.warning(f"Gagal mengambil IP (percobaan {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(7)
        raise Exception("Gagal mengambil IP setelah beberapa retry.")

    def scrape_detail(self, url):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.page.goto(url, wait_until="networkidle", timeout=60000)
                time.sleep(7)

                try:
                    # Klik tab "Specification" (klik <a> kedua di tab list)
                    spec_tab_selector = (
                        "#listing-detail > section:nth-child(2) > div > div > "
                        "div.u-width-4\/6.u-width-1\\@mobile.u-flex.u-flex--column.u-padding-left-sm.u-padding-right-md.u-padding-top-none.u-padding-top-none\\@mobile.u-padding-right-sm\\@mobile "
                        "> div:nth-child(1) > div > div.c-tabs--overflow > div > a:nth-child(2)"
                    )
                    if self.page.is_visible(spec_tab_selector):
                        self.page.click(spec_tab_selector)
                        # Tunggu konten tab spesifikasi muncul
                        self.page.wait_for_selector(
                            '#tab-specifications span.u-text-bold.u-width-1\\/2.u-align-right',
                            timeout=7000
                        )
                        time.sleep(1)
                except Exception as e:
                    logging.warning(f"Gagal klik tab specifications atau menunggu konten: {e}")

                engine_cc = None
                fuel_type = None
                try:
                    if self.page.is_visible('div#tab-specifications'):
                        try:
                            engine_cc_elem = self.page.query_selector(
                                '#tab-specifications > div:nth-child(3) > div:nth-child(2) > div > span.u-text-bold.u-width-1\\/2.u-align-right'
                            )
                            engine_cc = engine_cc_elem.inner_text().strip() if engine_cc_elem else None
                        except Exception as e:
                            logging.warning(f"Gagal mengambil engine_cc: {e}")
                        try:
                            fuel_type_elem = self.page.query_selector(
                                '#tab-specifications > div:nth-child(3) > div:nth-child(8) > div > span.u-text-bold.u-width-1\\/2.u-align-right'
                            )
                            fuel_type = fuel_type_elem.inner_text().strip() if fuel_type_elem else None
                        except Exception as e:
                            logging.warning(f"Gagal mengambil fuel_type: {e}")
                except Exception as e:
                    logging.warning(f"Gagal mengambil data spesifikasi: {e}")

                # Ambil semua gambar dari meta tag <meta name='prerender'>
                meta_imgs = self.page.query_selector_all("head > meta[name='prerender']")
                meta_img_urls = set()
                try:
                    for meta in meta_imgs:
                        content = meta.get_attribute("content")
                        if content and content.startswith("https://"):
                            meta_img_urls.add(content)
                except Exception as e:
                    logging.warning(f"Gagal mengambil gambar dari meta prerender: {e}")

                # Ambil juga gambar dari #details-gallery img (jaga-jaga)
                soup = BeautifulSoup(self.page.content(), "html.parser")
                # --- Breadcrumb mapping ---
                spans = soup.select("#listing-detail li > a > span")
                brand = model = variant = None
                model_group = None  
                
                # Filter spans yang tidak kosong
                valid_spans = [span for span in spans if span.text.strip()]
                num_spans = len(valid_spans)
                
                for i, span in enumerate(valid_spans):
                    logging.info(f"Span {i}: {span.text.strip()}")
                logging.info(f"Jumlah spans valid dalam breadcrumb: {num_spans}")
                
                relevant_spans = valid_spans[2:] if len(valid_spans) > 2 else []
                num_relevant = len(relevant_spans)
                
                if num_relevant == 2: 
                    brand = relevant_spans[0].text.strip()
                    model = relevant_spans[1].text.strip()
                elif num_relevant == 3:
                    brand = relevant_spans[0].text.strip()
                    model = relevant_spans[1].text.strip()
                    variant = relevant_spans[2].text.strip()
                elif num_relevant == 4:
                    brand = relevant_spans[0].text.strip()
                    model_group = relevant_spans[1].text.strip()
                    model = relevant_spans[2].text.strip()
                    variant = relevant_spans[3].text.strip()
                
                brand = (brand or "UNKNOWN").upper()
                model = (model or "UNKNOWN").upper()
                variant = (variant or "NO VARIANT").upper()
                model_group = model_group.upper() if model_group else "NO MODEL_GROUP"

                logging.info(f"Hasil mapping: Brand={brand}, Model Group={model_group}, Model={model}, Variant={variant}")

                gallery_imgs = [img.get("src") for img in soup.select("#details-gallery img") if img.get("src")]
                all_img_urls = set(gallery_imgs) | meta_img_urls
                image = list(all_img_urls)

                page_title = self.page.title()
                if page_title.strip() == "Just a moment...":
                    logging.warning("🛑 Halaman diblokir Cloudflare. Mengganti proxy dan retry...")
                    take_screenshot(self.page, "cloudflare_detected")
                    retry_count += 1
                    self.retry_with_new_proxy()
                    continue  
                else:
                    soup = BeautifulSoup(self.page.content(), "html.parser")

                    def extract(selector):
                        element = soup.select_one(selector)
                        return element.text.strip() if element else None
                    
                    def get_location_parts(soup):
                        # Ambil semua span child
                        spans = soup.select("div.c-card__body > div.u-flex.u-align-items-center > div > div > span")
                        valid_spans = [span.text.strip() for span in spans if span.text.strip()]
                        if len(valid_spans) >= 2:
                            return " - ".join(valid_spans[-2:])
                        elif len(valid_spans) == 1:
                            return valid_spans[0]
                        return ""

                    information_ads = extract("div:nth-child(1) > span.u-color-muted")
                    location = get_location_parts(soup)
                    condition = extract("div.owl-stage div:nth-child(1) span.u-text-bold")
                    price_string = extract("div.listing__item-price > h3")
                    year = extract("div.owl-stage div:nth-child(2) span.u-text-bold")
                    mileage = extract("div.owl-stage div:nth-child(3) span.u-text-bold")
                    transmission = extract("div.owl-stage div:nth-child(6) span.u-text-bold")
                    seat_capacity = extract("div.owl-stage div:nth-child(7) span.u-text-bold")
                    price = int(re.sub(r"[^\d]", "", price_string)) if price_string else 0
                    year_int = int(re.search(r"\d{4}", year).group()) if year else 0

                    return {
                        "listing_url": url,
                        "brand": brand,
                        "model_group": model_group,
                        "model": model,
                        "variant": variant,
                        "information_ads": information_ads,
                        "location": location,
                        "condition": condition,
                        "price": price,
                        "year": year_int,
                        "mileage": mileage,
                        "transmission": transmission,
                        "seat_capacity": seat_capacity,
                        "image": image,
                        "engine_cc": engine_cc,
                        "fuel_type": fuel_type,
                    }

            except Exception as e:
                logging.error(f"Gagal scraping detail {url}: {e}")
                take_screenshot(self.page, "scrape_detail_error")
                retry_count += 1
                self.retry_with_new_proxy()

        logging.error(f"❌ Gagal mengambil data dari {url} setelah {max_retries} percobaan.")
        return None

    def download_images(self, image_urls, brand, model, variant, car_id):
        """
        Download semua gambar ke folder images/brand/model/variant/id/
        """
        base_dir = Path("images_carlist") / str(brand).replace("/", "_") / str(model).replace("/", "_") / str(variant).replace("/", "_") / str(car_id)
        base_dir.mkdir(parents=True, exist_ok=True)
        local_paths = []
        for idx, url in enumerate(image_urls):
            try:
                ext = os.path.splitext(url)[1].split("?")[0] or ".jpg"
                file_name = f"image_{idx+1}{ext}"
                file_path = base_dir / file_name
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(resp.content)
                    local_paths.append(str(file_path))
            except Exception as e:
                logging.warning(f"Gagal download gambar {url}: {e}")
        return local_paths

    def save_to_db(self, car):
        try:
            self.cursor.execute(f"SELECT id, price, version FROM {DB_TABLE_SCRAP} WHERE listing_url = %s", (car["listing_url"],))
            row = self.cursor.fetchone()
            now = datetime.now()
            image_urls = car.get("image") or []
            image_urls_str = json.dumps(image_urls)
            brand = (car.get("brand") or "unknown").upper()
            model_group = (car.get("model_group") or "NO MODEL_GROUP").upper()
            model = (car.get("model") or "unknown").upper()
            variant = (car.get("variant") or "NO VARIANT").upper()
            car_id = None

            if row:
                car_id, old_price, version = row
                if car["price"] != old_price:
                    self.cursor.execute(f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """, (car_id, old_price, car["price"]))

                self.download_images(image_urls, brand, model, variant, car_id)

                self.cursor.execute(f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model_group=%s, model=%s, variant=%s, information_ads=%s,
                        location=%s, condition=%s, price=%s, year=%s, mileage=%s,
                        transmission=%s, seat_capacity=%s, engine_cc=%s, fuel_type=%s,
                        last_scraped_at=%s, version=%s, images=%s
                    WHERE id=%s
                """, (
                    brand, model_group, model, variant, car.get("information_ads"),
                    car.get("location"), car.get("condition"),car.get("price"), car.get("year"), car.get("mileage"),
                    car.get("transmission"), car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"),
                    now, version + 1, image_urls_str, car_id
                ))
            else:
                self.cursor.execute(f"""
                    INSERT INTO {DB_TABLE_SCRAP} (
                        listing_url, brand, model_group, model, variant, information_ads, location, condition,
                        price, year, mileage, transmission, seat_capacity, engine_cc, fuel_type, version, images
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    car["listing_url"], brand, model_group, model, variant,
                    car.get("information_ads"), car.get("location"), car.get("condition"),car.get("price"),
                    car.get("year"), car.get("mileage"), car.get("transmission"),
                    car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"), 1, image_urls_str
                ))
                car_id = self.cursor.fetchone()[0]
                self.download_images(image_urls, brand, model, variant, car_id)

            self.conn.commit()
            logging.info(f"✅ Data untuk {car['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan ke database: {e}")

    def scrape_all_brands(self, start_page=1, pages=None):
        self.reset_scraping()
        base_url = os.getenv("CARLISTMY_LISTING_URL")
        limit_scrap = int(os.getenv("LIMIT_SCRAP", "0"))

        if not base_url:
            logging.error("❌ CARLISTMY_LISTING_URL belum di-set di .env")
            return

        self.init_browser()
        try:
            self.get_current_ip()
        except Exception as e:
            logging.warning(f"Gagal get IP: {e}")

        # Jika pages tidak disediakan, gunakan range yang besar sebagai fallback
        page_list = pages if pages else range(start_page, 9999)

        total_scraped = 0
        for page in page_list:
            if self.stop_flag:
                break

            paginated_url = f"{base_url}&page={page}"
            max_page_retries = 3
            page_retry_count = 0
            page_loaded = False

            while page_retry_count < max_page_retries and not page_loaded:
                try:
                    logging.info(f"📄 Scraping halaman {page}: {paginated_url}")
                    self.page.goto(paginated_url, timeout=60000)
                    time.sleep(7)
                    page_loaded = True
                except Exception as e:
                    page_retry_count += 1
                    logging.warning(f"❌ Gagal memuat halaman {paginated_url}: {e}")
                    take_screenshot(self.page, f"page_load_error_{page}")
                    if page_retry_count < max_page_retries:
                        logging.info(f"🔄 Mencoba ulang halaman {page} (percobaan ke-{page_retry_count + 1})")
                        self.quit_browser()
                        time.sleep(10)
                        self.init_browser()
                        try:
                            self.get_current_ip()
                        except Exception as e:
                            logging.warning(f"Gagal get IP: {e}")
                        continue
                    else:
                        logging.error(f"❌ Gagal memuat halaman {page} setelah {max_page_retries} percobaan")
                        self.stop_flag = True
                        break

            if not page_loaded:
                break

            html = self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            listing_divs = soup.select('[id^="listing_"]')

            urls = []
            for div in listing_divs:
                tag_elem = div.select_one("span.visuallyhidden--small")
                tag_text = tag_elem.text.strip() if tag_elem else ""
                if tag_text == "Featured":
                    link_elem = div.select_one("h2 a")
                    if link_elem:
                        href = link_elem.get("href")
                        if href:
                            if href.startswith("/"):
                                href = "https://www.carlist.my" + href
                            urls.append(href)
            urls = list(set(urls))
            logging.info(f"📄 Ditemukan {len(urls)} listing URL di halaman {page} yang berlabel 'Featured'.")

            if not urls:
                logging.warning(f"📄 Ditemukan 0 listing URL dengan tag 'Featured' di halaman {page}")
                take_screenshot(self.page, f"no_listing_page{page}")
                self.stop_flag = True
                break

            logging.info("⏳ Menunggu selama 15-30 detik sebelum melanjutkan...")
            time.sleep(random.uniform(15, 30))

            for url in urls:
                if self.stop_flag:
                    break
                if limit_scrap and total_scraped >= limit_scrap:
                    logging.info(f"🏁 Limit scraping {limit_scrap} listing_url tercapai. Proses scraping selesai.")
                    self.stop_flag = True
                    break

                logging.info(f"🔍 Scraping detail: {url}")
                detail = self.scrape_detail(url)
                if detail:
                    self.save_to_db(detail)
                    self.listing_count += 1
                    total_scraped += 1
                    time.sleep(random.uniform(20, 40))

                    if self.listing_count >= self.batch_size:
                        self.quit_browser()
                        time.sleep(5)
                        self.init_browser()
                        try:
                            self.get_current_ip()
                        except Exception as e:
                            logging.warning(f"Gagal get IP: {e}")
                        self.listing_count = 0

            if limit_scrap and total_scraped >= limit_scrap:
                logging.info(f"🏁 Limit scraping {limit_scrap} listing_url tercapai. Proses scraping selesai.")
                break

            logging.info("⏭️ Melanjutkan ke halaman berikutnya...")
            time.sleep(random.uniform(5, 10))

        self.quit_browser()
        logging.info("✅ Proses scraping selesai.")

    def sync_to_cars(self):
        """
        Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}, d
        an sinkronisasi perubahan harga dari price_history ke price_history_combined.
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
                            location=%s, condition=%s,price=%s, year=%s, mileage=%s, transmission=%s,
                            seat_capacity=%s, image=%s, last_scraped_at=%s
                        WHERE listing_url=%s
                    """
                    self.cursor.execute(update_query, (
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("information_ads")],
                        row[col_names.index("location")],
                        row[col_names.index("condition")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("mileage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("image")],
                        row[col_names.index("last_scraped_at")],
                        listing_url
                    ))
                else:
                    insert_query = f"""
                        INSERT INTO {DB_TABLE_PRIMARY}
                            (listing_url, brand, model, variant, information_ads, location, condition,
                             price, year, mileage, transmission, seat_capacity, image, last_scraped_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        listing_url,
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("information_ads")],
                        row[col_names.index("location")],
                        row[col_names.index("condition")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("mileage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("image")],
                        row[col_names.index("last_scraped_at")]
                    ))

            # Sinkronisasi perubahan harga dari price_history ke price_history_combined
            sync_price_history_query = f"""
                INSERT INTO {DB_TABLE_HISTORY_PRICE_COMBINED} (car_id, car_scrap_id, old_price, new_price, changed_at)
                SELECT c.id, cs.id, ph.old_price, ph.new_price, ph.changed_at
                FROM {DB_TABLE_HISTORY_PRICE} ph
                JOIN {DB_TABLE_SCRAP} cs ON ph.car_id = cs.id
                JOIN {DB_TABLE_PRIMARY} c ON cs.listing_url = c.listing_url
                WHERE ph.car_id IS NOT NULL;
            """
            self.cursor.execute(sync_price_history_query)

            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
            logging.info("Sinkronisasi perubahan harga dari price_history ke price_history_combined selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error saat sinkronisasi data: {e}")

    def export_data(self):
        try:
            self.cursor.execute(f"SELECT * FROM {DB_TABLE_SCRAP}")
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logging.error(f"❌ Error export data: {e}")
            return []

    def stop_scraping(self):
        self.stop_flag = True
        logging.info("🛑 Scraping dihentikan oleh user.")

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

    def close(self):
        try:
            self.quit_browser()
        except:
            pass
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logging.error(f"❌ Error saat close koneksi: {e}")