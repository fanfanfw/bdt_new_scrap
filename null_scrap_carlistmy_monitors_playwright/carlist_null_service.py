import os
import re
import random
import time
import logging
import json
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from .database import get_connection

# Load ENV
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP_CARLIST", "cars_scrap_new")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE_CARLIST", "price_history")

PROXY_MODE = os.getenv("PROXY_MODE_CARLIST", "none").lower()
CUSTOM_PROXIES = os.getenv("CUSTOM_PROXIES_CARLIST", "")
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

log_dir = Path(__file__).resolve().parents[1] / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"scrape_carlistmy_null_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

def take_screenshot(page, name: str):
    try:
        err_dir = log_dir / f"{datetime.now().strftime('%Y%m%d')}_error_carlistmy_null"
        err_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = err_dir / f"{name}_{datetime.now().strftime('%H%M%S')}.png"
        page.screenshot(path=str(screenshot_path))
        logging.info(f"📸 Screenshot saved: {screenshot_path}")
    except Exception as e:
        logging.warning(f"Failed saving screenshot: {e}")

def parse_custom_proxies():
    proxies = []
    for p in CUSTOM_PROXIES.split(","):
        p = p.strip()
        if p:
            try:
                ip, port, user, pw = p.split(":")
                proxies.append({"server": f"{ip}:{port}", "username": user, "password": pw})
            except Exception as e:
                logging.warning(f"Invalid proxy format: {p}")
    return proxies

def parse_mileage(mileage_str):
    if not mileage_str or mileage_str.strip() == "- km":
        return 0
    try:
        # Cek rentang, ambil nilai kanan
        if "-" in mileage_str:
            right = mileage_str.split("-")[-1].strip()
        else:
            right = mileage_str.strip()
        # Hilangkan 'km' dan spasi
        right = right.replace("km", "").replace("KM", "").replace("Km", "").strip()
        # Ganti K/k dengan ribuan
        right = right.replace("K", "000").replace("k", "000")
        # Hilangkan spasi sisa
        right = right.replace(" ", "")
        # Ambil angka saja
        mileage_int = int(re.sub(r"[^\d]", "", right))
        return mileage_int
    except Exception:
        return 0

class CarlistMyNullService:
    def __init__(self, download_images_locally=True):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.playwright = None
        self.browser = None
        self.page = None
        self.custom_proxies = parse_custom_proxies()
        self.session_id = self.generate_session_id()
        self.download_images_locally = download_images_locally

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy(self):
        if PROXY_MODE == "oxylabs":
            username = f"{PROXY_USERNAME}-sessid-{self.session_id}"
            return {"server": PROXY_SERVER, "username": username, "password": PROXY_PASSWORD}
        elif PROXY_MODE == "custom" and self.custom_proxies:
            return random.choice(self.custom_proxies)
        else:
            return None

    def init_browser(self):
        self.playwright = sync_playwright().start()
        proxy_cfg = self.build_proxy()

        args = {"headless": True, "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"]}
        if proxy_cfg:
            args["proxy"] = proxy_cfg

        self.browser = self.playwright.chromium.launch(**args)
        self.page = self.browser.new_page(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            locale="en-US", timezone_id="Asia/Kuala_Lumpur", viewport={"width": 1920, "height": 1080}
        )
        stealth_sync(self.page)

    def quit_browser(self):
        try:
            self.browser.close()
            self.playwright.stop()
        except:
            pass
    
    def normalize_field(self, text, default_value):
        if not text or str(text).strip() in ["-", "N/A", ""]:
            return default_value
        cleaned = re.sub(r'[\-\(\)_]', ' ', text)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.upper()
        return cleaned if cleaned else default_value

    def scrape_null_entries(self, id_min=None, id_max=None):
        query = f"""
            SELECT id, listing_url FROM {DB_TABLE_SCRAP}
            WHERE (condition IS NULL OR brand IS NULL OR model IS NULL OR variant IS NULL
                OR price IS NULL OR information_ads IS NULL OR location IS NULL)
        """
        params = []
        if id_min is not None:
            query += " AND id >= %s"
            params.append(id_min)
        if id_max is not None:
            query += " AND id <= %s"
            params.append(id_max)

        self.cursor.execute(query, tuple(params))
        rows = self.cursor.fetchall()
        urls = [(r[0], r[1]) for r in rows if r[1]]
        logging.info(f"Total null listings found: {len(urls)}")

        for idx, (row_id, url) in enumerate(urls):
            attempt = 0
            success = False
            last_error = None
            while attempt < 3 and not success:
                try:
                    self.init_browser()
                    self.page.goto(url, wait_until="networkidle", timeout=60000)
                    time.sleep(7)
                    self.open_specification_tab()
                    self.load_gallery_images()
                    detail = self.extract_detail(url)
                    if detail:
                        self.save_to_db(detail)
                        success = True
                    self.quit_browser()
                except Exception as e:
                    last_error = e
                    logging.error(f"Failed scraping {url} (Attempt {attempt+1}/3): {e}")
                    take_screenshot(self.page, f"scrape_error_{idx}_{attempt+1}")
                    self.quit_browser()
                    attempt += 1
                    if attempt < 3:
                        self.session_id = self.generate_session_id()
                        time.sleep(random.uniform(5, 15))
                time.sleep(random.uniform(15, 25))
            if not success:
                logging.error(f"❌ Gagal scraping {url} setelah 3 percobaan. Error terakhir: {last_error}")

    def open_specification_tab(self):
        try:
            spec_tab = (
                "#listing-detail > section:nth-child(2) > div > div > "
                "div.u-width-4\\/6.u-width-1\\@mobile.u-flex.u-flex--column.u-padding-left-sm.u-padding-right-md.u-padding-top-none.u-padding-top-none\\@mobile.u-padding-right-sm\\@mobile "
                "> div:nth-child(1) > div > div.c-tabs--overflow > div > a:nth-child(2)"
            )
            if self.page.is_visible(spec_tab):
                self.page.click(spec_tab)
                self.page.wait_for_selector('#tab-specifications span.u-text-bold.u-width-1\\/2.u-align-right', timeout=7000)
        except Exception as e:
            logging.warning(f"Failed opening specifications tab: {e}")

    def load_gallery_images(self):
        try:
            thumb_buttons = self.page.query_selector_all("button.c-gallery__item-thumbnail")
            for btn in thumb_buttons:
                btn.click()
                time.sleep(0.2)
        except Exception as e:
            logging.warning(f"Failed clicking gallery thumbnails: {e}")

    def extract_detail(self, url):
        soup = BeautifulSoup(self.page.content(), "html.parser")
        spans = soup.select("#listing-detail li > a > span")
        valid_spans = [span for span in spans if span.text.strip()]
        num_spans = len(valid_spans)

        # LOGIKA BARU
        relevant_spans = valid_spans[2:] if num_spans > 2 else valid_spans

        brand = model_group = model = variant = None
        if len(relevant_spans) == 2: 
            brand, model = relevant_spans[0].text.strip(), relevant_spans[1].text.strip()
        elif len(relevant_spans) == 3:
            brand, model, variant = relevant_spans[0].text.strip(), relevant_spans[1].text.strip(), relevant_spans[2].text.strip()
        elif len(relevant_spans) == 4:
            brand, model_group, model, variant = relevant_spans[0].text.strip(), relevant_spans[1].text.strip(), relevant_spans[2].text.strip(), relevant_spans[3].text.strip()

        # Default (sama seperti di carlistmy_service.py)
        brand = (brand or "UNKNOWN").upper().replace("-", " ")
        model = (model or "UNKNOWN").upper()
        variant = (variant or "NO VARIANT").upper()
        model_group = (model_group or "NO MODEL GROUP").upper()

        # Extract images
        meta_imgs = self.page.query_selector_all("head > meta[name='prerender']")
        meta_img_urls = set()
        for meta in meta_imgs:
            content = meta.get_attribute("content")
            if content and content.startswith("https://"):
                meta_img_urls.add(content)

        gallery_imgs = [img.get("src") for img in soup.select("#details-gallery img") if img.get("src")]
        all_img_urls = set(gallery_imgs) | meta_img_urls
        images = list(all_img_urls)

        # Helper extract function
        def extract(selector):
            el = soup.select_one(selector)
            return el.text.strip() if el else None

        # LOGIKA LOKASI BARU (ambil 2 span terakhir jika ada)
        def get_location_parts(soup):
            spans = soup.select("div.c-card__body > div.u-flex.u-align-items-center > div > div > span")
            valid = [span.text.strip() for span in spans if span.text.strip()]
            if len(valid) >= 2:
                return " - ".join(valid[-2:])
            elif len(valid) == 1:
                return valid[0]
            return ""

        information_ads = extract("div:nth-child(1) > span.u-color-muted")
        location = get_location_parts(soup)
        condition = extract("div.owl-stage div:nth-child(1) span.u-text-bold")
        price_string = extract("div.listing__item-price > h3")
        year = extract("div.owl-stage div:nth-child(2) span.u-text-bold")
        mileage = extract("div.owl-stage div:nth-child(3) span.u-text-bold")
        transmission = extract("div.owl-stage div:nth-child(6) span.u-text-bold")
        seat_capacity = extract("div.owl-stage div:nth-child(7) span.u-text-bold")

        engine_cc, fuel_type = None, None
        try:
            if self.page.is_visible('div#tab-specifications'):
                engine_cc_elem = self.page.query_selector(
                    '#tab-specifications > div:nth-child(3) > div:nth-child(2) > div > span.u-text-bold.u-width-1\\/2.u-align-right'
                )
                engine_cc = engine_cc_elem.inner_text().strip() if engine_cc_elem else None

                fuel_type_elem = self.page.query_selector(
                    '#tab-specifications > div:nth-child(3) > div:nth-child(8) > div > span.u-text-bold.u-width-1\\/2.u-align-right'
                )
                fuel_type = fuel_type_elem.inner_text().strip() if fuel_type_elem else None
        except Exception as e:
            logging.warning(f"Failed reading specs: {e}")

        price = int(re.sub(r"[^\d]", "", price_string)) if price_string else 0
        year_int = int(re.search(r"\d{4}", year).group()) if year else 0
        # Konversi mileage ke integer
        mileage_int = parse_mileage(mileage)

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
            "mileage": mileage_int,
            "transmission": transmission,
            "seat_capacity": seat_capacity,
            "engine_cc": engine_cc,
            "fuel_type": fuel_type,
            "image": images
        }

    def download_images(self, image_urls, brand, model, variant, car_id):
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
                logging.warning(f"Failed downloading image {url}: {e}")
        return local_paths

    def save_to_db(self, car):
        try:
            self.cursor.execute(f"SELECT id, price, version FROM {DB_TABLE_SCRAP} WHERE listing_url = %s", (car["listing_url"],))
            row = self.cursor.fetchone()
            now = datetime.now()
            image_str = json.dumps(car.get("image") or [])

            # LOGIKA PENYESUAIAN FIELD
            brand = (car.get("brand") or "UNKNOWN").upper().replace("-", " ")
            model_group = self.normalize_field(car.get("model_group"), "NO MODEL GROUP").upper()
            model = self.normalize_field(car.get("model"), "NO MODEL").upper()
            variant = self.normalize_field(car.get("variant"), "NO VARIANT").upper()

            if row:
                car_id, old_price, version = row
                new_version = (version or 1) + 1

                if self.download_images_locally:
                    self.download_images(car.get("image"), brand, model, variant, car_id)

                self.cursor.execute(f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model_group=%s, model=%s, variant=%s, information_ads=%s,
                        location=%s, condition=%s, price=%s, year=%s, mileage=%s,
                        transmission=%s, seat_capacity=%s, engine_cc=%s, fuel_type=%s,
                        last_scraped_at=%s, last_status_check=%s, images=%s, version=%s
                    WHERE id=%s
                """, (
                    brand, model_group, model, variant, car.get("information_ads"),
                    car.get("location"), car.get("condition"), car.get("price"), car.get("year"), car.get("mileage"),
                    car.get("transmission"), car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"),
                    now, now, image_str, new_version, car_id
                ))

                if car["price"] != old_price:
                    self.cursor.execute(f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (listing_url, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """, (car["listing_url"], old_price, car["price"]))

            else:
                self.cursor.execute(f"""
                    INSERT INTO {DB_TABLE_SCRAP} (
                        listing_url, brand, model_group, model, variant, information_ads, location, condition,
                        price, year, mileage, transmission, seat_capacity, engine_cc, fuel_type, version, images, last_scraped_at, last_status_check
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s)
                """, (
                    car["listing_url"], brand, model_group, model, variant,
                    car.get("information_ads"), car.get("location"), car.get("condition"), car.get("price"),
                    car.get("year"), car.get("mileage"), car.get("transmission"),
                    car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"), image_str, now, now
                ))
            self.conn.commit()
            logging.info(f"✅ DB updated: {car['listing_url']}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ DB error: {e}")
