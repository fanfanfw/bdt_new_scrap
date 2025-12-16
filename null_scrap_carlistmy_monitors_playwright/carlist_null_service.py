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
        
        # Backup method: ekstraksi menggunakan BeautifulSoup untuk fuel type dan engine CC
        engine_cc_bs, fuel_type_bs = None, None
        try:
            # Cari ENGINE SPECIFICATIONS section
            engine_headers = soup.find_all(string=lambda text: text and "ENGINE SPECIFICATIONS" in text.upper())
            for header in engine_headers:
                header_div = header.parent
                if header_div and header_div.parent:
                    parent_section = header_div.parent
                    spec_rows = parent_section.select('div:not(:first-child)')
                    
                    for row in spec_rows:
                        label = row.select_one('span:not(.u-text-bold)')
                        value = row.select_one('span.u-text-bold')
                        
                        if label and value:
                            label_text = label.text.strip().lower()
                            value_text = value.text.strip()
                            
                            # Mencari fuel type secara spesifik
                            if "fuel type" in label_text and "consumption" not in label_text:
                                fuel_type_bs = value_text
                                logging.info(f"BeautifulSoup found fuel type: {fuel_type_bs}")
                            
                            # Mencari engine CC
                            if any(term in label_text for term in ["engine cc", "engine capacity", "displacement"]):
                                engine_cc_bs = value_text
                                # Ekstrak angka saja
                                digits = re.findall(r'\d+', engine_cc_bs)
                                if digits:
                                    engine_cc_bs = ''.join(digits)
                                    logging.info(f"BeautifulSoup found engine CC: {engine_cc_bs}")
        except Exception as e:
            logging.warning(f"Failed extracting specs with BeautifulSoup: {e}")
            
        # Lanjutkan dengan ekstraksi data lain
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
                # Metode 1: Cari secara spesifik fuel type dalam ENGINE SPECIFICATIONS section
                try:
                    # Ambil semua spesifikasi dari section Engine
                    engine_sections = self.page.query_selector_all('#tab-specifications > div')
                    
                    for section in engine_sections:
                        # Cari div dengan header "ENGINE SPECIFICATIONS" (biasanya div pertama)
                        header = section.query_selector('div.u-text-bold')
                        if header and "ENGINE SPECIFICATIONS" in header.inner_text().upper():
                            # Cari SEMUA row dengan label yang mengandung "FUEL" dalam ENGINE SPECIFICATIONS section
                            spec_rows = section.query_selector_all('div:not(:first-child)')
                            
                            for row in spec_rows:
                                label_elem = row.query_selector('div > span:not(.u-text-bold)')
                                if not label_elem:
                                    continue
                                    
                                label_text = label_elem.inner_text().strip().upper()
                                
                                # Pastikan ini adalah Fuel Type, bukan Fuel Consumption atau yang lain
                                if "FUEL TYPE" in label_text:
                                    value_elem = row.query_selector('div > span.u-text-bold')
                                    if value_elem:
                                        fuel_type = value_elem.inner_text().strip()
                                        logging.info(f"Found fuel type in ENGINE SPECIFICATIONS: {fuel_type}")
                                        break
                                
                                # Cari juga engine CC
                                if "ENGINE" in label_text and ("CC" in label_text or "CAPACITY" in label_text):
                                    value_elem = row.query_selector('div > span.u-text-bold')
                                    if value_elem:
                                        engine_cc = value_elem.inner_text().strip()
                                        logging.info(f"Found engine CC: {engine_cc}")
                except Exception as e:
                    logging.warning(f"Failed finding specs in ENGINE SPECIFICATIONS section: {e}")
                
                # Metode 2: Cari secara spesifik label "Fuel Type", hindari "Fuel Consumption"
                if not fuel_type:
                    try:
                        # Gunakan selector yang lebih spesifik untuk fuel type
                        fuel_type_rows = self.page.query_selector_all('div:has(span:text-matches("(?i)fuel\\s*type"))')
                        for row in fuel_type_rows:
                            value_elem = row.query_selector('span.u-text-bold')
                            if value_elem:
                                fuel_type = value_elem.inner_text().strip()
                                logging.info(f"Found fuel type using specific text-match: {fuel_type}")
                                break
                                
                        # Jika masih tidak ditemukan, coba dengan selector yang lebih longgar tapi filter hasil
                        if not fuel_type:
                            fuel_rows = self.page.query_selector_all('div:has(span:text-matches("(?i)fuel"))')
                            for row in fuel_rows:
                                label_elem = row.query_selector('span:not(.u-text-bold)')
                                if label_elem:
                                    label_text = label_elem.inner_text().strip().lower()
                                    # Filter hanya fuel type, bukan fuel consumption atau lainnya
                                    if "fuel type" in label_text and "consumption" not in label_text:
                                        value_elem = row.query_selector('span.u-text-bold')
                                        if value_elem:
                                            fuel_type = value_elem.inner_text().strip()
                                            logging.info(f"Found fuel type after filtering: {fuel_type}")
                                            break
                    except Exception as e:
                        logging.warning(f"Failed finding fuel type with text-matches: {e}")
                
                # Fallback ke metode lama jika masih belum dapat fuel type atau engine CC
                if not engine_cc:
                    try:
                        engine_cc_elem = self.page.query_selector(
                            '#tab-specifications > div:nth-child(3) > div:nth-child(2) > div > span.u-text-bold.u-width-1\\/2.u-align-right'
                        )
                        engine_cc = engine_cc_elem.inner_text().strip() if engine_cc_elem else None
                        if engine_cc:
                            logging.info(f"Found engine CC with fallback method: {engine_cc}")
                    except Exception as e:
                        logging.warning(f"Failed finding engine CC with fallback: {e}")
                
                # Bersihkan engine CC dan ekstrak angka saja
                if engine_cc:
                    try:
                        digits = re.findall(r'\d+', str(engine_cc))
                        if digits:
                            engine_cc = ''.join(digits)
                            logging.info(f"Extracted numeric engine CC: {engine_cc}")
                    except Exception as e:
                        logging.warning(f"Failed cleaning engine CC: {e}")
        except Exception as e:
            logging.warning(f"Failed reading specs: {e}")

        price = int(re.sub(r"[^\d]", "", price_string)) if price_string else 0
        year_int = int(re.search(r"\d{4}", year).group()) if year else 0
        # Konversi mileage ke integer
        mileage_int = parse_mileage(mileage)
        
        # Gunakan hasil BeautifulSoup jika metode Playwright tidak berhasil
        if not fuel_type and fuel_type_bs:
            fuel_type = fuel_type_bs
            logging.info(f"Using BeautifulSoup fuel type result: {fuel_type}")
            
        if not engine_cc and engine_cc_bs:
            engine_cc = engine_cc_bs
            logging.info(f"Using BeautifulSoup engine CC result: {engine_cc}")
        
        # Normalisasi fuel type
        def normalize_fuel_type(fuel_value):
            if not fuel_value:
                return None
                
            fuel_value = str(fuel_value).lower().strip()
            
            # Mapping nilai fuel type ke standar
            if any(word in fuel_value for word in ['petrol', 'gasoline', 'bensin', 'ron', 'unleaded', 'ulp']):
                return "Petrol - Unleaded (ULP)"
            elif any(word in fuel_value for word in ['diesel', 'tdi', 'dci', 'hdi', 'crdi']):
                return "Diesel"
            elif any(word in fuel_value for word in ['hybrid', 'hibrid']):
                if 'plug' in fuel_value:
                    return "Plug-in Hybrid"
                return "Hybrid"
            elif any(word in fuel_value for word in ['electric', 'ev', 'bev']):
                return "Electric"
            elif 'lpg' in fuel_value or 'cng' in fuel_value or 'gas' in fuel_value:
                return "Gas"
            else:
                # Check for numeric values that might be misinterpreted as fuel type (often engine specs)
                if re.match(r'^[\d\.]+$', fuel_value) or re.match(r'^\d+\s*cc$', fuel_value):
                    logging.warning(f"Detected potential numeric value as fuel type: {fuel_value}")
                    return None
                
                return fuel_value.capitalize()
        
        # Validasi fuel type
        if fuel_type:
            # Cek apakah fuel type mengandung nilai numerik atau pola konsumsi bahan bakar
            if re.match(r'^[\d\.]+$', str(fuel_type)):
                logging.warning(f"Fuel type appears to be numeric value: {fuel_type}. Setting to default.")
                fuel_type = None
            
            # Jika fuel_type mengandung karakter 'L' dan angka, kemungkinan itu adalah nilai konsumsi
            elif 'l/' in str(fuel_type).lower() or 'km/l' in str(fuel_type).lower() or 'liter' in str(fuel_type).lower():
                logging.warning(f"Fuel type appears to be consumption value: {fuel_type}. Setting to default.")
                fuel_type = None
                
            # Jika fuel_type terlalu pendek (misalnya hanya "6.4"), kemungkinan itu adalah nilai konsumsi
            elif len(str(fuel_type)) < 4:
                logging.warning(f"Fuel type too short, likely not valid: {fuel_type}. Setting to default.")
                fuel_type = None
        
        # Terapkan normalisasi
        normalized_fuel_type = normalize_fuel_type(fuel_type)
        if normalized_fuel_type:
            fuel_type = normalized_fuel_type
            logging.info(f"Normalized fuel type: {fuel_type}")
            
        # Fallback - coba tebak fuel type dari variant atau model jika masih kosong
        if not fuel_type:
            variant_model_text = f"{variant} {model}".lower()
            if any(term in variant_model_text for term in ['diesel', 'crdi', 'dci', 'tdi', 'hdi']):
                fuel_type = "Diesel"
                logging.info(f"Inferred diesel from variant/model: {variant} {model}")
            elif any(term in variant_model_text for term in ['hybrid', 'hev', 'phev']):
                if 'phev' in variant_model_text or 'plug' in variant_model_text:
                    fuel_type = "Plug-in Hybrid"
                else:
                    fuel_type = "Hybrid"
                logging.info(f"Inferred hybrid from variant/model: {variant} {model}")
            elif any(term in variant_model_text for term in ['electric', 'ev', 'bev']):
                fuel_type = "Electric"
                logging.info(f"Inferred electric from variant/model: {variant} {model}")
            else:
                # Default to petrol if nothing else matches
                fuel_type = "Petrol - Unleaded (ULP)"
                logging.info("No fuel type found, defaulting to Petrol")
        
        # Final logging dari hasil yang ditemukan
        logging.info(f"Final extraction results - Fuel Type: {fuel_type}, Engine CC: {engine_cc}")

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

    def download_images(self, image_urls, brand, model, variant, year, car_id):
        year_segment = str(year).strip() if year else "UNKNOWN_YEAR"
        base_dir = (
            Path("images_carlist")
            / str(brand).replace("/", "_")
            / str(model).replace("/", "_")
            / str(variant).replace("/", "_")
            / year_segment.replace("/", "_")
            / str(car_id)
        )
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
                    self.download_images(car.get("image"), brand, model, variant, car.get("year"), car_id)

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
