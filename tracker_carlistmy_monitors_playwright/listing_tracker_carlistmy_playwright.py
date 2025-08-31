import os
import time
import random
import logging
import re
import json
from datetime import datetime,timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from pathlib import Path
from bs4 import BeautifulSoup

from .database import get_connection

load_dotenv(override=True)

DB_TABLE_PRIMARY = os.getenv("DB_TABLE_SCRAP_CARLIST", "cars_scrap")

START_DATE = datetime.now().strftime('%Y%m%d')

log_dir = Path(__file__).resolve().parents[0].parents[0] / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"tracker_carlistmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("carlistmy_tracker")


def take_screenshot(page, name: str):
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_carlistmy_tracker"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"
        page.screenshot(path=str(screenshot_path))
        logger.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logger.warning(f"❌ Gagal menyimpan screenshot: {e}")

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
            continue
    return parsed

class ListingTrackerCarlistmyPlaywright:
    def __init__(self, listings_per_batch=15):
        self.listings_per_batch = listings_per_batch
        self.sold_text_indicator = "This car has already been sold."
        self.custom_proxies = get_custom_proxy_list()
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy_config(self):
        proxy_mode = os.getenv("PROXY_MODE_CARLIST", "none").lower()

        if proxy_mode == "oxylabs":
            username_base = os.getenv("PROXY_USERNAME", "")
            return {
                "server": os.getenv("PROXY_SERVER"),
                "username": f"{username_base}-sessid-{self.session_id}",
                "password": os.getenv("PROXY_PASSWORD")
            }

        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            return proxy

        else:
            return None

    def init_browser(self):
        self.playwright = sync_playwright().start()

        launch_kwargs = {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        }

        proxy = self.build_proxy_config()
        if proxy:
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy digunakan: {proxy['server']}")
        else:
            logging.info("⚡ Browser dijalankan tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur",
            geolocation={"longitude": 101.68627540160966, "latitude": 3.1504925396418315},
            permissions=["geolocation"]
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def save_price_change(self, old_price, new_price, listing_url):
        conn = get_connection()
        if not conn:
            logger.error("❌ Gagal koneksi database saat simpan harga.")
            return
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO price_history_scrap_carlistmy (old_price, new_price, changed_at, listing_url)
                VALUES (%s, %s, NOW(), %s)
            """, (old_price, new_price, listing_url))
            conn.commit()
            logger.info(f"📝 Perubahan harga disimpan untuk URL={listing_url}")
        except Exception as e:
            logger.error(f"❌ Gagal simpan perubahan harga untuk URL={listing_url}: {e}")
        finally:
            cursor.close()
            conn.close()

    def update_price(self, car_id, new_price):
        conn = get_connection()
        if not conn:
            logger.error("❌ Gagal koneksi database saat update harga.")
            return
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {DB_TABLE_PRIMARY}
                SET price = %s
                WHERE id = %s
            """, (new_price, car_id))
            conn.commit()
            logger.info(f"✅ Harga berhasil diupdate untuk ID={car_id}")
        except Exception as e:
            logger.error(f"❌ Gagal update harga ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def update_full_data(self, car_id, car_data):
        conn = get_connection()
        if not conn:
            logger.error("❌ Gagal koneksi database saat update full data.")
            return
        cursor = conn.cursor()
        try:
            now = datetime.now()
            image_urls_str = json.dumps(car_data.get("image", []))
            cursor.execute(f"""
                UPDATE {DB_TABLE_PRIMARY}
                SET brand=%s, model_group=%s, model=%s, variant=%s, information_ads=%s,
                    information_ads_date=%s, location=%s, condition=%s, price=%s, year=%s, mileage=%s,
                    transmission=%s, seat_capacity=%s, engine_cc=%s, fuel_type=%s,
                    last_scraped_at=%s, last_status_check=%s, images=%s
                WHERE id = %s
            """, (
                car_data.get("brand"), car_data.get("model_group"), car_data.get("model"), 
                car_data.get("variant"), car_data.get("information_ads"), car_data.get("information_ads_date"),
                car_data.get("location"), car_data.get("condition"), car_data.get("price"), car_data.get("year"), 
                car_data.get("mileage"), car_data.get("transmission"), car_data.get("seat_capacity"), 
                car_data.get("engine_cc"), car_data.get("fuel_type"), now, now, image_urls_str, car_id
            ))
            conn.commit()
            logger.info(f"✅ Full data berhasil diupdate untuk ID={car_id}")
        except Exception as e:
            logger.error(f"❌ Gagal update full data ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()


    def retry_with_new_proxy(self):
        self.quit_browser()
        time.sleep(random.uniform(5, 8))
        self.session_id = self.generate_session_id()
        self.init_browser()
        logger.info(f"🔁 Reinit browser dengan session ID baru: {self.session_id}")
        time.sleep(random.uniform(3, 5))

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception:
            pass
        try:
            self.playwright.stop()
        except Exception:
            pass
        logger.info("🛑 Browser Playwright ditutup.")

    def update_car_status(self, car_id, status, sold_at=None):
        conn = get_connection()
        if not conn:
            logger.error("Tidak bisa update status, koneksi database gagal.")
            return
        cursor = conn.cursor()
        try:
            now = datetime.now()
            if sold_at:
                cursor.execute(f"""
                    UPDATE {DB_TABLE_PRIMARY}
                    SET status = %s,
                        sold_at = %s,
                        last_status_check = %s
                    WHERE id = %s
                """, (status, sold_at, now, car_id))
            else:
                cursor.execute(f"""
                    UPDATE {DB_TABLE_PRIMARY}
                    SET status = %s,
                        last_status_check = %s
                    WHERE id = %s
                """, (status, now, car_id))
            conn.commit()
            logger.info(f"> ID={car_id} => Status diupdate ke '{status}', waktu cek status diset ke {now}")
        except Exception as e:
            logger.error(f"❌ Gagal update_car_status ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def normalize_field(self, text, default_value):
        if not text or str(text).strip() in ["-", "N/A", ""]:
            return default_value
        cleaned = re.sub(r'[\-\(\)_]', ' ', text)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.upper()
        return cleaned if cleaned else default_value

    def parse_mileage(self, mileage_str):
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

    def parse_information_ads_date(self, information_ads):
        """
        Konversi information_ads ke format YYYY-MM-DD
        Input: "Updated on: May 14, 2025"
        Output: "2025-05-14"
        """
        if not information_ads:
            return None
            
        try:
            # Mapping bulan ke angka
            month_mapping = {
                'january': '01', 'jan': '01',
                'february': '02', 'feb': '02', 
                'march': '03', 'mar': '03',
                'april': '04', 'apr': '04',
                'may': '05',
                'june': '06', 'jun': '06',
                'july': '07', 'jul': '07',
                'august': '08', 'aug': '08',
                'september': '09', 'sep': '09', 'sept': '09',
                'october': '10', 'oct': '10',
                'november': '11', 'nov': '11',
                'december': '12', 'dec': '12'
            }
            
            # Clean input string
            text = information_ads.lower().strip()
            
            # Remove common prefixes
            text = re.sub(r'^(updated\s+on:\s*|posted\s+on:\s*|date:\s*)', '', text)
            
            # Pattern untuk format: "May 14, 2025" atau "14 May 2025"
            # Pattern 1: Month Day, Year (May 14, 2025)
            pattern1 = r'([a-z]+)\s+(\d{1,2}),?\s+(\d{4})'
            match1 = re.search(pattern1, text)
            
            if match1:
                month_str, day, year = match1.groups()
                month_num = month_mapping.get(month_str[:3])  # Ambil 3 karakter pertama
                if month_num:
                    day_formatted = day.zfill(2)  # Pad dengan 0 jika perlu
                    result = f"{year}-{month_num}-{day_formatted}"
                    logger.info(f"Parsed date: '{information_ads}' -> '{result}'")
                    return result
            
            # Pattern 2: Day Month Year (14 May 2025)
            pattern2 = r'(\d{1,2})\s+([a-z]+)\s+(\d{4})'
            match2 = re.search(pattern2, text)
            
            if match2:
                day, month_str, year = match2.groups()
                month_num = month_mapping.get(month_str[:3])  # Ambil 3 karakter pertama
                if month_num:
                    day_formatted = day.zfill(2)  # Pad dengan 0 jika perlu
                    result = f"{year}-{month_num}-{day_formatted}"
                    logger.info(f"Parsed date: '{information_ads}' -> '{result}'")
                    return result
                    
            # Pattern 3: ISO-like format (2025-05-14 atau 2025/05/14)
            pattern3 = r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
            match3 = re.search(pattern3, text)
            
            if match3:
                year, month, day = match3.groups()
                month_formatted = month.zfill(2)
                day_formatted = day.zfill(2)
                result = f"{year}-{month_formatted}-{day_formatted}"
                logger.info(f"Parsed date: '{information_ads}' -> '{result}'")
                return result
            
            # Jika tidak ada pattern yang cocok
            logger.warning(f"Failed to parse date: '{information_ads}' - format tidak dikenali")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing information_ads_date '{information_ads}': {e}")
            return None

    def extract_price_from_page(self):
        try:
            price_element = self.page.query_selector(
                '#details-gallery h3.u-color-white.u-text-bold'
            )
            if price_element:
                price_text = price_element.inner_text().strip()
                logger.debug(f"[DEBUG] Teks harga ditemukan: {price_text}")
                price_cleaned = int(re.sub(r"[^\d]", "", price_text))
                return price_cleaned
            else:
                logger.warning("⚠️ Elemen harga tidak ditemukan.")
        except Exception as e:
            logger.error(f"❌ Gagal extract harga dari halaman: {e}")
        return None

    def detect_cloudflare_block(self):
        try:
            title = self.page.title()
            if "Just a moment..." in title:
                take_screenshot(self.page, "cloudflare_block")
                logger.warning("⚠️ Terblokir Cloudflare, reinit browser & ganti proxy.")
                return True
            return False
        except Exception as e:
            logger.warning(f"❌ Gagal cek title: {e}")
            return False

    def scrape_detail(self, url):
        """Scrape detail lengkap dari halaman listing"""
        try:
            # Proses klik tab specification (aman jika gagal)
            try:
                spec_tab_selector = (
                    "#listing-detail > section:nth-child(2) > div > div > "
                    "div.u-width-4\\/6.u-width-1\\\\@mobile.u-flex.u-flex--column.u-padding-left-sm.u-padding-right-md.u-padding-top-none.u-padding-top-none\\\\@mobile.u-padding-right-sm\\\\@mobile "
                    "> div:nth-child(1) > div > div.c-tabs--overflow > div > a:nth-child(2)"
                )
                if self.page.is_visible(spec_tab_selector):
                    self.page.click(spec_tab_selector)
                    self.page.wait_for_selector(
                        '#tab-specifications span.u-text-bold.u-width-1\\\\/2.u-align-right',
                        timeout=7000
                    )
                    time.sleep(1)
            except Exception as e:
                logger.warning(f"Gagal klik tab specifications: {e}")

            # Extract engine_cc dan fuel_type dari specifications
            engine_cc, fuel_type = None, None
            try:
                if self.page.is_visible('div#tab-specifications'):
                    specification_sections = self.page.query_selector_all('#tab-specifications > div')
                    
                    for section in specification_sections:
                        specification_rows = section.query_selector_all('div')
                        
                        for row in specification_rows:
                            label_elem = row.query_selector('div > span:not(.u-text-bold)')
                            if not label_elem:
                                continue
                                
                            label_text = label_elem.inner_text().strip().lower()
                            value_elem = row.query_selector('div > span.u-text-bold')
                            
                            if not value_elem:
                                continue
                                
                            value_text = value_elem.inner_text().strip()
                            
                            # More flexible matching patterns
                            if any(term in label_text for term in ['engine capacity', 'engine cc', 'displacement', 'engine size']):
                                engine_cc = value_text
                                logger.info(f"Found engine CC: {engine_cc}")
                            
                            if any(term in label_text for term in ['fuel type']) and 'consumption' not in label_text:
                                fuel_type = value_text
                                logger.info(f"Found fuel type: {fuel_type}")
                        
                    # Clean engine_cc value
                    if engine_cc:
                        try:
                            digits = re.findall(r'\d+', engine_cc)
                            if digits:
                                engine_cc = ''.join(digits)
                                logger.info(f"Cleaned engine CC: {engine_cc}")
                        except Exception as e:
                            logger.warning(f"Failed to clean engine CC: {e}")
                            
            except Exception as e:
                logger.warning(f"Gagal ambil spesifikasi: {e}")

            # Parse page content with BeautifulSoup
            soup = BeautifulSoup(self.page.content(), "html.parser")
            
            # Extract brand, model, variant from breadcrumb
            spans = soup.select("#listing-detail li > a > span")
            valid_spans = [span for span in spans if span.text.strip()]
            relevant_spans = valid_spans[2:] if len(valid_spans) > 2 else []
            brand = model = variant = model_group = None

            if len(relevant_spans) == 2: 
                brand, model = relevant_spans[0].text.strip(), relevant_spans[1].text.strip()
            elif len(relevant_spans) == 3:
                brand, model, variant = relevant_spans[0].text.strip(), relevant_spans[1].text.strip(), relevant_spans[2].text.strip()
            elif len(relevant_spans) == 4:
                brand, model_group, model, variant = relevant_spans[0].text.strip(), relevant_spans[1].text.strip(), relevant_spans[2].text.strip(), relevant_spans[3].text.strip()

            brand = (brand or "UNKNOWN").upper().replace("-", " ")
            model = (model or "UNKNOWN").upper()
            variant = (variant or "NO VARIANT").upper()
            model_group = (model_group or "NO MODEL GROUP").upper()

            # Extract images
            gallery_imgs = [img.get("src") for img in soup.select("#details-gallery img") if img.get("src")]
            meta_imgs = self.page.query_selector_all("head > meta[name='prerender']")
            meta_img_urls = set()
            try:
                for meta in meta_imgs:
                    content = meta.get_attribute("content")
                    if content and content.startswith("https://"):
                        meta_img_urls.add(content)
            except Exception as e:
                logger.warning(f"Gagal ambil meta image: {e}")
            
            all_img_urls = set(gallery_imgs) | meta_img_urls
            image = list(all_img_urls)

            def extract(selector):
                element = soup.select_one(selector)
                return element.text.strip() if element else None

            def get_location_parts(soup):
                spans = soup.select("div.c-card__body > div.u-flex.u-align-items-center > div > div > span")
                valid_spans = [span.text.strip() for span in spans if span.text.strip()]
                if len(valid_spans) >= 2:
                    return " - ".join(valid_spans[-2:])
                elif len(valid_spans) == 1:
                    return valid_spans[0]
                return ""

            # Extract other details
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
            mileage_int = self.parse_mileage(mileage)
            
            # Parse information_ads_date
            information_ads_date = self.parse_information_ads_date(information_ads)
            
            # Normalize fuel type
            if fuel_type:
                fuel_value = str(fuel_type).lower().strip()
                if any(word in fuel_value for word in ['petrol', 'gasoline', 'bensin', 'ron', 'unleaded', 'ulp']):
                    fuel_type = "Petrol - Unleaded (ULP)"
                elif any(word in fuel_value for word in ['diesel', 'tdi', 'dci', 'hdi', 'crdi']):
                    fuel_type = "Diesel"
                elif any(word in fuel_value for word in ['hybrid', 'hibrid']):
                    fuel_type = "Hybrid"
                elif any(word in fuel_value for word in ['electric', 'ev', 'bev']):
                    fuel_type = "Electric"
            
            # Default fuel type if not found
            if not fuel_type:
                fuel_type = "Petrol - Unleaded (ULP)"

            return {
                "listing_url": url,
                "brand": brand,
                "model_group": model_group,
                "model": model,
                "variant": variant,
                "information_ads": information_ads,
                "information_ads_date": information_ads_date,
                "location": location,
                "condition": condition,
                "price": price,
                "year": year_int,
                "mileage": mileage_int,
                "transmission": transmission,
                "seat_capacity": seat_capacity,
                "image": image,
                "engine_cc": engine_cc,
                "fuel_type": fuel_type,
            }

        except Exception as e:
            logger.error(f"❌ Gagal scrape detail untuk {url}: {e}")
            return None

    def track_listings(self, start_id=1, status_filter="all"):
        if status_filter not in ["all", "active", "unknown"]:
            logger.warning(f"⚠️ Status filter tidak valid: {status_filter}, fallback ke 'all'")
            status_filter = "all"

        conn = get_connection()
        if not conn:
            logger.error("❌ Gagal koneksi database.")
            return

        cursor = conn.cursor()
        threshold_date = datetime.now() - timedelta(days=30)

        if status_filter == "all":
            cursor.execute(f"""
                SELECT id, listing_url, status, price
                FROM {DB_TABLE_PRIMARY}
                WHERE id >= %s AND status != 'sold'
                AND (last_status_check IS NULL OR information_ads_date < %s)
                ORDER BY id
            """, (start_id, threshold_date))
        else:
            cursor.execute(f"""
                SELECT id, listing_url, status, price
                FROM {DB_TABLE_PRIMARY}
                WHERE status = %s AND id >= %s AND status != 'sold'
                AND (last_status_check IS NULL OR information_ads_date < %s)
                ORDER BY id
            """, (status_filter, start_id, threshold_date))

        listings = cursor.fetchall()
        cursor.close()
        conn.close()

        if not listings:
            logger.info("🚫 Tidak ada data yang perlu dicek. Browser tidak akan dijalankan.")
            return

        logger.info(f"📄 Total data: {len(listings)} | Reinit setiap {self.listings_per_batch} listing")
        self.init_browser()
        time.sleep(random.uniform(3, 5))

        for index, (car_id, url, _, old_price) in enumerate(listings, start=1):
            logger.info(f"🔍 Memeriksa ID={car_id} - {url}")

            try:
                self.page.goto(url, wait_until="networkidle", timeout=90000)
                time.sleep(7)

                if self.detect_cloudflare_block():
                    raise Exception("Cloudflare block detected")

                if "used-cars" not in self.page.url:
                    logger.info(f"🚫 Redirect terdeteksi. ID={car_id} kemungkinan sudah terjual.")
                    self.update_car_status(car_id, "sold", datetime.now())
                    continue

                self.page.evaluate("window.scrollTo(0, 1000)")
                time.sleep(random.uniform(2, 4))

                if self.sold_text_indicator in self.page.content():
                    self.update_car_status(car_id, "sold", datetime.now())
                    continue

                # Scrape detail lengkap dari halaman
                detail_data = self.scrape_detail(url)
                if detail_data:
                    new_price = detail_data.get("price", 0)
                    
                    # Cek apakah ada perubahan harga
                    if new_price != old_price:
                        logger.info(f"💲 Harga berubah! ID={car_id}: {old_price} ➜ {new_price}")
                        self.save_price_change(old_price, new_price, url)
                    
                    # Update semua data, bukan hanya harga
                    logger.info(f"🔄 Update full data untuk ID={car_id}")
                    self.update_full_data(car_id, detail_data)
                else:
                    logger.warning(f"⚠️ Gagal scrape detail untuk ID={car_id}, hanya update status")

                self.update_car_status(car_id, "active")

            except Exception as e:
                logger.error(f"❌ Gagal memeriksa ID={car_id}: {e}")
                take_screenshot(self.page, f"error_{car_id}")
                self.retry_with_new_proxy()

            if index % self.listings_per_batch == 0 and index < len(listings):
                logger.info("🔁 Reinit browser untuk batch selanjutnya")
                self.retry_with_new_proxy()

        self.quit_browser()
        logger.info("✅ Selesai semua listing.")