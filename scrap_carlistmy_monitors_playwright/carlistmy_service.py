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
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from .database import get_connection

load_dotenv(override=True)

START_DATE = datetime.now().strftime('%Y%m%d')

# ===== Konfigurasi Env
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP_CARLIST", "cars_scrap_new")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE_CARLIST", "price_history")

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

class CarlistMyService:
    def __init__(self, download_images_locally=True):
        self.download_images_locally = download_images_locally
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
            "headless": True,
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
        self.page.set_default_navigation_timeout(90000)
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
    
    def normalize_field(self, text, default_value):
        if not text or str(text).strip() in ["-", "N/A", ""]:
            return default_value
        cleaned = re.sub(r'[\-\(\)_]', ' ', text)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.upper()
        return cleaned if cleaned else default_value

    def scrape_detail(self, url):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
                try:
                    # Hindari menunggu network idle yang tidak selesai karena widget/chat, cukup pastikan konten utama muncul
                    self.page.wait_for_selector("#listing-detail", timeout=20000)
                except Exception as e:
                    logging.warning(f"Selector listing detail tidak muncul tepat waktu: {e}")
                time.sleep(5)

                # Deteksi Cloudflare
                page_title = self.page.title()
                if page_title.strip() == "Just a moment...":
                    logging.warning("🛑 Halaman diblokir Cloudflare saat detail. Mengganti proxy dan retry...")
                    take_screenshot(self.page, "cloudflare_detected_detail")
                    self.retry_with_new_proxy()
                    retry_count += 1
                    continue  # retry ulang

                # Proses klik tab specification (aman jika gagal)
                try:
                    spec_tab_selector = (
                        "#listing-detail > section:nth-child(2) > div > div > "
                        "div.u-width-4\/6.u-width-1\\@mobile.u-flex.u-flex--column.u-padding-left-sm.u-padding-right-md.u-padding-top-none.u-padding-top-none\\@mobile.u-padding-right-sm\\@mobile "
                        "> div:nth-child(1) > div > div.c-tabs--overflow > div > a:nth-child(2)"
                    )
                    if self.page.is_visible(spec_tab_selector):
                        self.page.click(spec_tab_selector)
                        self.page.wait_for_selector(
                            '#tab-specifications span.u-text-bold.u-width-1\\/2.u-align-right',
                            timeout=7000
                        )
                        time.sleep(1)
                except Exception as e:
                    logging.warning(f"Gagal klik tab specifications: {e}")

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
                        except Exception as e:
                            logging.warning(f"Gagal ambil fuel type dalam ENGINE SPECIFICATIONS: {e}")
                        
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
                                logging.warning(f"Gagal ambil fuel type dengan text-matches: {e}")
                                
                        # Metode 3: Scan semua specification sections
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
                                    logging.info(f"Playwright found engine CC: {engine_cc}")
                                
                                if any(term in label_text for term in ['fuel type', 'fuel', 'petrol/diesel']):
                                    fuel_type = value_text
                                    logging.info(f"Playwright found fuel type: {fuel_type}")
                        
                        # Try alternative selectors if not found
                        if engine_cc is None:
                            engine_cc_elem = self.page.query_selector('text="Engine CC" ~ span, text="Engine Capacity" ~ span')
                            if engine_cc_elem:
                                engine_cc = engine_cc_elem.inner_text().strip()
                                logging.info(f"Found engine CC with alternative selector: {engine_cc}")
                        
                        if fuel_type is None:
                            fuel_type_elem = self.page.query_selector('text="Fuel Type" ~ span, text="Fuel" ~ span')
                            if fuel_type_elem:
                                fuel_type = fuel_type_elem.inner_text().strip()
                                logging.info(f"Found fuel type with alternative selector: {fuel_type}")
                                
                        # Evaluasi numerik dari engine_cc - ekstrak angkanya saja
                        if engine_cc:
                            try:
                                # Ekstrak semua digit yang ada di string
                                digits = re.findall(r'\d+', engine_cc)
                                if digits:
                                    # Gabungkan semua digit jika ada beberapa kelompok digit
                                    engine_cc = ''.join(digits)
                                    logging.info(f"Extracted numeric engine CC: {engine_cc}")
                            except Exception as e:
                                logging.warning(f"Failed to clean engine CC: {e}")
                                
                except Exception as e:
                    logging.warning(f"Gagal ambil spesifikasi: {e}")

                # Ambil gambar
                meta_imgs = self.page.query_selector_all("head > meta[name='prerender']")
                meta_img_urls = set()
                try:
                    for meta in meta_imgs:
                        content = meta.get_attribute("content")
                        if content and content.startswith("https://"):
                            meta_img_urls.add(content)
                except Exception as e:
                    logging.warning(f"Gagal ambil meta image: {e}")

                # Parse page content with BeautifulSoup
                soup = BeautifulSoup(self.page.content(), "html.parser")
                
                # Backup method: extract specs using BeautifulSoup - search all specification sections
                if engine_cc is None or fuel_type is None:
                    try:
                        # Metode 1: Cari ENGINE SPECIFICATIONS section dan cek untuk label "Fuel Type"
                        engine_headers = soup.find_all(string=lambda text: text and "ENGINE SPECIFICATIONS" in text.upper())
                        for header in engine_headers:
                            header_div = header.parent
                            if header_div:
                                parent_section = header_div.parent
                                if parent_section:
                                    # Dapatkan semua baris di section ini, kecuali header
                                    spec_rows = parent_section.select('div:not(:first-child)')
                                    
                                    # Periksa SEMUA baris untuk label "Fuel Type" yang spesifik
                                    for row in spec_rows:
                                        label = row.select_one('span:not(.u-text-bold)')
                                        if label and "FUEL TYPE" in label.text.upper():
                                            value = row.select_one('span.u-text-bold')
                                            if value:
                                                fuel_type = value.text.strip()
                                                logging.info(f"BeautifulSoup found fuel type in ENGINE SPECIFICATIONS: {fuel_type}")
                                                break
                        
                        # Metode 2: Scan semua spec tables dengan filter yang lebih ketat
                        if not fuel_type or not engine_cc:
                            spec_sections = soup.select('#tab-specifications > div')
                            
                            for section in spec_sections:
                                # Check apakah ini section ENGINE SPECIFICATIONS
                                header = section.select_one('div.u-text-bold')
                                is_engine_section = header and "ENGINE SPECIFICATIONS" in header.text.upper()
                                
                                spec_rows = section.select('div')
                                
                                for row in spec_rows:
                                    label = row.select_one('div > span:not(.u-text-bold)')
                                    value = row.select_one('div > span.u-text-bold')
                                    
                                    if label and value:
                                        label_text = label.text.strip().lower()
                                        value_text = value.text.strip()
                                        
                                        # Hanya cari fuel type dalam ENGINE SPECIFICATIONS section
                                        # atau pastikan ini adalah "fuel type", bukan "fuel consumption"
                                        if not fuel_type:
                                            if (is_engine_section and "fuel" in label_text) or "fuel type" in label_text:
                                                if "consumption" not in label_text:
                                                    fuel_type = value_text
                                                    logging.info(f"BeautifulSoup found specific fuel type: {fuel_type}")
                                            
                                        if not engine_cc and any(term in label_text for term in ['engine capacity', 'engine cc', 'displacement']):
                                            engine_cc = value_text
                                            # Ekstrak angka dari engine_cc
                                            digits = re.findall(r'\d+', engine_cc)
                                            if digits:
                                                engine_cc = ''.join(digits)
                                            logging.info(f"BeautifulSoup found engine CC: {engine_cc}")
                                            
                                # Break jika kedua nilai sudah ditemukan
                                if fuel_type and engine_cc:
                                    break
                        
                        # Metode 3: Cari elemen dengan text yang TEPAT "Fuel Type" di seluruh halaman
                        if not fuel_type:
                            # Cari lebih spesifik "Fuel Type" dan hindari "Fuel Consumption" dll
                            fuel_labels = soup.find_all(['span', 'div'], string=lambda text: text and (
                                'fuel type' in text.lower() and 
                                'consumption' not in text.lower()
                            ))
                            
                            for label in fuel_labels:
                                # Ambil dari elemen bold terdekat
                                next_elem = label.find_next('span', class_='u-text-bold')
                                if next_elem:
                                    fuel_type = next_elem.text.strip()
                                    logging.info(f"BeautifulSoup found specific fuel type by global search: {fuel_type}")
                                    break
                                
                                # Atau coba ambil dari parent yang sama
                                parent = label.parent
                                if parent:
                                    value_elem = parent.select_one('span.u-text-bold')
                                    if value_elem:
                                        fuel_type = value_elem.text.strip()
                                        logging.info(f"BeautifulSoup found fuel type from parent element: {fuel_type}")
                                        break
                        
                        # Final fallback - search for any text element containing standard fuel type keywords
                        if not fuel_type:
                            # Kata kunci khusus untuk jenis bahan bakar, bukan nilai konsumsi
                            fuel_type_values = ['petrol - unleaded', 'diesel', 'hybrid', 'electric', 'gasoline', 'ulp']
                            
                            # Prioritaskan pencarian di elemen yang ditandai sebagai nilai/value
                            bold_elements = soup.select('span.u-text-bold')
                            for elem in bold_elements:
                                text = elem.text.strip().lower()
                                if any(fuel_val in text for fuel_val in fuel_type_values):
                                    # Pastikan ini bukan angka (untuk menghindari nilai fuel consumption)
                                    if not re.match(r'^[\d\.]+$', text) and 'l/100km' not in text:
                                        fuel_type = elem.text.strip()
                                        logging.info(f"Found standard fuel type from bold elements: {fuel_type}")
                                        break
                            
                            # Jika masih tidak ditemukan, cari di semua text elemen tapi dengan filter ketat
                            if not fuel_type:
                                fuel_elements = soup.find_all(string=lambda text: text and any(fuel_val in text.lower() for fuel_val in fuel_type_values))
                                
                                for elem in fuel_elements:
                                    # Filter numerik untuk menghindari nilai konsumsi
                                    if not re.match(r'^[\d\.]+$', elem.strip()) and 'l/100km' not in elem.lower():
                                        parent = elem.parent
                                        if parent and parent.name in ['span', 'div']:
                                            fuel_type = elem.strip()
                                            logging.info(f"Found fuel type from page text (strict filtering): {fuel_type}")
                                            break
                                    
                    except Exception as e:
                        logging.warning(f"Gagal ekstraksi spesifikasi dengan BeautifulSoup: {e}")

                spans = soup.select("#listing-detail li > a > span")
                valid_spans = [span for span in spans if span.text.strip()]
                num_spans = len(valid_spans)

                for i, span in enumerate(valid_spans):
                    logging.info(f"Span {i}: {span.text.strip()}")

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

                logging.info(f"Hasil mapping: Brand={brand}, Model Group={model_group}, Model={model}, Variant={variant}")

                gallery_imgs = [img.get("src") for img in soup.select("#details-gallery img") if img.get("src")]
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

                # Konversi mileage ke integer sesuai format
                mileage_int = parse_mileage(mileage)
                
                # Bersihkan dan standarisasi engine_cc (ekstrak angka saja)
                if engine_cc:
                    try:
                        # Ekstrak semua digit dari string engine_cc
                        digits = re.findall(r'\d+', str(engine_cc))
                        if digits:
                            # Gabungkan semua digit yang ditemukan
                            engine_cc = ''.join(digits)
                            logging.info(f"Cleaned engine CC to numeric value: {engine_cc}")
                        else:
                            logging.warning(f"Invalid engine_cc value (no digits): {engine_cc}")
                            engine_cc = None
                    except Exception as e:
                        logging.warning(f"Error cleaning engine_cc: {e}")
                
                # Normalize fuel type untuk memastikan konsistensi
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
                        
                # Final validation untuk engine_cc dan fuel_type
                try:
                    # Validasi fuel_type untuk memastikan bukan nilai konsumsi atau numerik
                    if fuel_type:
                        # Jika fuel_type hanya berisi digit atau angka dengan desimal, kemungkinan itu adalah nilai konsumsi
                        if re.match(r'^[\d\.]+$', fuel_type):
                            logging.warning(f"Fuel type appears to be numeric value: {fuel_type}. Setting to default.")
                            fuel_type = "Petrol - Unleaded (ULP)"
                        
                        # Jika fuel_type mengandung karakter 'L' dan angka, kemungkinan itu adalah nilai konsumsi
                        elif 'l/' in fuel_type.lower() or 'km/l' in fuel_type.lower() or 'liter' in fuel_type.lower():
                            logging.warning(f"Fuel type appears to be consumption value: {fuel_type}. Setting to default.")
                            fuel_type = "Petrol - Unleaded (ULP)"
                            
                        # Jika fuel_type terlalu pendek (misalnya hanya "6.4"), kemungkinan itu adalah nilai konsumsi
                        elif len(fuel_type) < 4:
                            logging.warning(f"Fuel type too short, likely not valid: {fuel_type}. Setting to default.")
                            fuel_type = "Petrol - Unleaded (ULP)"
                            
                        # Pastikan fuel_type adalah salah satu dari nilai standar
                        standard_types = ["petrol", "diesel", "hybrid", "plug-in hybrid", "electric", "gas"]
                        if not any(std_type in fuel_type.lower() for std_type in standard_types):
                            logging.warning(f"Fuel type not matching standard types: {fuel_type}. Setting to default.")
                            fuel_type = "Petrol - Unleaded (ULP)"
                        
                    # Fix untuk nilai engine_cc yang invalid
                    if engine_cc:
                        cc_value = int(engine_cc)
                        # Filter nilai engine_cc yang tidak masuk akal (terlalu kecil atau besar)
                        if cc_value < 500 or cc_value > 10000:
                            logging.warning(f"Engine CC value out of reasonable range: {cc_value}")
                            engine_cc = None
                except Exception as e:
                    logging.warning(f"Error validating final values: {e}")
                    
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
                    "image": image,
                    "engine_cc": engine_cc,
                    "fuel_type": fuel_type,
                }

            except Exception as e:
                logging.error(f"Gagal scraping detail {url}: {e}")
                take_screenshot(self.page, "scrape_detail_error")
                self.retry_with_new_proxy()
                retry_count += 1

        logging.error(f"❌ Gagal mengambil data dari {url} setelah {max_retries} percobaan.")
        return None

    def sanitize_image_filename(self, url, fallback_name):
        parsed = urlparse(url)
        basename = os.path.basename(parsed.path)
        if not basename:
            basename = fallback_name
        name, ext = os.path.splitext(basename)
        if not ext:
            ext = ".jpg"
        return f"{name}{ext}"

    def download_images(self, image_urls, brand, model, variant, year, car_id, referer=None):
        """
        Download semua gambar ke folder images_carlist/brand/model/variant/year/id/
        Menggunakan requests Session dengan UA + referer agar tidak mudah di-block.
        """
        if not image_urls:
            return []

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

        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            }
        )
        if referer:
            session.headers["Referer"] = referer

        local_paths = []
        for idx, url in enumerate(image_urls):
            if not url:
                continue

            file_name = self.sanitize_image_filename(url, f"image_{idx+1}.jpg")
            file_path = base_dir / file_name

            if file_path.exists():
                local_paths.append(str(file_path))
                continue

            try:
                resp = session.get(url, timeout=30, stream=True)
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                local_paths.append(str(file_path))
            except Exception as e:
                logging.warning(f"Gagal download gambar {url}: {e}")

        return local_paths

    def save_to_db(self, car):
        try:
            self.cursor.execute(f"SELECT id, price, version, information_ads_date FROM {DB_TABLE_SCRAP} WHERE listing_url = %s", (car["listing_url"],))
            row = self.cursor.fetchone()
            now = datetime.now()
            image_urls = car.get("image") or []
            image_urls_str = json.dumps(image_urls)
            brand = (car.get("brand") or "UNKNOWN").upper().replace("-", " ")
            model_group = self.normalize_field(car.get("model_group"), "NO MODEL GROUP").upper()
            model = self.normalize_field(car.get("model"), "NO MODEL").upper()
            variant = self.normalize_field(car.get("variant"), "NO VARIANT").upper()
            car_id = None

            if row:
                car_id, old_price, version, existing_ads_date = row
                if self.download_images_locally:
                    self.download_images(image_urls, brand, model, variant, car.get("year"), car_id, car.get("listing_url"))
                ads_date_to_use = existing_ads_date or now.strftime("%Y-%m-%d")
                self.cursor.execute(f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model_group=%s, model=%s, variant=%s, information_ads=%s,
                        location=%s, condition=%s, price=%s, year=%s, mileage=%s,
                        transmission=%s, seat_capacity=%s, engine_cc=%s, fuel_type=%s,
                        last_scraped_at=%s, last_status_check=%s, images=%s, information_ads_date=%s
                    WHERE id=%s
                """, (
                    brand, model_group, model, variant, car.get("information_ads"),
                    car.get("location"), car.get("condition"), car.get("price"), car.get("year"), car.get("mileage"),
                    car.get("transmission"), car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"),
                    now, now, image_urls_str, ads_date_to_use, car_id
                ))
            else:
                current_date = now.strftime("%Y-%m-%d")
                self.cursor.execute(f"""
                    INSERT INTO {DB_TABLE_SCRAP} (
                        listing_url, brand, model_group, model, variant, information_ads, location, condition,
                        price, year, mileage, transmission, seat_capacity, engine_cc, fuel_type, version, images, information_ads_date, last_scraped_at, last_status_check
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    car["listing_url"], brand, model_group, model, variant,
                    car.get("information_ads"), car.get("location"), car.get("condition"), car.get("price"),
                    car.get("year"), car.get("mileage"), car.get("transmission"),
                    car.get("seat_capacity"), car.get("engine_cc"), car.get("fuel_type"), 1, image_urls_str, current_date, now, now
                ))
                car_id = self.cursor.fetchone()[0]
                if self.download_images_locally:
                    self.download_images(image_urls, brand, model, variant, car.get("year"), car_id, car.get("listing_url"))

            self.conn.commit()
            logging.info(f"✅ Data untuk {car['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan ke database: {e}")

    def scrape_all_brands(self, start_page=1, pages=None, max_main_page_retries=3):
        self.reset_scraping()
        base_url = os.getenv("CARLISTMY_LISTING_URL")
        limit_scrap = int(os.getenv("LIMIT_SCRAP", "0"))

        if not base_url:
            logging.error("❌ CARLISTMY_LISTING_URL belum di-set di .env")
            return

        retries = 0
        success = False

        while retries < max_main_page_retries and not success:
            self.init_browser()

            paginated_url = base_url
            logging.info(f"📄 Scraping halaman utama: {paginated_url}")
            try:
                self.page.goto(paginated_url, timeout=60000)
                time.sleep(7)
            except Exception as e:
                logging.warning(f"❌ Gagal memuat halaman {paginated_url}: {e}")
                take_screenshot(self.page, f"page_load_error_retry_{retries+1}")
                self.quit_browser()
                retries += 1
                continue

            html = self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            listing_divs = soup.select('[id^="listing_"]')

            if not listing_divs:
                logging.warning(f"📄 Ditemukan 0 listing URL di halaman utama pada attempt ke-{retries+1}")
                take_screenshot(self.page, f"no_listing_page_retry_{retries+1}")
                self.quit_browser()
                retries += 1
                continue
            else:
                success = True

        if not success:
            logging.error(f"❌ Gagal mendapatkan listing URL setelah {max_main_page_retries} attempt.")
            return

        # Ambil semua listing URL
        url_tag_price_list = []
        for div in listing_divs:
            link_elem = div.select_one("h2 a")
            if link_elem:
                href = link_elem.get("href")
                if href:
                    if href.startswith("/"):
                        href = "https://www.carlist.my" + href
                    tag_elem = div.select_one("span.visuallyhidden--small")
                    tag_text = tag_elem.text.strip() if tag_elem else ""
                    price_elem = div.select_one(".listing__price.delta.weight--bold")
                    price_text = price_elem.text.strip() if price_elem else ""
                    price_clean = price_text.replace('RM', '').replace(',', '').strip()
                    try:
                        price_int = int(price_clean)
                    except Exception:
                        price_int = 0
                    url_tag_price_list.append((href, tag_text, price_int))

        url_tag_price_list = list(set(url_tag_price_list))
        logging.info(f"📄 Ditemukan {len(url_tag_price_list)} listing URL di halaman utama.")

        logging.info("⏳ Menunggu selama 5-7 detik sebelum melanjutkan...")
        time.sleep(random.uniform(5, 7))

        # Statistik insert/update/skip
        total_listing = len(url_tag_price_list)
        insert_update_count = 0
        skip_count = 0

        urls_to_scrape = []
        current_date = datetime.now().strftime("%Y-%m-%d")  # Tanggal saat ini untuk information_ads_date
        
        for url, ads_tag, price in url_tag_price_list:
            if self.stop_flag:
                break
            if price == 0:
                logging.info(f"SKIP: {url} | price: 0 (tidak valid, tidak diinsert)")
                skip_count += 1
                continue

            self.cursor.execute(f"SELECT id, price, version, images FROM {DB_TABLE_SCRAP} WHERE listing_url = %s", (url,))
            result = self.cursor.fetchone()

            if not result:
                try:
                    self.cursor.execute(f"""
                        INSERT INTO {DB_TABLE_SCRAP} (listing_url, price, ads_tag, version, information_ads_date) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (url, price, ads_tag, 1, current_date))
                    self.conn.commit()
                    logging.info(f"INSERT: {url} | price: {price} | ads_tag: {ads_tag} | version: 1 | ads_date: {current_date}")
                    urls_to_scrape.append(url)
                    insert_update_count += 1
                except Exception as e:
                    self.conn.rollback()
                    logging.error(f"Gagal insert awal listing_url: {url}, error: {e}")
            else:
                db_id, old_price, old_version, images_str = result
                # Cek jika harga sama dan images kosong, scrape ulang
                if price == old_price and (images_str == '[]' or images_str is None):
                    logging.info(f"UPDATE: {url} | price sama, images kosong, akan di-scrape ulang dan update data")
                    urls_to_scrape.append(url)
                    insert_update_count += 1
                elif price != old_price and old_price is not None:
                    try:
                        new_version = (old_version or 1) + 1
                        # Update price dan version, tapi JANGAN ubah information_ads_date
                        self.cursor.execute(f"UPDATE {DB_TABLE_SCRAP} SET price=%s, version=%s WHERE id=%s", (price, new_version, db_id))
                        self.cursor.execute(f"INSERT INTO {DB_TABLE_HISTORY_PRICE} (listing_url, old_price, new_price) VALUES (%s, %s, %s)", (url, old_price, price))
                        self.conn.commit()
                        logging.info(f"UPDATE: {url} | price changed {old_price} -> {price} | version: {new_version}")
                        urls_to_scrape.append(url)
                        insert_update_count += 1
                    except Exception as e:
                        self.conn.rollback()
                        logging.error(f"Gagal update price/version atau insert price_history untuk {url}: {e}")
                else:
                    logging.info(f"SKIP: {url} | price: {price} | version: {old_version}")
                    skip_count += 1

        logging.info(f"📊 Statistik listing:")
        logging.info(f"Total ditemukan: {total_listing}")
        logging.info(f"Insert/update: {insert_update_count}")
        logging.info(f"Skip: {skip_count}")

        logging.info(f"Akan scrape detail {len(urls_to_scrape)} listing (baru atau harga berubah) di halaman utama.")

        total_scraped = 0
        for url in urls_to_scrape:
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

        self.quit_browser()
        logging.info("✅ Proses scraping selesai.")

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
