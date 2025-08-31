import os
import time
import random
import logging
import sys
import re
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path

from .database import get_connection

load_dotenv(override=True)

DB_TABLE_PRIMARY = os.getenv("DB_TABLE_SCRAP_MUDAH", "cars_scrap")

START_DATE = datetime.now().strftime('%Y%m%d')

log_dir = Path(__file__).resolve().parents[0].parents[0] / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"tracker_mudahmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("tracker")


def take_screenshot(page, name: str):
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_mudahmy_tracker"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"

        page.screenshot(path=str(screenshot_path))
        logger.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logger.warning(f"❌ Gagal menyimpan screenshot: {e}")

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

class ListingTrackerMudahmyPlaywright:
    def __init__(self, batch_size=5):
        self.batch_size = batch_size
        self.active_selector = "#ad_view_ad_highlights h1"
        self.sold_text_indicator = "This car has already been sold."
        self.custom_proxies = get_custom_proxy_list()
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy_config(self):
        proxy_mode = os.getenv("PROXY_MODE_MUDAH", "none").lower()

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
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security"
            ],
            "slow_mo": 1000
        }

        proxy = self.build_proxy_config()
        if proxy:
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy digunakan: {proxy['server']}")
        else:
            logging.info("⚡ Browser tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US"
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)

        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def retry_with_new_proxy(self):
        try:
            self.quit_browser()
            time.sleep(5)
            self.session_id = self.generate_session_id()
            self.init_browser()

            self.page.goto("https://example.com", timeout=10000)
            if self.page.url == "about:blank":
                raise Exception("Browser masih stuck di about:blank")

            self.get_current_ip()
            logger.info("🔁 Browser reinit dengan session proxy baru.")
        except Exception as e:
            logger.error(f"Gagal retry dengan proxy baru: {e}")
            raise

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

    def random_delay(self, min_d=11, max_d=33):
        delay = random.uniform(min_d, max_d)
        logger.info(f"⏱️ Delay acak antar listing: {delay:.2f} detik")
        sys.stdout.flush()  
        time.sleep(delay)

    def is_redirected(self, title, url):
        title = title.lower().strip()
        url = url.strip()
        if (
                "cars for sale in malaysia" in title and
                "/cars-for-sale" in url
        ):
            return True
        return False

    def update_car_status(self, car_id, status, sold_at=None):
        conn = get_connection()
        if not conn:
            logger.error("Tidak bisa update status, koneksi database gagal.")
            return

        cursor = conn.cursor()
        try:
            if sold_at:
                cursor.execute(f"""
                    UPDATE {DB_TABLE_PRIMARY}
                    SET status = %s, sold_at = %s, last_status_check = %s
                    WHERE id = %s
                """, (status, sold_at, datetime.now(), car_id))
            else:
                cursor.execute(f"""
                    UPDATE {DB_TABLE_PRIMARY}
                    SET status = %s, last_status_check = %s
                    WHERE id = %s
                """, (status, datetime.now(), car_id))

            conn.commit()
            logger.info(f"> ID={car_id} => Status diupdate ke '{status}', last_status_check diperbarui.")
        except Exception as e:
            logger.error(f"❌ Error update_car_status untuk ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def extract_price_from_page(self):
        try:
            price_meta = self.page.query_selector('div[itemprop="offers"] meta[itemprop="price"]')
            if price_meta:
                price_value = price_meta.get_attribute("content")
                logger.info(f"[DEBUG] Harga dari meta content: {price_value}")
                return int(price_value)
            else:
                logger.warning("⚠️ Tidak ditemukan elemen meta[itemprop='price']")
        except Exception as e:
            logger.error(f"❌ Gagal extract harga dari halaman: {e}")
        return None

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
            logger.warning(f"❌ Gagal ekstrak highlight info: {e}")
            return None

    def normalize_model_variant(self, text):
        """Membersihkan string model/variant"""
        if not text or str(text).strip() in ["N/A", "-", ""]:
            return "NO VARIANT"
        cleaned = re.sub(r'[\-\(\)_]', ' ', text)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.upper()
        if not cleaned:
            return "NO VARIANT"
        return cleaned

    def normalize_brand_name(self, brand_str):
        """Normalize brand name by replacing dashes with spaces and cleaning up format."""
        if not brand_str or brand_str == "N/A":
            return brand_str
        try:
            normalized = (
                brand_str.replace('-', ' ')
                .replace('_', ' ')
                .strip()
                .upper()
            )
            normalized = ' '.join(normalized.split())
            logger.info(f"Brand normalized: '{brand_str}' -> '{normalized}'")
            return normalized
        except Exception as e:
            logger.warning(f"Error normalizing brand '{brand_str}': {e}")
            return brand_str

    def convert_mileage(self, mileage_str):
        """Convert mileage string to integer in km"""
        if not mileage_str or mileage_str == "N/A":
            return None
        try:
            if mileage_str.startswith("<"):
                return int(mileage_str[1:-1]) * 1000 if "k" in mileage_str else int(mileage_str[1:])
            if " - " in mileage_str:
                parts = mileage_str.split(" - ")
                max_part = parts[-1]
                if "k" in max_part:
                    return int(float(max_part.replace("k", "")) * 1000)
                return int(max_part)
            if mileage_str.startswith(">"):
                return int(mileage_str[1:-1]) * 1000 if "k" in mileage_str else int(mileage_str[1:])
            if "k" in mileage_str:
                return int(float(mileage_str.replace("k", "")) * 1000)
            return int(mileage_str.replace(",", "").replace("km", "").strip())
        except Exception as e:
            logger.warning(f"Gagal mengkonversi mileage '{mileage_str}': {e}")
            return None

    def convert_year_to_int(self, year_str):
        """Convert year string to integer"""
        if not year_str or year_str == "N/A":
            return None
        year_match = re.search(r'\d{4}', year_str)
        if year_match:
            year_int = int(year_match.group(0))
            logger.info(f"Converting year from '{year_str}' to {year_int}")
            return year_int
        return None

    def convert_information_ads_to_date(self, info_ads_str):
        """Convert information_ads text to actual date"""
        if not info_ads_str or info_ads_str.strip() in ["N/A", ""]:
            return datetime.now().strftime('%Y-%m-%d')
        
        try:
            info_text = info_ads_str.lower().strip()
            current_year = datetime.now().year
            current_date = datetime.now()
            
            # Pattern untuk "posted X mins ago", "posted X hours ago", etc.
            if "mins ago" in info_text or "min ago" in info_text:
                # Untuk menit yang lalu, ambil tanggal hari ini
                return current_date.strftime('%Y-%m-%d')
                
            elif "hours ago" in info_text or "hour ago" in info_text:
                # Untuk jam yang lalu, ambil tanggal hari ini  
                return current_date.strftime('%Y-%m-%d')
                
            elif "days ago" in info_text:
                # Extract number of days: "posted 19 days ago"
                days_match = re.search(r'(\d+)\s+days?\s+ago', info_text)
                if days_match:
                    days_ago = int(days_match.group(1))
                    target_date = current_date - timedelta(days=days_ago)
                    return target_date.strftime('%Y-%m-%d')
                return current_date.strftime('%Y-%m-%d')
                
            elif "day ago" in info_text:
                # "posted 1 day ago" = kemarin
                target_date = current_date - timedelta(days=1)
                return target_date.strftime('%Y-%m-%d')
                
            else:
                # Pattern untuk "posted 18 Mar", "posted 1 Apr", etc.
                # Extract day and month
                date_match = re.search(r'(\d+)\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', info_text)
                if date_match:
                    day = int(date_match.group(1))
                    month_str = date_match.group(2)
                    
                    # Map month abbreviations to numbers
                    month_map = {
                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                    }
                    
                    month = month_map.get(month_str, 1)
                    
                    # Tentukan tahun: jika bulan > bulan sekarang, berarti tahun lalu
                    if month > current_date.month:
                        year = current_year - 1
                    else:
                        year = current_year
                    
                    try:
                        target_date = datetime(year, month, day)
                        return target_date.strftime('%Y-%m-%d')
                    except ValueError:
                        # Invalid date, use current date
                        logger.warning(f"Invalid date constructed from '{info_ads_str}': {year}-{month}-{day}")
                        return current_date.strftime('%Y-%m-%d')
                        
            # Fallback: jika tidak match pattern apapun
            logger.warning(f"Could not parse information_ads date: '{info_ads_str}', using current date")
            return current_date.strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.error(f"Error converting information_ads to date '{info_ads_str}': {e}")
            return datetime.now().strftime('%Y-%m-%d')

    def scrape_full_listing_data_in_new_tab(self, url):
        """Scrape semua data dari halaman listing dalam tab baru - seperti mudahmy_service.py"""
        detail_page = self.context.new_page()
        try:
            logger.info(f"🆕 Opening new tab for detail scraping: {url}")
            detail_page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Check if blocked
            if (
                "Access Denied" in detail_page.title() or
                "block" in detail_page.url or
                detail_page.locator("text='Access Denied'").count() > 0 or
                detail_page.locator("text='verify you are human'").count() > 0
            ):
                logger.warning("🚨 Blokir atau captcha terdeteksi di halaman detail!")
                return None

            # Wait for specifications section
            try:
                detail_page.wait_for_selector('#ad_view_car_specifications', timeout=15000)
                time.sleep(3)
            except Exception as e:
                logger.warning(f"⚠️ Specifications section tidak ditemukan: {e}")
                return None

            # Try to expand specifications - MULTIPLE METHODS like in mudahmy_service.py
            show_more_clicked = False
            
            # Method 1: Direct selector click
            try:
                show_more_btn = detail_page.wait_for_selector(
                    "#ad_view_car_specifications button:has-text('SHOW MORE')", 
                    timeout=5000,
                    state="visible"
                )
                if show_more_btn:
                    show_more_btn.scroll_into_view_if_needed()
                    show_more_btn.click()
                    time.sleep(3)
                    if detail_page.locator("button:has-text('SHOW LESS')").count() > 0:
                        show_more_clicked = True
                        logger.info("✅ Tombol 'SHOW MORE' specifications diklik (metode 1)")
                        time.sleep(2)
            except Exception:
                logger.info("❌ Metode 1 gagal: mencoba metode berikutnya")

            # Method 2: JavaScript click
            if not show_more_clicked:
                try:
                    detail_page.evaluate("""
                        const btn = document.querySelector('#ad_view_car_specifications button');
                        if (btn && btn.innerText.includes('SHOW MORE')) {
                            btn.click();
                        }
                    """)
                    time.sleep(3)
                    if detail_page.locator("button:has-text('SHOW LESS')").count() > 0:
                        show_more_clicked = True
                        logger.info("✅ Tombol 'SHOW MORE' specifications diklik via JavaScript")
                except Exception:
                    logger.info("❌ Metode 2 gagal: mencoba metode final")

            # Method 3: DOM Manipulation (like in mudahmy_service.py)
            if not show_more_clicked:
                try:
                    detail_page.evaluate("""
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
                    logger.info("✅ Specifications diperluas via DOM manipulation")
                except Exception:
                    logger.info("❌ Semua metode gagal expand specifications")

            # Safe extraction helper
            def safe_extract(selectors, fallback="N/A"):
                for selector in selectors:
                    try:
                        if detail_page.locator(selector).count() > 0:
                            return detail_page.locator(selector).first.inner_text().strip()
                    except Exception:
                        continue
                return fallback

            # Extract all data
            data = {}
            # Extract brand dengan validasi tambahan
            brand_raw = safe_extract([
                "#ad_view_car_specifications div:nth-child(1) > div:nth-child(3)",
                "div:has-text('Brand') + div",
            ])
            # Fix brand extraction issue - jangan ambil "Model" sebagai brand
            if brand_raw and brand_raw.lower() not in ['model', 'n/a', 'brand']:
                data["brand"] = brand_raw
            else:
                data["brand"] = "N/A"
                logger.warning(f"Brand extraction issue, got: '{brand_raw}' - set to N/A")
            
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
            
            # Get highlight info for condition and information_ads
            full_info = self.get_highlight_info(detail_page)
            if not full_info:
                full_info = safe_extract([
                    "#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div > div",
                    "div.text-\\[\\#666666\\].text-xs.lg\\:text-base",
                ])
            if full_info and full_info != "N/A":
                parts = full_info.split(",", 1)
                data["condition"] = parts[0].strip()
                data["information_ads"] = parts[1].strip() if len(parts) > 1 else ""
            else:
                data["condition"] = "N/A"
                data["information_ads"] = ""

            data["location"] = safe_extract([
                "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(4) > div",
                "div.font-bold.truncate.text-sm.md\\:text-base",
                "#ad_view_ad_highlights div.font-bold.truncate",
            ])
            data["price"] = safe_extract([
                "div.flex.gap-1.md\\:items-end > div"
            ])
            data["year"] = safe_extract([
                "#ad_view_car_specifications div:nth-child(3) > div:nth-child(3)",
                "div:has-text('Year') + div",
            ])
            # Extract mileage dengan validasi
            mileage_raw = safe_extract([
                "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(3) > div",
                "div:has-text('Mileage') + div",
            ])
            # Fix mileage extraction - jangan ambil text label sebagai value
            if mileage_raw and not any(x in mileage_raw.lower() for x in ['mileage to', 'mileage', 'km to']):
                data["mileage"] = mileage_raw
            else:
                data["mileage"] = "N/A"
                logger.warning(f"Mileage extraction issue, got: '{mileage_raw}' - set to N/A")
            data["transmission"] = safe_extract([
                "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(2) > div",
                "div:has-text('Transmission') + div",
            ])
            data["seat_capacity"] = safe_extract([
                "#ad_view_car_specifications > div > div > div > div > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3)",
                "div:has-text('Seat Capacity') + div",
            ])
            data["fuel_type"] = safe_extract([
                "#ad_view_car_specifications > div > div > div:nth-child(1) > div > div > div:nth-child(2) > div:nth-child(4) > div:nth-child(3)",
                "#ad_view_car_specifications div.flex.flex-col.gap-4 div:has-text('Fuel Type') + div",
                "div:has-text('Fuel Type') + div",
            ])

            # Extract images - same approach as mudahmy_service.py
            image_urls = set()
            try:
                detail_page.wait_for_selector('#ad_view_gallery', timeout=15000)
                logger.info("🖼️ Galeri ditemukan, siap proses gambar")
                
                # Process Show All gallery (like in mudahmy_service.py)
                show_all_clicked = False
                try:
                    show_all_button = detail_page.wait_for_selector(
                        "#ad_view_gallery a[data-action-step='17']",
                        timeout=5000
                    )
                    if show_all_button:
                        show_all_button.click()
                        logger.info("✅ Tombol 'Show All' gallery diklik (metode 1)")
                        show_all_clicked = True
                        time.sleep(random.uniform(6, 9))
                except Exception as e:
                    logger.info(f"❌ Gagal klik tombol 'Show All' gallery metode 1: {e}")

                if not show_all_clicked:
                    try:
                        show_all_button = detail_page.query_selector("button:has-text('Show All'), a:has-text('Show All')")
                        if show_all_button:
                            show_all_button.scroll_into_view_if_needed()
                            show_all_button.click()
                            logger.info("✅ Tombol 'Show All' gallery diklik (metode 2)")
                            show_all_clicked = True
                            time.sleep(random.uniform(6, 9))
                    except Exception as e:
                        logger.info(f"❌ Gagal klik tombol 'Show All' gallery metode 2: {e}")

                if not show_all_clicked:
                    try:
                        main_image_div = detail_page.query_selector("#ad_view_gallery div[data-action-step='1']")
                        if main_image_div:
                            main_image_div.click()
                            logger.info("✅ Gambar utama galeri diklik (metode 3)")
                            time.sleep(random.uniform(6, 9))
                    except Exception as e:
                        logger.info(f"❌ Tidak bisa klik gambar utama sebagai fallback: {e}")

                # Extract image URLs
                image_divs = detail_page.query_selector_all("div[data-index]")
                logger.info(f"🔍 Ditemukan {len(image_divs)} div dengan data-index")
                
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
                        
                data["images"] = list(image_urls)
                if image_urls:
                    logger.info(f"✅ Berhasil ekstrak {len(image_urls)} gambar")
                else:
                    logger.warning("⚠️ Tidak ada gambar ditemukan")
                    
            except Exception as e:
                logger.warning(f"❌ Gagal memproses galeri: {e}")
                data["images"] = []

            logger.info(f"✅ Berhasil scrape data lengkap: {len(data)} fields")
            return data

        except Exception as e:
            logger.error(f"❌ Error scraping full listing data: {e}")
            return None
        finally:
            detail_page.close()

    def update_full_listing_data(self, car_id, scraped_data, listing_url):
        """Update semua data listing ke database dan catat perubahan harga jika ada"""
        conn = get_connection()
        if not conn:
            logger.error("Tidak bisa update data, koneksi database gagal.")
            return False

        cursor = conn.cursor()
        try:
            # Get current data from database
            cursor.execute(f"SELECT price FROM {DB_TABLE_PRIMARY} WHERE id = %s", (car_id,))
            current_data = cursor.fetchone()
            old_price = current_data[0] if current_data else 0

            # Convert price
            price_int = 0
            if scraped_data.get("price"):
                match_price = re.sub(r"[^\d]", "", scraped_data["price"])
                price_int = int(match_price) if match_price else 0

            # Convert mileage
            mileage_str = scraped_data.get("mileage", "")
            mileage_conv = self.convert_mileage(mileage_str)
            if mileage_conv is None:
                mileage_conv = 0

            # Normalize fields dengan length validation
            normalized_brand = self.normalize_brand_name(scraped_data.get("brand"))
            normalized_model = self.normalize_model_variant(scraped_data.get("model"))
            normalized_variant = self.normalize_model_variant(scraped_data.get("variant"))
            
            # Truncate fields yang mungkin terlalu panjang untuk database schema
            def safe_truncate(value, max_length=255):
                if value and len(str(value)) > max_length:
                    logger.warning(f"Truncating value '{value}' to {max_length} chars")
                    return str(value)[:max_length]
                return value
            
            # Apply safe truncation to all string fields
            normalized_brand = safe_truncate(normalized_brand)
            normalized_model = safe_truncate(normalized_model)
            normalized_variant = safe_truncate(normalized_variant)
            information_ads = safe_truncate(scraped_data.get("information_ads"))
            location = safe_truncate(scraped_data.get("location"))
            condition = safe_truncate(scraped_data.get("condition", "N/A"), 50)
            transmission = safe_truncate(scraped_data.get("transmission"), 20)
            seat_capacity = safe_truncate(scraped_data.get("seat_capacity"), 10)
            engine_cc = safe_truncate(scraped_data.get("engine_cc"), 20)
            fuel_type = safe_truncate(scraped_data.get("fuel_type"), 20)

            # Update all data - same columns as mudahmy_service.py
            update_query = f"""
                UPDATE {DB_TABLE_PRIMARY}
                SET brand=%s, model=%s, variant=%s,
                    information_ads=%s, location=%s,
                    price=%s, year=%s, mileage=%s,
                    transmission=%s, seat_capacity=%s,
                    last_scraped_at=%s, last_status_check=%s, condition=%s, engine_cc=%s,
                    fuel_type=%s, images=%s,
                    information_ads_date=%s
                WHERE id=%s
            """
            now_dt = datetime.now()
            # Convert information_ads text to actual date
            ads_date = self.convert_information_ads_to_date(information_ads)
            logger.info(f"Converted information_ads '{information_ads}' to date: {ads_date}")
            
            cursor.execute(update_query, (
                normalized_brand,
                normalized_model,
                normalized_variant,
                information_ads,
                location,
                price_int,
                self.convert_year_to_int(scraped_data.get("year")),
                mileage_conv,
                transmission,
                seat_capacity,
                now_dt,
                now_dt,
                condition,
                engine_cc,
                fuel_type,
                json.dumps(scraped_data.get("images", [])),
                ads_date,
                car_id
            ))

            # Insert price history if price changed
            if old_price != price_int and old_price != 0:
                try:
                    cursor.execute("""
                        INSERT INTO price_history_scrap_mudahmy (old_price, new_price, changed_at, listing_url)
                        VALUES (%s, %s, NOW(), %s)
                    """, (old_price, price_int, listing_url))
                    logger.info(f"📈 Price changed from {old_price} to {price_int}, logged to history")
                except Exception as e:
                    logger.error(f"❌ Failed to insert price history: {e}")

            conn.commit()
            logger.info(f"✅ Full data update completed for ID={car_id}")
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error updating full listing data for ID={car_id}: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def track_listings(self, start_id=1, status_filter='all'):
        conn = get_connection()
        if not conn:
            logger.error("Koneksi database gagal, tidak bisa memulai tracking.")
            return

        status_condition = {
            'all': "status IN ('active', 'unknown')",
            'active': "status = 'active'",
            'unknown': "status = 'unknown'"
        }.get(status_filter.lower(), "status IN ('active', 'unknown'")

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT id, listing_url, status
            FROM {DB_TABLE_PRIMARY}
            WHERE {status_condition}
            AND id >= %s
            AND (last_status_check IS NULL OR information_ads_date < NOW() - INTERVAL '1 days')
            ORDER BY id
        """, (start_id,))
        listings = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"📄 Total data: {len(listings)} (Filter: {status_filter})")

        url_count = 0

        for i in range(0, len(listings), self.batch_size):
            batch = listings[i:i + self.batch_size]
            self.init_browser()
            logger.info("✅ Browser siap, mulai cek listing.")

            for car_id, url, _ in batch:
                logger.info(f"🔍 Memeriksa ID={car_id} - {url}")
                redirected_sold = False

                try:
                    self.page.goto(url, wait_until="networkidle", timeout=30000)

                    if self.page.url == "about:blank":
                        logger.error("Halaman stuck di about:blank")
                        take_screenshot(self.page, "about_blank_error")
                        self.quit_browser()
                        continue

                    try:
                        current_url = self.page.evaluate("() => window.location.href")
                        title = self.page.evaluate("() => document.title")
                        logger.info(f"🔎 [Fallback JS] URL = {current_url}")
                        logger.info(f"🔎 [Fallback JS] Title = {title}")
                        if self.is_redirected(title, current_url):
                            logger.info(f"🔁 ID={car_id} => Redirect terdeteksi. Tandai sebagai SOLD.")
                            self.update_car_status(car_id, "sold", datetime.now())
                            redirected_sold = True
                    except Exception as eval_err:
                        logger.warning(f"⚠️ Gagal evaluasi fallback JS untuk ID={car_id}: {eval_err}")

                    # Hanya lakukan scraping jika status masih active (tidak redirect ke sold)
                    if not redirected_sold:
                        # Lakukan full re-scraping dalam tab baru untuk listing yang masih aktif
                        scraped_data = self.scrape_full_listing_data_in_new_tab(url)
                        if scraped_data:
                            # Update semua data ke database
                            success = self.update_full_listing_data(car_id, scraped_data, url)
                            if success:
                                logger.info(f"✅ Data lengkap berhasil diupdate untuk ID={car_id}")
                            else:
                                logger.error(f"❌ Gagal update data lengkap untuk ID={car_id}")
                        else:
                            logger.warning(f"⚠️ Gagal scrape data lengkap untuk ID={car_id}")
                    else:
                        logger.info(f"⏩ Skip scraping untuk ID={car_id} - Status sudah SOLD")

                    if not redirected_sold:
                        if self.page.locator(self.active_selector).count() > 0:
                            logger.info(f"> ID={car_id} => Aktif (H1 ditemukan)")
                            self.update_car_status(car_id, "active")
                        else:
                            content = self.page.content().lower()
                            if self.sold_text_indicator.lower() in content:
                                self.update_car_status(car_id, "sold", datetime.now())
                            else:
                                self.update_car_status(car_id, "unknown")
                                
                except TimeoutError:
                    logger.warning(f"⚠️ Timeout saat memeriksa ID={car_id}. Coba cek redirect secara manual...")
                    try:
                        current_url = self.page.evaluate("() => window.location.href")
                        title = self.page.evaluate("() => document.title")
                        logger.info(f"🔎 [Fallback JS] URL = {current_url}")
                        logger.info(f"🔎 [Fallback JS] Title = {title}")
                        if self.is_redirected(title, current_url):
                            logger.info(f"🔁 ID={car_id} => Redirect terdeteksi. Tandai sebagai SOLD.")
                            self.update_car_status(car_id, "sold", datetime.now())
                        elif current_url == url:
                            logger.info(f"✅ ID={car_id} => Masih di URL yang sama. Tandai sebagai ACTIVE.")
                            self.update_car_status(car_id, "active")
                        else:
                            logger.info(f"❓ ID={car_id} => Tidak redirect, dan tidak di URL yang sama. UNKNOWN.")
                            self.update_car_status(car_id, "unknown")
                    except Exception as inner:
                        logger.error(f"❌ Gagal fallback setelah timeout: {inner}")
                        take_screenshot(self.page, f"timeout_fallback_{car_id}")
                        self.update_car_status(car_id, "unknown")

                except Exception as e:
                    logger.error(f"❌ Gagal memeriksa ID={car_id}: {e}")
                    take_screenshot(self.page, f"error_{car_id}")
                    self.update_car_status(car_id, "unknown")

                self.random_delay()

                if url_count % random.randint(5, 9) == 0:
                    pause_duration = random.uniform(7, 10)
                    logger.info(f"⏸️ Mini pause {pause_duration:.2f} detik untuk menghindari deteksi bot...")
                    time.sleep(pause_duration)

                url_count += 1

                if url_count % 25 == 0:
                    break_time = random.uniform(606, 1122)
                    logger.info(f"💤 Sudah memeriksa {url_count} URL. Istirahat selama {break_time / 60:.2f} menit...")
                    time.sleep(break_time)

            self.quit_browser()

        logger.info("✅ Proses tracking selesai.")