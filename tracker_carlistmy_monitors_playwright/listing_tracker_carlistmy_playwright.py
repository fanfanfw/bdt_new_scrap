import os
import time
import random
import logging
import re
from datetime import datetime,timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path

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
            "headless": False,
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
                AND (last_status_check IS NULL OR last_status_check < %s)
                ORDER BY id
            """, (start_id, threshold_date))
        else:
            cursor.execute(f"""
                SELECT id, listing_url, status, price
                FROM {DB_TABLE_PRIMARY}
                WHERE status = %s AND id >= %s AND status != 'sold'
                AND (last_status_check IS NULL OR last_status_check < %s)
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

        for index, (car_id, url, current_status, old_price) in enumerate(listings, start=1):
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

                new_price = self.extract_price_from_page()
                if new_price is not None and new_price != old_price:
                    logger.info(f"💲 Harga berubah! ID={car_id}: {old_price} ➜ {new_price}")
                    self.save_price_change(old_price, new_price, url)
                    self.update_price(car_id, new_price)

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