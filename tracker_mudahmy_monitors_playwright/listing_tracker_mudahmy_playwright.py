import os
import time
import random
import logging
import sys
import re
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path

from .database import get_connection

load_dotenv(override=True)

DB_TABLE_PRIMARY = os.getenv("DB_TABLE_SCRAP_MUDAH", "cars_scrap")

START_DATE = datetime.now().strftime('%Y%m%d')

base_dir = Path(__file__).resolve().parents[2]
log_dir = base_dir / "logs"
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
            "headless": True,
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
            AND (last_scraped_at IS NULL OR last_scraped_at < NOW() - INTERVAL '30 days')
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

            for car_id, url, current_status in batch:
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

                    # Ambil dan update harga jika berubah
                    new_price = self.extract_price_from_page()
                    old_price = None
                    try:
                        conn_check = get_connection()
                        cur = conn_check.cursor()
                        cur.execute(f"SELECT price FROM {DB_TABLE_PRIMARY} WHERE id = %s", (car_id,))
                        row = cur.fetchone()
                        if row:
                            old_price = row[0]
                        cur.close()
                        conn_check.close()
                    except Exception as e:
                        logger.warning(f"Gagal ambil harga lama dari DB untuk ID={car_id}: {e}")

                    if new_price is not None and old_price is not None and new_price != old_price:
                        # Catat ke tabel history
                        try:
                            conn_hist = get_connection()
                            cur_hist = conn_hist.cursor()

                            cur_hist.execute(f"""
                                INSERT INTO price_history_scrap (car_id, old_price, new_price, changed_at)
                                VALUES (%s, %s, %s, NOW())
                            """, (car_id, old_price, new_price))
                            cur_hist.execute(f"""
                                UPDATE {DB_TABLE_PRIMARY}
                                SET price = %s
                                WHERE id = %s
                            """, (new_price, car_id))
                            conn_hist.commit()
                            cur_hist.close()
                            conn_hist.close()
                            logger.info(f"✅ Harga diupdate ke {new_price} dan history dicatat untuk ID={car_id}")
                        except Exception as e:
                            logger.error(f"❌ Gagal update harga atau insert history: {e}")
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
                    pause_duration = random.uniform(20, 60)
                    logger.info(f"⏸️ Mini pause {pause_duration:.2f} detik untuk menghindari deteksi bot...")
                    time.sleep(pause_duration)

                url_count += 1

                if url_count % 25 == 0:
                    break_time = random.uniform(606, 1122)
                    logger.info(f"💤 Sudah memeriksa {url_count} URL. Istirahat selama {break_time / 60:.2f} menit...")
                    time.sleep(break_time)

            self.quit_browser()

        logger.info("✅ Proses tracking selesai.")