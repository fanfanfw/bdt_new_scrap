import argparse
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
import undetected_chromedriver as uc
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

load_dotenv(override=True)

BASE_URL = "https://momobil.id"
LISTING_URL = BASE_URL + "/mobil-bekas?brand={brand}"

IMAGE_DIR = Path("momobilid/image")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "image_scrap_momobilid_selenium.log"

DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
USER_AGENT = os.getenv("MOMOBIL_USER_AGENT", DEFAULT_USER_AGENT)


def ensure_directories() -> None:
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging(verbose: bool = False) -> None:
    ensure_directories()
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def parse_proxies(raw: str) -> List[Dict[str, str]]:
    proxies: List[Dict[str, str]] = []
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 4:
            ip, port, user, password = parts
            proxies.append(
                {
                    "server": f"http://{ip}:{port}",
                    "username": user,
                    "password": password,
                }
            )
        elif len(parts) == 2:
            ip, port = parts
            proxies.append({"server": f"http://{ip}:{port}"})
        else:
            logging.warning("Format proxy tidak dikenali: %s", item)
    return proxies


def normalize_image_url(url: str) -> str:
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("/"):
        url = urljoin(BASE_URL, url)
    parsed = urlparse(url)
    if parsed.path.startswith("/_next/image") and parsed.query:
        inner = parse_qs(parsed.query).get("url", [None])[0]
        if inner:
            inner = unquote(inner)
            if inner.startswith("//"):
                inner = "https:" + inner
            elif inner.startswith("/"):
                inner = urljoin(BASE_URL, inner)
            return inner
    return url


def looks_like_car_image(url: str) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return (
        ("res.cloudinary.com" in lowered and "momobil" in lowered and "product" in lowered)
        or ("_next/image" in lowered and ("momobil%2fproduct" in lowered or "res.cloudinary.com" in lowered))
    )


def build_filename(image_url: str, slug: str) -> str:
    parsed = urlparse(image_url)
    name = os.path.basename(parsed.path.split("?")[0])
    if not name:
        name = "image"
    safe_slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", slug or "listing").strip("-") or "listing"
    safe_name = re.sub(r"[^a-zA-Z0-9._-]+", "-", name)
    return f"{safe_slug}_{safe_name}"


class MomobilSeleniumScraper:
    def __init__(
        self,
        brand: str,
        headless: bool,
        max_listings: Optional[int],
        min_delay: float,
        max_delay: float,
        rotate_batch_size: int,
        max_load_clicks: Optional[int],
        use_proxy: bool,
    ):
        self.brand = brand
        self.headless = headless
        self.max_listings = max_listings
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.rotate_batch_size = rotate_batch_size
        self.max_load_clicks = max_load_clicks
        self.use_proxy = use_proxy

        self.proxy_pool = parse_proxies(os.getenv("PROXY_SCRAP", "")) if use_proxy else []
        self.proxy_index = -1
        self.driver = None
        self.wait: Optional[WebDriverWait] = None
        self.requests_proxy: Optional[Dict[str, str]] = None

        ensure_directories()

    def _next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxy_pool:
            if self.use_proxy:
                logging.info("ℹ️ Tidak ada proxy di PROXY_SCRAP, lanjut tanpa proxy.")
            return None
        self.proxy_index = (self.proxy_index + 1) % len(self.proxy_pool)
        proxy = self.proxy_pool[self.proxy_index]
        logging.info("🌐 Proxy dipakai: %s", proxy.get("server"))
        return proxy

    def _launch_driver(self) -> None:
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

        proxy_cfg = self._next_proxy()
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument(f"--user-agent={USER_AGENT}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--window-size=1366,768")
        if self.headless:
            chrome_options.add_argument("--headless=new")

        if proxy_cfg:
            server = proxy_cfg["server"]
            username = proxy_cfg.get("username")
            password = proxy_cfg.get("password")
            if username or password:
                logging.warning("⚠️ Proxy dengan auth tidak diset untuk browser (Chrome CLI tidak support auth langsung).")
            else:
                chrome_options.add_argument(f"--proxy-server={server}")
                self.requests_proxy = {"http": server, "https": server}
        else:
            self.requests_proxy = None

        # Force driver to match installed Chrome 142.x
        self.driver = uc.Chrome(options=chrome_options, version_main=142, headless=self.headless)
        self.wait = WebDriverWait(self.driver, 25)
        logging.info("✅ Browser Selenium siap (headless=%s)", self.headless)

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def _is_blocked(self) -> bool:
        try:
            content = self.driver.page_source.lower()
        except Exception:
            return False
        markers = [
            "you have been blocked",
            "sorry, you have been blocked",
            "attention required",
            "cloudflare",
            "application error: a client-side exception",
        ]
        return any(m in content for m in markers)

    def _goto_with_retry(self, url: str, context: str, attempts: int = 3) -> bool:
        for attempt in range(1, attempts + 1):
            try:
                self.driver.get(url)
                self._sleep()
                if not self._is_blocked():
                    return True
                logging.warning("🚫 Terblokir (%s) attempt %s/%s, rotasi proxy.", context, attempt, attempts)
                self._launch_driver()
            except (TimeoutException, WebDriverException) as exc:
                logging.warning("⚠️ Gagal membuka %s: %s (attempt %s/%s)", context, exc, attempt, attempts)
                self._launch_driver()
            except Exception as exc:
                logging.warning("⚠️ Error koneksi ke driver (%s): %s (attempt %s/%s)", context, exc, attempt, attempts)
                self._launch_driver()
        logging.error("❌ Gagal mengakses %s setelah %s percobaan.", context, attempts)
        return False

    def _extract_listing_links(self) -> List[str]:
        anchors = self.driver.find_elements(By.CSS_SELECTOR, 'a[id^="hyperlink_cardProductHomepage"]')
        normalized: List[str] = []
        seen: Set[str] = set()
        for a in anchors:
            href = a.get_attribute("href") or ""
            if not href:
                continue
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            normalized.append(full)
        return normalized

    def _click_load_more(self, current_count: int) -> bool:
        try:
            btn = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'Muat Lainnya')]"))
            )
        except TimeoutException:
            logging.info("❌ Tombol 'Muat Lainnya' tidak ditemukan.")
            return False

        try:
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", btn)
            btn.click()
            logging.debug("👉 Klik 'Muat Lainnya'...")
            self._sleep()
            # wait for more cards
            WebDriverWait(self.driver, 15).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, 'a[id^="hyperlink_cardProductHomepage"]')) > current_count
            )
            return True
        except Exception as exc:
            logging.warning("⚠️ Gagal klik 'Muat Lainnya': %s", exc)
            return False

    def collect_listing_links(self) -> List[str]:
        target_url = LISTING_URL.format(brand=self.brand)
        logging.info("🔎 Buka halaman listing: %s", target_url)

        if not self._goto_with_retry(target_url, "listing"):
            return []

        collected: List[str] = []
        seen: Set[str] = set()
        click_count = 0

        while True:
            links_in_dom = self._extract_listing_links()
            new_added = 0
            for href in links_in_dom:
                if href in seen:
                    continue
                seen.add(href)
                collected.append(href)
                new_added += 1
            logging.info("📄 Total link terkumpul: %s", len(collected))

            if self.max_listings and len(collected) >= self.max_listings:
                logging.info("🔢 Batas max_listings tercapai.")
                break

            if self.max_load_clicks is not None and click_count >= self.max_load_clicks:
                logging.info("🔢 Batas klik 'Muat Lainnya' tercapai.")
                break

            if not self._click_load_more(len(seen)):
                break

            click_count += 1
            if new_added == 0:
                logging.info("⏹️ Tidak ada link baru, berhenti load more.")
                break

        if self.max_listings:
            return collected[: self.max_listings]
        return collected

    def _extract_images_on_page(self) -> List[str]:
        imgs = self.driver.find_elements(By.TAG_NAME, "img")
        raw_urls: List[str] = []
        for img in imgs:
            src = img.get_attribute("src")
            if src:
                raw_urls.append(src)
            srcset = img.get_attribute("srcset")
            if srcset:
                for item in srcset.split(","):
                    candidate = item.strip().split(" ")[0]
                    if candidate:
                        raw_urls.append(candidate)
        try:
            script_tag = self.driver.find_element(By.ID, "__NEXT_DATA__")
            json_text = script_tag.get_attribute("innerText") or ""
            if json_text:
                # naive scan for URLs
                for match in re.findall(r"https?://[^\"'\\s]+", json_text):
                    raw_urls.append(match)
        except Exception:
            pass

        unique: List[str] = []
        seen: Set[str] = set()
        for raw in raw_urls:
            normalized = normalize_image_url(raw)
            if not normalized or normalized in seen:
                continue
            if not looks_like_car_image(normalized):
                continue
            seen.add(normalized)
            unique.append(normalized)
        return unique

    def _download_image(self, image_url: str, slug: str) -> bool:
        filename = build_filename(image_url, slug)
        save_path = IMAGE_DIR / filename
        if save_path.exists():
            logging.debug("⏭️ Skip (sudah ada): %s", save_path)
            return False

        headers = {"User-Agent": USER_AGENT, "Referer": BASE_URL}
        try:
            resp = requests.get(image_url, headers=headers, timeout=30, proxies=self.requests_proxy)
            resp.raise_for_status()
            save_path.write_bytes(resp.content)
            logging.info("💾 Simpan: %s", save_path)
            return True
        except Exception as exc:
            logging.warning("❌ Gagal download %s: %s", image_url, exc)
            return False

    def scrape_detail_pages(self, links: List[str]) -> None:
        current_batch = 0
        for idx, link in enumerate(links, start=1):
            batch_index = (idx - 1) // self.rotate_batch_size if self.rotate_batch_size > 0 else 0
            if batch_index > current_batch:
                logging.info("🔁 Rotasi proxy untuk batch ke-%s (mulai link #%s).", batch_index + 1, idx)
                self._launch_driver()
                current_batch = batch_index

            slug = urlparse(link).path.rstrip("/").split("/")[-1] or f"listing-{idx}"
            logging.info("➡️  [%s/%s] Buka detail: %s", idx, len(links), link)
            if not self._goto_with_retry(link, f"detail {slug}", attempts=3):
                continue

            images = self._extract_images_on_page()
            if not images:
                logging.info("ℹ️  Tidak menemukan gambar pada %s", link)
                continue

            downloaded = 0
            for image_url in images:
                if self._download_image(image_url, slug):
                    downloaded += 1
                self._sleep()

            logging.info("✅ %s gambar diproses (%s baru) untuk %s", len(images), downloaded, slug)
            self._sleep()

    def run(self) -> None:
        self._launch_driver()
        try:
            links = self.collect_listing_links()
            logging.info("🎯 Total detail link: %s", len(links))
            self.scrape_detail_pages(links)
        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scraper gambar Momobil.id (Selenium fallback).")
    parser.add_argument("--brand", default="TOYOTA", help="Brand filter, default: TOYOTA")
    parser.add_argument("--headless", action="store_true", help="Jalankan browser dalam mode headless.")
    parser.add_argument("--max-listings", type=int, default=None, help="Batas jumlah detail link yang diambil.")
    parser.add_argument("--min-delay", type=float, default=1.5, help="Jeda minimal antar aksi (detik).")
    parser.add_argument("--max-delay", type=float, default=3.5, help="Jeda maksimal antar aksi (detik).")
    parser.add_argument("--rotate-batch-size", type=int, default=20, help="Rotasi proxy setiap N detail (default 20).")
    parser.add_argument("--max-load-clicks", type=int, default=None, help="Batas klik 'Muat Lainnya'.")
    parser.add_argument("--no-proxy", action="store_true", help="Jalankan tanpa proxy meskipun PROXY_SCRAP ada.")
    parser.add_argument("--verbose", action="store_true", help="Aktifkan logging debug.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    configure_logging(verbose=args.verbose)

    if args.min_delay > args.max_delay:
        args.min_delay, args.max_delay = args.max_delay, args.min_delay

    scraper = MomobilSeleniumScraper(
        brand=args.brand,
        headless=args.headless,
        max_listings=args.max_listings,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        rotate_batch_size=args.rotate_batch_size,
        max_load_clicks=args.max_load_clicks,
        use_proxy=not args.no_proxy,
    )
    scraper.run()
