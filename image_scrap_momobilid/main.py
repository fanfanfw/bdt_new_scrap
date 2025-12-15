import argparse
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

load_dotenv(override=True)

BASE_URL = "https://momobil.id"
LISTING_URL = BASE_URL + "/mobil-bekas?brand={brand}"

IMAGE_DIR = Path("momobilid/image")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "image_scrap_momobilid.log"

DEFAULT_USER_AGENT = (
"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)
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
    """
    Parse PROXY_SCRAP env in format:
    ip:port:user:pass[,ip:port:user:pass]
    """
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
    """
    Convert relative URLs and Next.js /_next/image proxy URLs to the real Cloudinary URL.
    """
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


def build_extra_headers() -> Dict[str, str]:
    return {
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Sec-Ch-Ua": '"Google Chrome";v="142", "Chromium";v="142", "Not.A/Brand";v="99"',
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Upgrade-Insecure-Requests": "1",
    }


class MomobilImageScraper:
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
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        self.requests_proxy: Optional[Dict[str, str]] = None
        ensure_directories()

    def start(self) -> None:
        self.playwright = sync_playwright().start()
        self._launch_browser()

    def stop(self) -> None:
        try:
            if self.browser:
                self.browser.close()
        finally:
            if self.playwright:
                self.playwright.stop()

    def _next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxy_pool:
            if self.use_proxy:
                logging.info("ℹ️ Tidak ada proxy di PROXY_SCRAP, lanjut tanpa proxy.")
            return None
        self.proxy_index = (self.proxy_index + 1) % len(self.proxy_pool)
        proxy = self.proxy_pool[self.proxy_index]
        logging.info("🌐 Proxy dipakai: %s", proxy.get("server"))
        return proxy

    def _launch_browser(self) -> None:
        if self.browser:
            try:
                self.browser.close()
            except Exception as exc:  # pragma: no cover - defensive
                logging.debug("Gagal menutup browser lama: %s", exc)

        proxy_config = self._next_proxy()
        launch_kwargs = {
            "headless": self.headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security",
            ],
        }
        if proxy_config:
            launch_kwargs["proxy"] = proxy_config

        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="id-ID",
            timezone_id="Asia/Jakarta",
            extra_http_headers={
                "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        self.page = self.context.new_page()
        stealth_sync(self.page)

        if proxy_config:
            server = proxy_config["server"].replace("http://", "").replace("https://", "")
            username = proxy_config.get("username")
            password = proxy_config.get("password")
            auth = f"{username}:{password}@" if username and password else ""
            proxy_uri = f"http://{auth}{server}"
            self.requests_proxy = {"http": proxy_uri, "https": proxy_uri}
        else:
            self.requests_proxy = None

        logging.info("✅ Browser siap (headless=%s)", self.headless)

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def _is_blocked(self) -> bool:
        try:
            content = self.page.content().lower()
        except Exception:
            return False
        markers = [
            "you have been blocked",
            "sorry, you have been blocked",
            "attention required",
            "cf-browser-verification",
            "cloudflare",
        ]
        return any(marker in content for marker in markers)

    def _goto_with_retry(self, url: str, context: str, attempts: int = 3) -> bool:
        for attempt in range(1, attempts + 1):
            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
                self._sleep()
                if not self._is_blocked():
                    return True
                logging.warning("🚫 Terblokir (%s) attempt %s/%s, rotasi proxy.", context, attempt, attempts)
                self._launch_browser()
            except PlaywrightTimeoutError:
                logging.warning("⚠️ Timeout membuka %s (attempt %s/%s), rotasi proxy.", context, attempt, attempts)
                self._launch_browser()
            except Exception as exc:
                logging.warning("⚠️ Gagal membuka %s: %s (attempt %s/%s)", context, exc, attempt, attempts)
                self._launch_browser()
        logging.error("❌ Gagal mengakses %s setelah %s percobaan.", context, attempts)
        return False

    def _extract_listing_links(self) -> List[str]:
        links: List[str] = self.page.evaluate(
            """
() => {
  const anchors = Array.from(document.querySelectorAll('a[id^="hyperlink_cardProductHomepage"]'));
  return anchors
    .map(a => a.getAttribute('href') || '')
    .filter(Boolean);
}
"""
        )
        normalized: List[str] = []
        seen: Set[str] = set()
        for href in links:
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            normalized.append(full)
        return normalized

    def _click_load_more(self, current_count: int) -> bool:
        locator = self.page.get_by_text("Muat Lainnya", exact=False)
        if locator.count() == 0:
            logging.info("❌ Tombol 'Muat Lainnya' tidak ditemukan.")
            return False

        try:
            locator.first.scroll_into_view_if_needed()
            locator.first.click(timeout=5000)
            logging.debug("👉 Klik 'Muat Lainnya'...")
            self._sleep()
            self.page.wait_for_function(
                "expected => document.querySelectorAll('a[id^=\"hyperlink_cardProductHomepage\"]').length > expected",
                arg=current_count,
                timeout=15000,
            )
            return True
        except PlaywrightTimeoutError:
            logging.info("⏹️ Tidak ada penambahan iklan setelah klik.")
            return False
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
        raw_urls: List[str] = self.page.evaluate(
            """
() => {
  const found = new Set();
  const add = (value) => { if (value && typeof value === 'string') { found.add(value); } };

  document.querySelectorAll('img').forEach((img) => {
    add(img.getAttribute('src'));
    const srcset = img.getAttribute('srcset');
    if (srcset) {
      srcset.split(',').forEach(item => add(item.trim().split(' ')[0]));
    }
  });

  const nextData = document.querySelector('#__NEXT_DATA__');
  if (nextData?.textContent) {
    try {
      const json = JSON.parse(nextData.textContent);
      const walk = (node) => {
        if (!node) return;
        if (typeof node === 'string') {
          add(node);
        } else if (Array.isArray(node)) {
          node.forEach(walk);
        } else if (typeof node === 'object') {
          Object.values(node).forEach(walk);
        }
      };
      walk(json);
    } catch (e) {
      // ignore
    }
  }

  return Array.from(found);
}
"""
        )

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
                self._launch_browser()
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
        self.start()
        try:
            links = self.collect_listing_links()
            logging.info("🎯 Total detail link: %s", len(links))
            self.scrape_detail_pages(links)
        finally:
            self.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scraper gambar Momobil.id menggunakan Playwright.")
    parser.add_argument("--brand", default="TOYOTA", help="Brand filter, default: TOYOTA")
    parser.add_argument("--headless", action="store_true", help="Jalankan browser dalam mode headless.")
    parser.add_argument("--max-listings", type=int, default=None, help="Batas jumlah detail link yang diambil.")
    parser.add_argument("--min-delay", type=float, default=1.5, help="Jeda minimal antar aksi (detik).")
    parser.add_argument("--max-delay", type=float, default=3.5, help="Jeda maksimal antar aksi (detik).")
    parser.add_argument("--rotate-batch-size", type=int, default=20, help="Rotasi proxy setiap N detail (default 20, sesuai 1 halaman load).")
    parser.add_argument("--max-load-clicks", type=int, default=None, help="Batas klik 'Muat Lainnya'.")
    parser.add_argument("--no-proxy", action="store_true", help="Jalankan tanpa proxy meskipun PROXY_SCRAP ada.")
    parser.add_argument("--verbose", action="store_true", help="Aktifkan logging debug.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    configure_logging(verbose=args.verbose)

    if args.min_delay > args.max_delay:
        args.min_delay, args.max_delay = args.max_delay, args.min_delay

    scraper = MomobilImageScraper(
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
