import hashlib
import time
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

def hash_listing_url_price(url, price_int):
    combined = url + str(price_int)
    return hashlib.sha256(combined.encode()).hexdigest()

def scrape_mudah_main_page(page_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print(f"🔍 Membuka halaman: {page_url}")
        page.goto(page_url, timeout=60000)
        page.wait_for_load_state('load')
        time.sleep(3)  # beri waktu render JS

        base_url = "https://www.mudah.my"

        cards = page.query_selector_all("div[data-testid^='listing-ad-item-']")
        print(f"✅ Ditemukan {len(cards)} card")

        for card in cards:
            try:
                # Cek apakah ada label 'Today'
                if not card.query_selector("span:has-text('Today')"):
                    continue

                # Ambil URL
                a_tag = card.query_selector("a[href*='mudah.my']")
                if not a_tag:
                    continue

                listing_url = a_tag.get_attribute("href")
                if listing_url.startswith("/"):
                    listing_url = urljoin(base_url, listing_url)

                # Ambil harga
                price_el = card.query_selector("div.flex.items-center.flex-wrap > div")
                if not price_el:
                    continue

                price_text = price_el.inner_text().strip()
                price_clean = price_text.replace("RM", "").replace(",", "").strip()
                price_int = int(price_clean) if price_clean.isdigit() else 0

                combined = listing_url + str(price_int)
                hashed = hash_listing_url_price(listing_url, price_int)

                print(f"[HASH] {hashed} ← {combined}")

            except Exception as e:
                print(f"[❌ ERROR] {e}")
                continue

        browser.close()

# ✅ Jalankan
if __name__ == "__main__":
    scrape_mudah_main_page("https://www.mudah.my/malaysia/cars-for-sale?o=115")
