import asyncio
from playwright.async_api import async_playwright

BASE_URL = "https://www.mudah.my/malaysia/cars-for-sale?o=2"

async def scrape_listing_urls(page):
    try:
        await page.wait_for_selector('[data-testid^="listing-ad-item-"]', timeout=30000)
        listing_data = []

        listing_containers = await page.query_selector_all('[data-testid^="listing-ad-item-"]')
        for container in listing_containers:
            try:
                time_text = await container.inner_text()
                if "Today" in time_text:
                    a_tag = await container.query_selector('a[href]')
                    if a_tag:
                        href = await a_tag.get_attribute("href")
                        if href and href.startswith("https://www.mudah.my/"):
                            # Ekstrak waktu posting, misalnya: "Today, 11:23"
                            lines = time_text.splitlines()
                            time_line = next((line for line in lines if "Today" in line), None)
                            listing_data.append((href, time_line))
            except:
                continue
        # Hilangkan duplikat tapi urutannya tetap
        seen = set()
        ordered_unique = []
        for item in listing_data:
            if item not in seen:
                ordered_unique.append(item)
                seen.add(item)
        return ordered_unique
    except Exception as e:
        print(f"Error saat mengambil URL listing: {str(e)}")
        return []

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            channel="chrome",
            args=['--disable-blink-features=AutomationControlled']
        )
        
        context = await browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )

        page_num = 1
        max_retries = 2

        while True:
            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    page_url = f"{BASE_URL}?o={page_num}" if page_num > 1 else BASE_URL
                    page = await context.new_page()
                    print(f"\n📄 Memproses halaman: {page_url} (Percobaan {retry_count + 1})")

                    await page.goto(page_url, timeout=60000, wait_until="domcontentloaded")
                    listing_data = await scrape_listing_urls(page)
                    await page.close()

                    if not listing_data:
                        print("✅ Tidak ditemukan listing dengan tag 'Today'. Menghentikan proses.")
                        success = True
                        break

                    for url, time_info in listing_data:
                        print(f"{url} | Posted: {time_info}")

                    success = True
                    page_num += 1

                except Exception as e:
                    print(f"⚠️ Error pada percobaan {retry_count + 1}: {str(e)}")
                    retry_count += 1
                    if 'page' in locals():
                        await page.close()
                    await asyncio.sleep(5)

            if not success:
                print(f"Gagal setelah {max_retries} percobaan. Melanjutkan ke halaman berikutnya.")
                page_num += 1
                await asyncio.sleep(10)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
