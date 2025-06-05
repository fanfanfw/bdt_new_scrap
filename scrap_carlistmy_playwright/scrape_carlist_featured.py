import asyncio
import random
import os
from dotenv import load_dotenv
from pathlib import Path
from playwright.async_api import async_playwright

# Load .env dari parent folder
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

PROXIES = os.getenv("CUSTOM_PROXIES").split(",")

def get_random_proxy():
    proxy = random.choice(PROXIES)
    ip, port, user, password = proxy.split(":")
    return {
        "server": f"http://{ip}:{port}",
        "username": user,
        "password": password
    }

async def scrape_featured_listing_urls():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        page_number = 1
        listing_urls = []

        while True:
            proxy_config = get_random_proxy()
            context = await browser.new_context(proxy=proxy_config)
            page = await context.new_page()

            url = f"https://www.carlist.my/cars-for-sale/malaysia?page_size=25&page_number={page_number}&sort=modification_date_search.desc"
            print(f"\nScraping Page {page_number} with proxy {proxy_config['server']}")

            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_selector('[id^="listing_"]', timeout=30000)

                listings = await page.query_selector_all('[id^="listing_"]')
                print(f"Found {len(listings)} listings on page {page_number}")

                for listing in listings:
                    tag_elem = await listing.query_selector("span.visuallyhidden--small")
                    tag_text = (await tag_elem.inner_text()).strip() if tag_elem else ""
                    print(f" - Tag: {tag_text}")

                    if tag_text == "Featured":
                        link_elem = await listing.query_selector("h2 a")
                        if link_elem:
                            href = await link_elem.get_attribute("href")
                            if href:
                                listing_urls.append(href)
                                print(f"   ✓ {href}")

                next_button = await page.query_selector("li.next a")
                if not next_button:
                    print("No more pages. Scraping complete.")
                    break

                await page.close()
                await context.close()
                page_number += 1

            except Exception as e:
                print(f"Error on page {page_number}: {e}")
                await page.close()
                await context.close()
                break

        await browser.close()
        print(f"\nTotal Featured Listings: {len(listing_urls)}")

if __name__ == "__main__":
    asyncio.run(scrape_featured_listing_urls())