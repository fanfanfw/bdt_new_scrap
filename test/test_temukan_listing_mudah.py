import asyncio
import aiohttp
import os
import random
from urllib.parse import urlparse
from playwright.async_api import async_playwright

BASE_URL = "https://www.mudah.my/malaysia/cars-for-sale/nissan/almera"

async def download_image(url, session, file_path):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(await response.read())
                print(f"Downloaded: {file_path}")
            else:
                print(f"Gagal download: {url} - Status: {response.status}")
    except Exception as e:
        print(f"Error download {url}: {str(e)}")

async def scrape_listing_urls(page):
    try:
        await page.wait_for_selector('[data-testid^="listing-ad-item-"]', timeout=30000)
        listing_urls = []

        listing_containers = await page.query_selector_all('[data-testid^="listing-ad-item-"]')
        for container in listing_containers:
            try:
                time_text = await container.inner_text()
                if "Today" in time_text:
                    a_tag = await container.query_selector('a[href]')
                    if a_tag:
                        href = await a_tag.get_attribute("href")
                        if href and href.startswith("https://www.mudah.my/"):
                            listing_urls.append(href)
            except:
                continue
        return list(set(listing_urls))
    except Exception as e:
        print(f"Error saat mengambil URL listing: {str(e)}")
        return []

async def scrape_listing_images(context, url, session):
    page = await context.new_page()
    print(f"\nMengunjungi: {url}")

    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_selector('body', state='attached', timeout=15000)

        # Versi 1: Klik tombol "Show All"
        show_all_clicked = False
        try:
            show_all_button = await page.wait_for_selector(
                "#ad_view_gallery a[data-action-step='17']",
                timeout=5000
            )
            if show_all_button:
                await show_all_button.click()
                print("Tombol 'Show All' diklik (metode 1)")
                show_all_clicked = True
                await page.wait_for_timeout(3000)
        except:
            pass

        # Versi 2: Klik berdasarkan teks jika metode 1 gagal
        if not show_all_clicked:
            try:
                show_all_button = await page.query_selector("button:has-text('Show All'), a:has-text('Show All')")
                if show_all_button:
                    await show_all_button.scroll_into_view_if_needed()
                    await show_all_button.click()
                    print("Tombol 'Show All' diklik (metode 2)")
                    show_all_clicked = True
                    await page.wait_for_timeout(3000)
            except:
                pass

        # Versi 3: Klik thumbnail utama jika semua gagal
        if not show_all_clicked:
            try:
                main_image_div = await page.query_selector("#ad_view_gallery div[data-action-step='1']")
                if main_image_div:
                    await main_image_div.click()
                    print("Gambar utama galeri diklik (metode 3)")
                    await page.wait_for_timeout(3000)
            except:
                print("Tidak bisa klik gambar utama sebagai fallback")

        # Scroll
        for _ in range(3):
            await page.mouse.wheel(0, random.randint(500, 1000))
            await page.wait_for_timeout(1000)

        try:
            await page.wait_for_selector("#tabpanel-0", timeout=5000)
        except:
            print("Gallery utama tidak terdeteksi")
            await page.close()
            return

        # Ambil gambar
        image_divs = await page.query_selector_all("#tabpanel-0 > div > div > div > div[data-index]")
        print(f"Ditemukan {len(image_divs)} div dengan data-index.")

        image_urls = set()
        for div in image_divs:
            try:
                img = await div.query_selector("img")
                if img:
                    src = await img.get_attribute("src")
                    if src and src.startswith(('http', '//')):
                        clean_url = src.split('?')[0]
                        if not clean_url.startswith('http'):
                            clean_url = f"https:{clean_url}"
                        image_urls.add(clean_url)
            except:
                continue

        print(f"Ditemukan {len(image_urls)} gambar unik.")

        if image_urls:
            path = urlparse(url).path
            listing_id = path.split("-")[-1].replace(".htm", "")
            folder_path = os.path.join("images", listing_id)

            for idx, img_url in enumerate(image_urls):
                file_path = os.path.join(folder_path, f"image_{idx + 1}.jpg")
                await download_image(img_url, session, file_path)
        else:
            print("Tidak ada gambar yang ditemukan")

    except Exception as e:
        print(f"\u274c Gagal proses {url}: {str(e)}")
    finally:
        await page.close()

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
        
        session = aiohttp.ClientSession()

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
                    listing_urls = await scrape_listing_urls(page)
                    await page.close()
                    
                    if not listing_urls:
                        print("✅ Tidak ditemukan listing dengan tag 'Today'. Menghentikan proses.")
                        success = True
                        break
                    
                    for url in listing_urls:
                        await scrape_listing_images(context, url, session)
                        sleep_time = random.uniform(7.0, 12.0)
                        print(f"Tidur selama {sleep_time:.1f} detik...\n")
                        await asyncio.sleep(sleep_time)
                    
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

        await session.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())