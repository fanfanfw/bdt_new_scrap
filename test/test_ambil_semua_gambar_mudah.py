import asyncio
import aiohttp
import os
from playwright.async_api import async_playwright

URL = "https://www.mudah.my/2018-nissan-almera-1-5-a-untung-sikit-jual-111070102.htm"

async def download_image(url, session, file_path):
    async with session.get(url) as response:
        if response.status == 200:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(await response.read())
            print(f"Downloaded: {file_path}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(URL, timeout=60000)

        try:
            # Coba klik tombol "Show All"
            show_all_button = await page.query_selector("#ad_view_gallery a[data-action-step='17']")
            if show_all_button:
                await show_all_button.click()
                print("Tombol 'Show All' diklik.")
            else:
                # Jika tidak ada, klik gambar utama (gambar pertama)
                image_thumb = await page.query_selector("#ad_view_gallery div[data-action-step='1']")
                if image_thumb:
                    await image_thumb.click()
                    print("Gambar utama galeri diklik.")
                else:
                    print("Tidak ada elemen untuk dibuka (Show All maupun thumbnail).")
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"Kesalahan saat membuka galeri: {e}")

        # Ambil gambar dari galeri terbuka
        image_divs = await page.query_selector_all("#tabpanel-0 > div > div > div > div[data-index]")
        print(f"Ditemukan {len(image_divs)} div dengan data-index.")

        image_urls = set()
        for div in image_divs:
            img = await div.query_selector("img")
            if img:
                src = await img.get_attribute("src")
                if src and src.startswith("http"):
                    image_urls.add(src)

        print(f"Ditemukan {len(image_urls)} gambar.")

        async with aiohttp.ClientSession() as session:
            for idx, url in enumerate(image_urls):
                file_name = f"images/image_{idx + 1}.jpg"
                await download_image(url, session, file_name)

        await browser.close()

asyncio.run(main())
