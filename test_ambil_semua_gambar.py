import asyncio
import aiohttp
import os
from playwright.async_api import async_playwright

URL = "https://www.carlist.my/used-cars/2020-toyota-alphard-2-5-g-sc-modellista-mpv/17193638"

async def download_image(url, session, file_path):
    async with session.get(url) as response:
        if response.status == 200:
            # Pastikan direktori untuk menyimpan gambar ada
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(await response.read())
            print(f"Downloaded: {file_path}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Set True untuk tanpa tampilan browser
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(URL, timeout=60000)

        # Mengambil semua meta tags yang memiliki atribut name="prerender"
        meta_elements = await page.query_selector_all("head > meta[name='prerender']")

        # Set untuk menyimpan URL gambar unik
        unique_urls = set()

        # Membuka sesi untuk pengunduhan gambar
        async with aiohttp.ClientSession() as session:
            # Ambil konten dari setiap elemen meta
            for idx, meta in enumerate(meta_elements):
                content = await meta.get_attribute("content")
                if content and content.startswith("https://"):
                    # Pastikan URL belum ada dalam set
                    if content not in unique_urls:
                        unique_urls.add(content)
                        # Tentukan path file lokal berdasarkan index dan URL
                        file_name = f"car_images/image_{idx + 1}.webp"
                        await download_image(content, session, file_name)

        await browser.close()

asyncio.run(main())
