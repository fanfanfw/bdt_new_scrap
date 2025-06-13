from playwright.sync_api import sync_playwright

def scrape_prices(url):
    with sync_playwright() as p:
        # Buka browser
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url)
        
        # Tunggu sampai konten dimuat
        page.wait_for_selector('[data-testid^="listing-ad-item-"]')

        # Ambil semua item listing
        listings = page.locator('[data-testid^="listing-ad-item-"]')
        price_list = []

        for i in range(listings.count()):
            listing = listings.nth(i)
            
            # Coba ambil harga penuh (prioritas utama)
            full_price = listing.locator('div.text-sm.text-black.font-normal')
            if full_price.count() > 0:
                price_text = full_price.first.text_content()
            else:
                # Jika tidak ada harga penuh, ambil harga lainnya (bulanan atau lainnya)
                other_price = listing.locator('span.text-sm.font-bold, div.text-sm.font-bold')
                if other_price.count() > 0:
                    price_text = other_price.first.text_content()
                else:
                    continue
            
            # Bersihkan teks harga
            price_clean = (
                price_text.replace('RM', '')
                .strip()
                .replace(',', '')
                .replace(' ', '')
                .split('/')[0]  # Untuk menghilangkan "/month" jika ada
                .split()[0]     # Ambil hanya angka pertama jika ada teks lain
            )
            
            try:
                price_int = int(price_clean)
                price_list.append(price_int)
            except ValueError:
                continue

        # Menampilkan semua harga dalam format integer
        if price_list:
            print("Daftar Harga (dalam RM):")
            for idx, price in enumerate(price_list, start=1):
                print(f"{idx}. {price}")
            print(f"\nTotal harga yang ditemukan: {len(price_list)}")
        else:
            print("Tidak ada harga yang ditemukan.")

        # Menutup browser
        browser.close()

# URL target
url = 'https://www.mudah.my/malaysia/cars-for-sale?o=3'
scrape_prices(url)