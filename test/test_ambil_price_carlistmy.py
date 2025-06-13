from playwright.sync_api import sync_playwright

def scrape_prices(url):
    with sync_playwright() as p:
        # Buka browser
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url)
        
        # Tunggu sampai konten dimuat
        page.wait_for_selector('.listing__price.delta.weight--bold')

        # Ambil semua item listing
        listings = page.locator('.listing__price.delta.weight--bold')
        price_list = []

        for i in range(listings.count()):
            listing = listings.nth(i)
            
            # Ambil harga
            price_text = listing.text_content()

            # Bersihkan teks harga
            price_clean = (
                price_text.replace('RM', '')
                .strip()
                .replace(',', '')
            )
            
            try:
                # Konversi harga ke integer
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
url = 'https://www.carlist.my/cars-for-sale/malaysia?page_number=9&page_size=25'
scrape_prices(url)