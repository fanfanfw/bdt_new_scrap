import argparse
from scrap_carlistmy_monitors_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari carlist.my")
    parser.add_argument("--page", type=int, default=1, help="Halaman awal (default=1)")
    parser.add_argument("--max-pages", type=int, default=None, help="Jumlah halaman yang ingin diambil (opsional)")
    parser.add_argument("--end-page", type=int, default=None, help="Halaman akhir jika ingin descending")
    parser.add_argument("--desc", action="store_true", help="Scrape secara menurun dari halaman besar ke kecil")

    args = parser.parse_args()

    if args.desc and args.end_page is not None:
        pages = list(range(args.page, args.end_page - 1, -1))  
    elif args.max_pages:
        pages = list(range(args.page, args.page + args.max_pages))  
    else:
        pages = None  

    scraper = CarlistMyService()
    try:
        scraper.scrape_all_brands(start_page=args.page, pages=pages)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()


# python run_scraper --page 1 --max-pages 5
# python run_scraper --page 100 --end-page 50 --desc
