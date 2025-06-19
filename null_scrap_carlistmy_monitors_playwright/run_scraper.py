import argparse
from null_scrap_carlistmy_monitors_playwright.carlist_null_service import CarlistMyNullService
from dotenv import load_dotenv

load_dotenv(override=True)

def main():
    parser = argparse.ArgumentParser(description="Scrape data ulang (null fields) dari carlist.my")
    parser.add_argument('--image-download', choices=['yes', 'no'], default='yes', help="Download images locally or not")
    parser.add_argument('--id-min', type=int, help="Mulai dari id berapa")
    parser.add_argument('--id-max', type=int, help="Sampai id berapa")

    args = parser.parse_args()

    download_images_locally = args.image_download == 'yes'
    id_min = args.id_min
    id_max = args.id_max

    scraper = CarlistMyNullService(download_images_locally=download_images_locally)
    try:
        scraper.scrape_null_entries(id_min=id_min, id_max=id_max)
    finally:
        scraper.quit_browser()
        scraper.conn.close()

if __name__ == "__main__":
    main()
