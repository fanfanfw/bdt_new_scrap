import os
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

load_dotenv(override=True)

class DataArchiver:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(f"logs/data_archiver_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler()
            ]
        )
    
    def get_connection(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME_CARLIST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            self.cursor = self.conn.cursor()
            logging.info("✅ Koneksi ke database berhasil")
        except Exception as e:
            logging.error(f"❌ Error koneksi ke database: {e}")
            raise e
    
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def create_archive_tables(self):
        """Membuat tabel arsip untuk semua tabel utama"""
        archive_tables = {
            'cars_scrap_carlistmy': 'cars_scrap_carlistmy_archive',
            'cars_scrap_mudahmy': 'cars_scrap_mudahmy_archive', 
            'price_history_scrap_carlistmy': 'price_history_scrap_carlistmy_archive',
            'price_history_scrap_mudahmy': 'price_history_scrap_mudahmy_archive'
        }
        
        for original_table, archive_table in archive_tables.items():
            try:
                # Membuat tabel arsip dengan struktur yang sama
                self.cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {archive_table} 
                    (LIKE {original_table} INCLUDING ALL)
                """)
                
                # Menambahkan kolom archived_at jika belum ada
                self.cursor.execute(f"""
                    ALTER TABLE {archive_table} 
                    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """)
                
                self.conn.commit()
                logging.info(f"✅ Tabel arsip {archive_table} berhasil dibuat/diupdate")
                
            except Exception as e:
                self.conn.rollback()
                logging.error(f"❌ Error membuat tabel arsip {archive_table}: {e}")
    
    def get_old_car_records(self, table_name, months=6):
        """Mengambil records mobil yang information_ads_date > 6 bulan"""
        cutoff_date = datetime.now() - timedelta(days=months * 30)
        
        self.cursor.execute(f"""
            SELECT * FROM {table_name} 
            WHERE information_ads_date < %s
        """, (cutoff_date,))
        
        return self.cursor.fetchall()
    
    def get_table_columns(self, table_name):
        """Mengambil nama kolom dari tabel"""
        self.cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        
        return [row[0] for row in self.cursor.fetchall()]
    
    def archive_cars_data(self, cars_table, archive_table, months=6):
        """Archive data mobil"""
        try:
            # Ambil data lama yang akan diarsipkan
            old_records = self.get_old_car_records(cars_table, months)
            
            if not old_records:
                logging.info(f"ℹ️  Tidak ada data lama di tabel {cars_table}")
                return []
            
            # Ambil listing_urls untuk archiving price history
            original_columns = self.get_table_columns(cars_table)
            listing_url_index = original_columns.index('listing_url')
            listing_urls = [record[listing_url_index] for record in old_records]
            
            # LANGSUNG gunakan INSERT...SELECT untuk efficiency dan hindari cascade delete
            columns_str = ', '.join(original_columns)
            
            # Insert ke archive menggunakan INSERT...SELECT dengan upsert (hindari gagal duplikat listing_url)
            cutoff_date = datetime.now() - timedelta(days=months * 30)
            self.cursor.execute(f"""
                INSERT INTO {archive_table} ({columns_str})
                SELECT {columns_str} FROM {cars_table}
                WHERE information_ads_date < %s
                ON CONFLICT (listing_url) DO UPDATE SET
                    brand = COALESCE(EXCLUDED.brand, {archive_table}.brand),
                    model_group = COALESCE(EXCLUDED.model_group, {archive_table}.model_group),
                    model = COALESCE(EXCLUDED.model, {archive_table}.model),
                    variant = COALESCE(EXCLUDED.variant, {archive_table}.variant),
                    price = COALESCE(EXCLUDED.price, {archive_table}.price),
                    mileage = COALESCE(EXCLUDED.mileage, {archive_table}.mileage),
                    year = COALESCE(EXCLUDED.year, {archive_table}.year),
                    transmission = COALESCE(EXCLUDED.transmission, {archive_table}.transmission),
                    seat_capacity = COALESCE(EXCLUDED.seat_capacity, {archive_table}.seat_capacity),
                    engine_cc = COALESCE(EXCLUDED.engine_cc, {archive_table}.engine_cc),
                    fuel_type = COALESCE(EXCLUDED.fuel_type, {archive_table}.fuel_type),
                    information_ads = COALESCE(EXCLUDED.information_ads, {archive_table}.information_ads),
                    information_ads_date = COALESCE(EXCLUDED.information_ads_date, {archive_table}.information_ads_date),
                    location = COALESCE(EXCLUDED.location, {archive_table}.location),
                    condition = COALESCE(EXCLUDED.condition, {archive_table}.condition),
                    last_scraped_at = COALESCE(EXCLUDED.last_scraped_at, {archive_table}.last_scraped_at),
                    last_status_check = COALESCE(EXCLUDED.last_status_check, {archive_table}.last_status_check),
                    images = COALESCE(EXCLUDED.images, {archive_table}.images),
                    archived_at = NOW()
                WHERE NOT (
                    EXCLUDED.brand IS NULL AND EXCLUDED.model IS NULL AND EXCLUDED.variant IS NULL
                    AND EXCLUDED.price IS NULL AND EXCLUDED.mileage IS NULL AND EXCLUDED.year IS NULL
                )
            """, (cutoff_date,))
            
            inserted_count = self.cursor.rowcount
            
            # Hapus dari tabel asli (ini akan trigger cascade delete untuk price history)
            self.cursor.execute(f"""
                DELETE FROM {cars_table} 
                WHERE information_ads_date < %s
            """, (cutoff_date,))
            
            deleted_count = self.cursor.rowcount
            
            self.conn.commit()
            logging.info(f"✅ {inserted_count} record berhasil diarsipkan dari {cars_table}")
            logging.info(f"   - Inserted: {inserted_count}, Deleted: {deleted_count}")
            
            return listing_urls  # Return listing_urls for price history archiving
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error archiving {cars_table}: {e}")
            return []
    
    def archive_price_history_data(self, price_history_table, archive_table, listing_urls):
        """Archive data price history berdasarkan listing_urls yang sudah diarsipkan"""
        if not listing_urls:
            return
            
        try:
            # Cek dulu apakah ada data yang perlu diarsipkan
            urls_str = ', '.join(['%s'] * len(listing_urls))
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {price_history_table} 
                WHERE listing_url IN ({urls_str})
            """, listing_urls)
            
            price_count = self.cursor.fetchone()[0]
            
            if price_count == 0:
                logging.info(f"ℹ️  Tidak ada data price history untuk listing_urls yang diarsipkan di tabel {price_history_table}")
                return
            
            # Insert ke tabel arsip menggunakan INSERT...SELECT (lebih efisien)
            self.cursor.execute(f"""
                INSERT INTO {archive_table} 
                SELECT ph.*, CURRENT_TIMESTAMP as archived_at
                FROM {price_history_table} ph
                WHERE ph.listing_url IN ({urls_str})
            """, listing_urls)
            
            inserted_count = self.cursor.rowcount
            
            # Hapus dari tabel asli
            self.cursor.execute(f"""
                DELETE FROM {price_history_table} 
                WHERE listing_url IN ({urls_str})
            """, listing_urls)
            
            deleted_count = self.cursor.rowcount
            
            self.conn.commit()
            logging.info(f"✅ {inserted_count} price history record berhasil diarsipkan dari {price_history_table}")
            logging.info(f"   - Inserted: {inserted_count}, Deleted: {deleted_count}")
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error archiving price history {price_history_table}: {e}")

    def log_price_changes_to_archive(self, cars_table, archive_table, price_history_archive_table, cutoff_date):
        """
        Catat perubahan harga antara tabel utama dan arsip jika tidak ada catatan history di arsip.
        Dipakai saat listing sudah ada di arsip dan akan diarsip ulang dengan harga baru.
        """
        try:
            self.cursor.execute(f"""
                INSERT INTO {price_history_archive_table} (old_price, new_price, changed_at, listing_url, archived_at)
                SELECT a.price AS old_price,
                       c.price AS new_price,
                       NOW() AS changed_at,
                       c.listing_url,
                       NOW() AS archived_at
                FROM {cars_table} c
                JOIN {archive_table} a ON a.listing_url = c.listing_url
                WHERE c.information_ads_date < %s
                  AND a.price IS DISTINCT FROM c.price
                  AND NOT EXISTS (
                      SELECT 1 FROM {price_history_archive_table} ph
                      WHERE ph.listing_url = c.listing_url
                        AND ph.old_price = a.price
                        AND ph.new_price = c.price
                  )
            """, (cutoff_date,))
            
            logged = self.cursor.rowcount
            if logged > 0:
                logging.info(f"  📝 {logged} perubahan harga dicatat ke {price_history_archive_table} (arsip vs tabel utama)")
            else:
                logging.info(f"  ℹ️  Tidak ada perubahan harga baru antara {cars_table} dan {archive_table}")
        
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error mencatat perubahan harga ke arsip {price_history_archive_table}: {e}")
    
    def run_archive_process(self, months=6):
        """Menjalankan proses archiving lengkap"""
        try:
            self.get_connection()
            
            logging.info(f"🚀 Memulai proses archiving data yang lebih lama dari {months} bulan...")
            
            # Buat tabel arsip
            self.create_archive_tables()
            
            # Archive data carlistmy
            logging.info("📦 Archiving data carlistmy...")
            
            # PERTAMA: Archive price history dulu sebelum cars (hindari cascade delete)
            cutoff_date = datetime.now() - timedelta(days=months * 30)
            
            # Archive price history carlistmy berdasarkan cars yang akan diarsip
            logging.info("  📋 Archiving price history carlistmy terlebih dahulu...")
            self.cursor.execute(f"""
                INSERT INTO price_history_scrap_carlistmy_archive 
                SELECT ph.*, CURRENT_TIMESTAMP as archived_at
                FROM price_history_scrap_carlistmy ph
                WHERE EXISTS (
                    SELECT 1 FROM cars_scrap_carlistmy c
                    WHERE c.listing_url = ph.listing_url
                    AND c.information_ads_date < %s
                )
            """, (cutoff_date,))
            
            price_inserted_carlist = self.cursor.rowcount
            
            if price_inserted_carlist > 0:
                # Delete price history yang sudah diarsip
                self.cursor.execute(f"""
                    DELETE FROM price_history_scrap_carlistmy ph
                    WHERE EXISTS (
                        SELECT 1 FROM cars_scrap_carlistmy c
                        WHERE c.listing_url = ph.listing_url
                        AND c.information_ads_date < %s
                    )
                """, (cutoff_date,))
                price_deleted_carlist = self.cursor.rowcount
                logging.info(f"  ✅ {price_inserted_carlist} price history carlistmy diarsipkan (inserted: {price_inserted_carlist}, deleted: {price_deleted_carlist})")
            else:
                logging.info("  ℹ️  Tidak ada price history carlistmy yang perlu diarsipkan")
            
            # Catat perubahan harga jika listing sudah ada di arsip dengan harga berbeda
            self.log_price_changes_to_archive(
                'cars_scrap_carlistmy',
                'cars_scrap_carlistmy_archive',
                'price_history_scrap_carlistmy_archive',
                cutoff_date
            )
            
            # KEDUA: Archive cars data
            self.archive_cars_data(
                'cars_scrap_carlistmy', 
                'cars_scrap_carlistmy_archive', 
                months
            )
            
            # Archive data mudahmy
            logging.info("📦 Archiving data mudahmy...")
            
            # Archive price history mudahmy dulu
            logging.info("  📋 Archiving price history mudahmy terlebih dahulu...")
            self.cursor.execute(f"""
                INSERT INTO price_history_scrap_mudahmy_archive 
                SELECT ph.*, CURRENT_TIMESTAMP as archived_at
                FROM price_history_scrap_mudahmy ph
                WHERE EXISTS (
                    SELECT 1 FROM cars_scrap_mudahmy c
                    WHERE c.listing_url = ph.listing_url
                    AND c.information_ads_date < %s
                )
            """, (cutoff_date,))
            
            price_inserted_mudah = self.cursor.rowcount
            
            if price_inserted_mudah > 0:
                # Delete price history yang sudah diarsip
                self.cursor.execute(f"""
                    DELETE FROM price_history_scrap_mudahmy ph
                    WHERE EXISTS (
                        SELECT 1 FROM cars_scrap_mudahmy c
                        WHERE c.listing_url = ph.listing_url
                        AND c.information_ads_date < %s
                    )
                """, (cutoff_date,))
                price_deleted_mudah = self.cursor.rowcount
                logging.info(f"  ✅ {price_inserted_mudah} price history mudahmy diarsipkan (inserted: {price_inserted_mudah}, deleted: {price_deleted_mudah})")
            else:
                logging.info("  ℹ️  Tidak ada price history mudahmy yang perlu diarsipkan")
            
            # Catat perubahan harga jika listing sudah ada di arsip dengan harga berbeda
            self.log_price_changes_to_archive(
                'cars_scrap_mudahmy',
                'cars_scrap_mudahmy_archive',
                'price_history_scrap_mudahmy_archive',
                cutoff_date
            )
            
            # Archive cars mudahmy
            self.archive_cars_data(
                'cars_scrap_mudahmy',
                'cars_scrap_mudahmy_archive',
                months
            )
            
            logging.info("✅ Proses archiving selesai!")
            
        except Exception as e:
            logging.error(f"❌ Error dalam proses archiving: {e}")
        finally:
            self.close_connection()
    
    def dry_run_archive(self, months=6):
        """Simulasi archiving tanpa benar-benar memindahkan data"""
        try:
            self.get_connection()
            
            logging.info(f"🔍 Simulasi archiving data yang lebih lama dari {months} bulan...")
            
            tables = ['cars_scrap_carlistmy', 'cars_scrap_mudahmy']
            
            total_cars_to_archive = 0
            total_price_history_to_archive = 0
            
            for cars_table in tables:
                # Hitung jumlah mobil yang akan diarsipkan
                old_records = self.get_old_car_records(cars_table, months)
                cars_count = len(old_records)
                
                if cars_count > 0:
                    # Ambil kolom untuk mendapat listing_url
                    original_columns = self.get_table_columns(cars_table)
                    listing_url_index = original_columns.index('listing_url')
                    listing_urls = [record[listing_url_index] for record in old_records]
                    
                    # Hitung jumlah price history yang akan diarsipkan
                    price_history_table = f"price_history_scrap_{cars_table.split('_')[-1]}"
                    urls_str = ', '.join(['%s'] * len(listing_urls))
                    self.cursor.execute(f"""
                        SELECT COUNT(*) FROM {price_history_table} 
                        WHERE listing_url IN ({urls_str})
                    """, listing_urls)
                    price_count = self.cursor.fetchone()[0]
                    
                    logging.info(f"  {cars_table}: {cars_count} records akan diarsipkan")
                    logging.info(f"  {price_history_table}: {price_count} records akan diarsipkan")
                    
                    total_cars_to_archive += cars_count
                    total_price_history_to_archive += price_count
                else:
                    logging.info(f"  {cars_table}: Tidak ada data yang perlu diarsipkan")
            
            logging.info(f"\n📊 Total yang akan diarsipkan:")
            logging.info(f"  Total mobil: {total_cars_to_archive} records")
            logging.info(f"  Total price history: {total_price_history_to_archive} records")
            
        except Exception as e:
            logging.error(f"❌ Error dalam dry run: {e}")
        finally:
            self.close_connection()
    
    def get_archive_statistics(self):
        """Menampilkan statistik data arsip"""
        try:
            self.get_connection()
            
            tables = [
                'cars_scrap_carlistmy_archive',
                'cars_scrap_mudahmy_archive',
                'price_history_scrap_carlistmy_archive', 
                'price_history_scrap_mudahmy_archive'
            ]
            
            logging.info("📊 Statistik data arsip:")
            
            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    
                    # Ambil tanggal arsip terbaru dan terlama
                    self.cursor.execute(f"""
                        SELECT MIN(archived_at), MAX(archived_at) 
                        FROM {table} 
                        WHERE archived_at IS NOT NULL
                    """)
                    date_range = self.cursor.fetchone()
                    
                    logging.info(f"  {table}: {count} records")
                    if date_range[0]:
                        logging.info(f"    Periode arsip: {date_range[0]} - {date_range[1]}")
                        
                except Exception as e:
                    logging.warning(f"  {table}: Tabel belum ada atau error - {e}")
            
        except Exception as e:
            logging.error(f"❌ Error mendapatkan statistik: {e}")
        finally:
            self.close_connection()

if __name__ == "__main__":
    archiver = DataArchiver()
    
    # Tampilkan statistik sebelum archiving
    logging.info("📊 Statistik sebelum archiving:")
    archiver.get_archive_statistics()
    
    # Jalankan proses archiving untuk data > 3 bulan
    archiver.run_archive_process(months=3)
    
    # Tampilkan statistik setelah archiving
    logging.info("📊 Statistik setelah archiving:")
    archiver.get_archive_statistics()
