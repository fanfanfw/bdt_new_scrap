import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

def get_connection():
    try:
        print(f"DB_NAME: {os.getenv('DB_NAME_CARLIST')}")
        print(f"DB_USER: {os.getenv('DB_USER')}")
        print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
        print(f"DB_HOST: {os.getenv('DB_HOST')}")
        print(f"DB_PORT: {os.getenv('DB_PORT')}")

        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME_CARLIST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        # Validasi koneksi
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()

        print("✅ Koneksi ke database berhasil dan valid.")
        return conn

    except Exception as e:
        print(f"❌ Error koneksi ke database: {e}")
        raise e
