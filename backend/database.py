import psycopg2

# used to get db_url from the file, db_info.env
from dotenv import load_dotenv
import os

load_dotenv("db_info.env")
DATABASE_URL = os.getenv("DB_URL")

# method to test connection
def test_connection():
  print("USING DB URL:", DATABASE_URL)
  try:
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to database")
    conn.close()
  except Exception as e:
    print("Connection failed:", e)

# opens a connection to Neon database
def get_connection():
  conn = psycopg2.connect(DATABASE_URL)
  return conn

if __name__ == "__main__":
  test_connection()