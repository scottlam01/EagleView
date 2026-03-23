import psycopg2
from dotenv import load_dotenv
import os

load_dotenv("db_info.env") # looks for .env file and loads into os.environ

print(os.getenv("DB_NAME"))
print(os.getenv("DB_USER"))
print(os.getenv("DB_PASSWORD"))
print(os.getenv("DB_HOST"))
print(os.getenv("DB_PORT"))

# Establish a connection to your PostgreSQL database
conn = psycopg2.connect(
  dbname=os.getenv("DB_NAME"),
  user=os.getenv("DB_USER"),
  password=os.getenv("DB_PASSWORD"),
  host=os.getenv("DB_HOST", "localhost"),
  port=os.getenv("DB_PORT", "5432")
)

# function to load csv via file path and table name
def load_csv_to_table(conn, table_name, file_path):
  cur = conn.cursor()
  
  print(f"Loading {file_path} into {table_name}...")
  
  with open(file_path, "r", encoding="utf-8") as f:
    cur.copy_expert(
      f"COPY {table_name} FROM STDIN WITH CSV HEADER",
      f
    )
  conn.commit()
  
  cur.close()

# try to copy csv into postgres
try:
  load_csv_to_table(conn, "areas", "data/processed/areas.csv")
  load_csv_to_table(conn, "occupations", "data/processed/occupations.csv")
  load_csv_to_table(conn, "jobs", "data/processed/jobs.csv")
  load_csv_to_table(conn, "rpp", "data/processed/rpp.csv")
  print("✅ All tables copied successfully!")

except Exception as e:
  conn.rollback()  # rollback everything if one fails
  print("❌ Error:", e)

finally: 
  conn.close()  # Close the database connection