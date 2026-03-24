import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv("db_info.env")

DATABASE_URL = os.getenv("DB_URL")
engine = create_engine(DATABASE_URL)

# Load CSVs
areas_df = pd.read_csv("data/processed/areas.csv")
occupations_df = pd.read_csv("data/processed/occupations.csv")
jobs_df = pd.read_csv("data/processed/jobs.csv")
rpp_df = pd.read_csv("data/processed/rpp.csv")

# Upload to Neon
areas_df.to_sql("areas", engine, if_exists="append", index=False)
occupations_df.to_sql("occupations", engine, if_exists="append", index=False)
jobs_df.to_sql("jobs", engine, if_exists="append", index=False)
rpp_df.to_sql("rpp", engine, if_exists="append", index=False)

print("Data uploaded!")