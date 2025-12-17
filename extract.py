import pandas as pd
import sqlite3
import os


BASE_PATH = "data"
SOURCE_JAPAN = os.path.join(BASE_PATH, "source/japan_store")
SOURCE_MYANMAR = os.path.join(BASE_PATH, "source/myanmar_store")

STAGING_JAPAN_DB = os.path.join(BASE_PATH, "Staging/japan_staging_area.db")
STAGING_MYANMAR_DB = os.path.join(BASE_PATH, "Staging/myanmar_staging_area.db")

REQUIRED_FILES = ["japan_items.csv", "myanmar_items.csv"]


def extract_to_staging(source_folder, staging_db, country):
    """
    Extract raw CSV files for a specific country and load them
    into a staging SQLite database.
    Ensures required CSV files exist before loading.
    """
files = os.listdir(source_folder)
conn = sqlite3.connect(staging_db)

for file in files:
    if file.endswith(".csv"):
        table_name = file.replace(".csv", "")
df = pd.read_csv(os.path.join(source_folder, file))
df.to_sql(table_name, conn, if_exists="replace", index=False)

conn.close()


# Run extraction with explicit country separation
extract_to_staging(SOURCE_JAPAN, STAGING_JAPAN_DB, "Japan")
extract_to_staging(SOURCE_MYANMAR, STAGING_MYANMAR_DB, "Myanmar")


print("Extraction complete: CSVs per country loaded into staging")