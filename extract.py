import pandas as pd
import sqlite3
import os

def load_csv():
    """
    Loads source CSV data into SQLite files
    located in the staging area.
    No transformation is applied at this stage.
    """

    BASE_PATH = "data"

    # Source folders
    JAPAN_SOURCE = os.path.join(BASE_PATH, "source", "japan_store")
    MYANMAR_SOURCE = os.path.join(BASE_PATH, "source", "myanmar_store")

    # Staging databases
    JAPAN_STAGING_DB = os.path.join(BASE_PATH, "Staging", "japan_staging_area.db")
    MYANMAR_STAGING_DB = os.path.join(BASE_PATH, "Staging", "myanmar_staging_area.db")

    # Japan
    conn_japan = sqlite3.connect(JAPAN_STAGING_DB)
    for file in os.listdir(JAPAN_SOURCE):
        if file.endswith(".csv"):
            csv_path = os.path.join(JAPAN_SOURCE, file)
            table_name = file.replace(".csv", "")
            df = pd.read_csv(csv_path)
            df.to_sql(table_name, conn_japan, if_exists="replace", index=False)
    conn_japan.close()

    # Myanmar
    conn_myanmar = sqlite3.connect(MYANMAR_STAGING_DB)
    for file in os.listdir(MYANMAR_SOURCE):
        if file.endswith(".csv"):
            csv_path = os.path.join(MYANMAR_SOURCE, file)
            table_name = file.replace(".csv", "")
            df = pd.read_csv(csv_path)
            df.to_sql(table_name, conn_myanmar, if_exists="replace", index=False)
    conn_myanmar.close()

    print("Extraction complete: CSV data loaded into staging databases.")


# Run extraction
if __name__ == "__main__":
    load_csv()
