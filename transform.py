import sqlite3
import pandas as pd
import os

def clean_sqlite_table():
    """
    Read from staging and perform data cleaning.
    Standardize values across datasets by converting
    Japan store prices (JPY) to USD.
    """

    BASE_PATH = "data"

    # Staging and Transformation databases
    JAPAN_STAGING_DB = os.path.join(
        BASE_PATH, "Staging", "japan_staging_area.db"
    )
    MYANMAR_STAGING_DB = os.path.join(
        BASE_PATH, "Staging", "myanmar_staging_area.db"
    )
    TRANSFORMATION_DB = os.path.join(
        BASE_PATH, "Transformation", "transformation_layer.db"
    )

    # Fixed exchange rate for standardization (exam-safe)
    JPY_TO_USD = 0.0067  # 1 JPY ≈ 0.0067 USD

    # ------------------------------
    # JAPAN STORE CLEANING
    # ------------------------------
    conn_japan = sqlite3.connect(JAPAN_STAGING_DB)
    japan_df = pd.read_sql("SELECT * FROM japan_items", conn_japan)
    conn_japan.close()

    # 1. Strip extra whitespace from column names
    japan_df.columns = japan_df.columns.str.strip()

    # 2. Strip whitespace inside text columns
    japan_df = japan_df.apply(
        lambda col: col.str.strip()
        if col.dtype == "object" else col
    )

    # 3. Replace empty strings with NaN
    japan_df.replace("", pd.NA, inplace=True)

    # 4. Drop duplicate rows
    japan_df.drop_duplicates(inplace=True)

    # Convert JPY to USD
    japan_df["price_usd"] = japan_df["price"] * JPY_TO_USD
    japan_df.drop(columns=["price"], inplace=True)

    # Add country identifier
    japan_df["country"] = "Japan"

    # ------------------------------
    # MYANMAR STORE CLEANING
    # ------------------------------
    conn_myanmar = sqlite3.connect(MYANMAR_STAGING_DB)
    myanmar_df = pd.read_sql("SELECT * FROM myanmar_items", conn_myanmar)
    conn_myanmar.close()

    # 1. Strip extra whitespace from column names
    myanmar_df.columns = myanmar_df.columns.str.strip()

    # 2. Strip whitespace inside text columns
    myanmar_df = myanmar_df.apply(
        lambda col: col.str.strip()
        if col.dtype == "object" else col
    )

    # 3. Replace empty strings with NaN
    myanmar_df.replace("", pd.NA, inplace=True)

    # 4. Drop duplicate rows
    myanmar_df.drop_duplicates(inplace=True)

    # Prices already in USD
    myanmar_df.rename(columns={"price": "price_usd"}, inplace=True)

    # Add country identifier
    myanmar_df["country"] = "Myanmar"

    # ------------------------------
    # SAVE CLEANED DATA
    # ------------------------------
    trans_conn = sqlite3.connect(TRANSFORMATION_DB)

    japan_df.to_sql(
        "japan_transformed",
        trans_conn,
        if_exists="replace",
        index=False
    )

    myanmar_df.to_sql(
        "myanmar_transformed",
        trans_conn,
        if_exists="replace",
        index=False
    )

    trans_conn.close()

    print("Transformation complete: Data cleaned and standardized.")


# Run transformation
if __name__ == "__main__":
    clean_sqlite_table()
