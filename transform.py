import pandas as pd
import sqlite3
import os


BASE_PATH = "data"
STAGING_JAPAN_DB = os.path.join(BASE_PATH, "Staging/japan_staging_area.db")
STAGING_MYANMAR_DB = os.path.join(BASE_PATH, "Staging/myanmar_staging_area.db")
TRANSFORMATION_DB = os.path.join(BASE_PATH, "Transformation/transformation_layer.db")

JPY_TO_USD = 0.0067


def transform_items():
    """
    Clean and standardize item data from staging databases.
    Prices are converted to USD and schemas are unified.
    """
conn_japan = sqlite3.connect(STAGING_JAPAN_DB)
conn_myanmar = sqlite3.connect(STAGING_MYANMAR_DB)
conn_transform = sqlite3.connect(TRANSFORMATION_DB)

# Japan
japan = pd.read_sql("SELECT * FROM japan_items", conn_japan)
japan.dropna(inplace=True)
japan["price"] = japan["price"].astype(float)
japan["price_usd"] = japan["price"] * JPY_TO_USD
japan["country"] = "Japan"

japan_clean = japan[["item_id", "item_name", "price_usd", "country"]]
japan_clean.to_sql("japan_items_clean", conn_transform, if_exists="replace", index=False)


# Myanmar
myanmar = pd.read_sql("SELECT * FROM myanmar_items", conn_myanmar)
myanmar.dropna(inplace=True)
myanmar["price"] = myanmar["price"].astype(float)
myanmar["price_usd"] = myanmar["price"]
myanmar["country"] = "Myanmar"

myanmar_clean = myanmar[["item_id", "item_name", "price_usd", "country"]]
myanmar_clean.to_sql("myanmar_items_clean", conn_transform, if_exists="replace", index=False)


conn_japan.close()
conn_myanmar.close()
conn_transform.close()

# Run transformation
transform_items()
print("Transformation complete: Staging → Transformation")