import pandas as pd
import sqlite3
import os

BASE_PATH = "data"
TRANSFORMATION_DB = os.path.join(BASE_PATH, "Transformation/transformation_layer.db")
PRESENTATION_DB = os.path.join(BASE_PATH, "Presentation/presentation_layer.db")


def load_presentation():
    """
    Combine transformed datasets into a single presentation table
    ready for analytics and reporting.
    """
conn_transform = sqlite3.connect(TRANSFORMATION_DB)
conn_present = sqlite3.connect(PRESENTATION_DB)

japan = pd.read_sql("SELECT * FROM japan_items_clean", conn_transform)
myanmar = pd.read_sql("SELECT * FROM myanmar_items_clean", conn_transform)

final_df = pd.concat([japan, myanmar], ignore_index=True)
final_df.to_sql("consolidated_store_data", conn_present, if_exists="replace", index=False)

conn_transform.close()
conn_present.close()

# Run load
load_presentation()
print("Load complete: Transformation → Presentation")