import pandas as pd
import sqlite3
import os

def load_presentation():
    """
    This creates the final consolidated “BIG TABLE”.
    Loads all cleaned Japan + Myanmar tables
    from the transformation DB into the presentation DB.
    """

    BASE_PATH = "data"

    # Transformation and Presentation databases
    TRANSFORMATION_DB = os.path.join(
        BASE_PATH, "Transformation", "transformation_layer.db"
    )
    PRESENTATION_DB = os.path.join(
        BASE_PATH, "Presentation", "presentation_layer.db"
    )

    # Connect to transformation database
    trans_conn = sqlite3.connect(TRANSFORMATION_DB)

    # Read transformed tables
    japan_df = pd.read_sql(
        "SELECT * FROM japan_transformed",
        trans_conn
    )
    myanmar_df = pd.read_sql(
        "SELECT * FROM myanmar_transformed",
        trans_conn
    )

    trans_conn.close()

    # Combine datasets
    consolidated_df = pd.concat(
        [japan_df, myanmar_df],
        ignore_index=True
    )

    # Load into presentation database
    pres_conn = sqlite3.connect(PRESENTATION_DB)
    consolidated_df.to_sql(
        "consolidated_store_data",
        pres_conn,
        if_exists="replace",
        index=False
    )
    pres_conn.close()

    print("Load complete: Consolidated table created in presentation layer.")


# Run load step
if __name__ == "__main__":
    load_presentation()
