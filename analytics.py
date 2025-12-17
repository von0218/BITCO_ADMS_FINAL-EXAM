import pandas as pd
import sqlite3
import os

BASE_PATH = "data"
PRESENTATION_DB = os.path.join(
    BASE_PATH, "Presentation", "presentation_layer.db"
)


def run_analytics():
    """
    Perform analytical queries on the presentation layer.
    This step validates ETL success and generates insights
    from the final consolidated dataset.
    """
    conn = sqlite3.connect(PRESENTATION_DB)
    df = pd.read_sql(
        "SELECT * FROM consolidated_store_data",
        conn
    )

    insights = {}

    # Insight 1: Top 3 items per country by price
    insights["top_3_items_per_country"] = (
        df.sort_values("price_usd", ascending=False)
          .groupby("country")
          .head(3)
    )

    # Insight 2: Price range (max - min) by country
    price_range = df.groupby("country")["price_usd"].agg(["min", "max"])
    insights["price_range_by_country"] = price_range.assign(
        range=lambda x: x["max"] - x["min"]
    )

    # Insight 3: Item count per country
    insights["item_count_by_country"] = df["country"].value_counts()

    # Insight 4: High-priced items proportion (> 75th percentile)
    q75 = df["price_usd"].quantile(0.75)
    insights["high_price_item_ratio"] = (
        df[df["price_usd"] > q75]["country"].value_counts()
        / df["country"].value_counts()
    ).fillna(0).round(2)

    # Insight 5: Price distribution summary
    insights["price_distribution"] = df["price_usd"].describe()

    conn.close()
    return insights


alt_insights = run_analytics()

print("Alternative Analytics complete. Insights generated:")
for key, value in alt_insights.items():
    print(f"\n{key}:\n{value}")
