import pandas as pd
from collections import defaultdict
import os

# --- 1. Paramètres ---
input_file = "inputs/huge_orders.csv"
chunksize = 500_000

# --- 2. Accumulateurs ---
category_sales = defaultdict(float)  # total des ventes par catégorie
country_sales = defaultdict(float)   # total des ventes par pays
country_count = defaultdict(int)     # nombre de ventes par pays

# --- 3. Lecture en chunks ---
for chunk in pd.read_csv(input_file, chunksize=chunksize):
    # Ajouter colonne "total"
    chunk["total"] = chunk["quantity"] * chunk["price"]

    # --- a. Ventes par catégorie ---
    grouped = chunk.groupby("product_category")["total"].sum()
    for category, total in grouped.items():
        category_sales[category] += total

    # --- b. Moyenne des ventes par pays ---
    country_group = chunk.groupby("country")["total"].agg(["sum", "count"])
    for country, row in country_group.iterrows():
        country_sales[country] += row["sum"]
        country_count[country] += row["count"]

# --- 4. Affichage des résultats ---

print("\n✅ Total des ventes par catégorie de produit :")
for category, total in category_sales.items():
    print(f"- {category}: {total:,.2f} DH")

print("\n📊 Moyenne des ventes par pays :")
for country in country_sales:
    average = country_sales[country] / country_count[country]
    print(f"- {country}: {average:,.2f} DH (basé sur {country_count[country]:,} ventes)")


