import dask.dataframe as dd
import pandas as pd
import os
import time
import psutil

# --- Fonction mémoire ---
def get_memory_mb():
    process = psutil.Process()
    return process.memory_info().rss / 1_000_000  # en Mo

# --- Dossiers ---
os.makedirs("outputs/high_price_orders", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# ================================
# 📦 Traitement avec DASK
# ================================
print("\n[DASK] Traitement en cours...")
start_dask = time.time()
mem_dask_before = get_memory_mb()

df_dask = dd.read_csv("inputs/huge_orders.csv")
df_dask["total"] = df_dask["quantity"] * df_dask["price"]

# Total des ventes par catégorie
dask_category_sales = df_dask.groupby("product_category")["total"].sum().compute()
dask_category_sales.to_csv("outputs/dask_total_sales_by_category.csv", header=True)

# Sauvegarde des commandes avec prix > 200
df_high_price = df_dask[df_dask["price"] > 200]
df_high_price.to_csv("outputs/high_price_orders/high_price_orders_*.csv", index=False)

mem_dask_after = get_memory_mb()
end_dask = time.time()

# ================================
# 🐼 Traitement avec PANDAS
# ================================
print("\n[PANDAS] Traitement en cours...")
start_pd = time.time()
mem_pd_before = get_memory_mb()

df_pd = pd.read_csv("inputs/huge_orders.csv")
df_pd["total"] = df_pd["quantity"] * df_pd["price"]

pd_category_sales = df_pd.groupby("product_category")["total"].sum()
pd_category_sales.to_csv("outputs/pandas_total_sales_by_category.csv", header=["total_sales"])

mem_pd_after = get_memory_mb()
end_pd = time.time()

# ================================
# 🔍 Résumé comparatif
# ================================
print("\n✅ Comparaison DASK vs PANDAS :\n")
print("[DASK]")
print(f"- Temps d'exécution : {end_dask - start_dask:.2f} secondes")
print(f"- Mémoire utilisée  : {mem_dask_after - mem_dask_before:.2f} Mo")

print("\n[PANDAS]")
print(f"- Temps d'exécution : {end_pd - start_pd:.2f} secondes")
print(f"- Mémoire utilisée  : {mem_pd_after - mem_pd_before:.2f} Mo")

print("\n📁 Fichiers générés dans le dossier outputs/")
