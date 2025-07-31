import pandas as pd
import sys

# --- 1. Chargement du CSV ---
df = pd.read_csv("C:\\Users\\user\\Desktop\\Arkx_projects\\pandas_project\\inputs\\Amazon_products.csv", encoding='utf-8')  # adapte le chemin et l'encodage

# --- 2. Inspecter la mémoire utilisée ---
print("=== df.info(deep=True) ===")
df.info(memory_usage='deep')

print("\n=== df.memory_usage(deep=True) ===")
mem_usage_bytes = df.memory_usage(deep=True)
print(mem_usage_bytes)

# --- 3. Consommation mémoire totale ---
mem_total_MB = mem_usage_bytes.sum() / 1024**2
print(f"\nMémoire totale utilisée : {mem_total_MB:.2f} MB")

# --- 4. Taille "shallow" de l’objet DataFrame ---
size_df = sys.getsizeof(df)
print(f"\nTaille de l’objet DataFrame (shallow) : {size_df} bytes")

# --- 5. Top 3 des colonnes les plus gourmandes ---
top3_cols = mem_usage_bytes.sort_values(ascending=False).head(3)
print("\nTop 3 des colonnes qui consomment le plus de mémoire :")
print(top3_cols)

# --- 6. Vérifier les types de données ---
print("\nTypes de données par colonne :")
print(df.dtypes)
