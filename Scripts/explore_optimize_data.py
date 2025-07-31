import pandas as pd
import duckdb
import os

# --- CONFIG : Chemin local vers le fichier CSV ---
csv_path = "C:\\Users\\user\\Desktop\\Arkx_projects\\pandas_project\\inputs\\Amazon_products.csv"

# Vérifie que le fichier existe
if not os.path.isfile(csv_path):
    raise FileNotFoundError(f"Fichier CSV non trouvé : {csv_path}")

# --- 1. Chargement du CSV avec encodage ISO-8859-1 ---
df = pd.read_csv(csv_path, encoding='ISO-8859-1')

# --- 2. Affichage des infos ---
print(f"Nombre de lignes : {df.shape[0]}")
print(f"Nombre de colonnes : {df.shape[1]}")
print("\nUtilisation de la mémoire :")
print(df.info(memory_usage='deep'))

# --- 3. Correction encodage sur colonnes texte ---
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].apply(lambda x: x.encode('latin1', errors='ignore').decode('latin1') if isinstance(x, str) else x)

# --- 4. Renommage colonnes ---
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# --- 5. Suppression lignes sans titre ou prix ---
df = df.dropna(subset=['title', 'final_price'])

# --- 6. Conversion prix en float, filtre valeurs négatives ---
df['final_price'] = pd.to_numeric(df['final_price'], errors='coerce')
df = df[df['final_price'] >= 0]

# --- 7. Export parquet ---
parquet_path = os.path.join(os.path.dirname(csv_path), "cleaned_shopee_products.parquet")
df.to_parquet(parquet_path, index=False)
print(f"\n✅ Données nettoyées sauvegardées dans {parquet_path}")

# --- 8. Exploration simple avec DuckDB ---
print("\n🔍 Aperçu des 10 produits les plus chers :")
query = f"""
    SELECT title, final_price, rating
    FROM '{parquet_path}'
    ORDER BY final_price DESC
    LIMIT 10
"""
results = duckdb.query(query).to_df()
print(results)
