import pandas as pd
import os

# --- 1. Chargement du fichier ---
base_dir = os.path.dirname(os.path.dirname(__file__))
csv_path = os.path.join(base_dir, "inputs", "Amazon_products.csv")
df = pd.read_csv(csv_path, encoding='ISO-8859-1')

# --- 2. Standardisation des colonnes ---
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# --- 3. Comptage des valeurs manquantes ---
print("🔍 Valeurs manquantes par colonne :")
print(df.isna().sum())

# --- 4. Nettoyage des données ---
# Convertir les colonnes nécessaires
df['final_price'] = pd.to_numeric(df.get('final_price'), errors='coerce')
df['rating'] = pd.to_numeric(df.get('rating'), errors='coerce')
df['max_quantity_available'] = pd.to_numeric(df.get('max_quantity_available'), errors='coerce')

# Remplacer les stocks manquants par 0
df['max_quantity_available'] = df['max_quantity_available'].fillna(0)

# Supprimer les lignes où le prix est manquant
df_cleaned = df.dropna(subset=['final_price'])

# --- 5. Tri par note décroissante et extraction des 5 meilleurs produits ---
top5_rated = df_cleaned.sort_values(by='rating', ascending=False).head(5)

print("\n🌟 Top 5 produits les mieux notés :")
print(top5_rated[['title', 'rating', 'final_price', 'max_quantity_available']])

# --- 6. Export optionnel ---
output_path = os.path.join(base_dir, "outputs", "top5_high_rated.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
top5_rated.to_csv(output_path, index=False)
