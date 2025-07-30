import pandas as pd
import os
import re

# === 1. Chargement du dataset ===
base_dir = os.path.dirname(os.path.dirname(__file__))
csv_path = os.path.join(base_dir, "inputs", "Amazon_products.csv")
df = pd.read_csv(csv_path, encoding='ISO-8859-1')

print("✅ Dataset chargé avec succès")
print("📊 Aperçu des types de données :\n", df.dtypes)
print("🔍 Valeurs manquantes :\n", df.isna().sum())

# Standardisation des noms de colonnes
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# === 2. Suppression des doublons exacts ===
df = df.drop_duplicates()
print(f"🧹 Dataset après suppression des doublons exacts : {df.shape[0]} lignes")

# === 3. Prioriser les meilleurs doublons par product_id ===
# Conversions nécessaires
df['rating'] = pd.to_numeric(df.get('rating'), errors='coerce')
df['reviews_count'] = pd.to_numeric(df.get('reviews_count'), errors='coerce')
df['final_price'] = pd.to_numeric(df.get('final_price'), errors='coerce')

# Tri
df = df.sort_values(by=[
    'asin', 
    'rating', 
    'reviews_count', 
    'final_price'
], ascending=[True, False, False, True])

# Suppression des doublons sur asin (conservation du meilleur)
df = df.drop_duplicates(subset='asin', keep='first')
print(f"✅ Après dédoublonnage par product_id : {df.shape[0]} lignes")

# === 4. Remplissage intelligent des valeurs manquantes ===
# Remplir brand avec seller_name si brand est manquant
df['brand'] = df['brand'].fillna(df['seller_name'])

# Catégories manquantes = "unknown"
df['categories'] = df['categories'].fillna("unknown")

# === 5. Normalisation des textes ===
def clean_text(text):
    if pd.isna(text): return ""
    text = text.lower().strip()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Retire caractères spéciaux
    return text

df['title'] = df['title'].astype(str).apply(clean_text)
df['brand'] = df['brand'].astype(str).apply(clean_text)


# === 6. Validation finale ===
# assert df['product_id'].is_unique
assert df['asin'].is_unique, "❌ Des doublons subsistent dans product_id"
# Supprimer les lignes où asin, title ou final_price est manquant
df = df.dropna(subset=['asin', 'title', 'final_price'])

# Vérification finale
assert df['asin'].is_unique, "❌ Des doublons subsistent dans asin"


# === 7. Export final en .parquet ===
output_path = os.path.join(base_dir, "outputs", "products_cleaned_no_groupby.parquet")
df.to_parquet(output_path, index=False)
print(f"📦 Données nettoyées enregistrées dans : {output_path}")
