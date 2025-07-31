import pandas as pd
import os

# --- 1. Chargement du fichier CSV ---
csv_path = "C:\\Users\\user\\Desktop\\Arkx_projects\\pandas_project\\inputs\\Amazon_products.csv"
df = pd.read_csv(csv_path, encoding='ISO-8859-1')

# --- 2. Standardisation des noms de colonnes ---
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# --- 3. Conversions nécessaires ---
df['final_price'] = pd.to_numeric(df.get('final_price'), errors='coerce')
df['rating'] = pd.to_numeric(df.get('rating'), errors='coerce')
df['department'] = df.get('department', '').astype(str).str.lower()
df['categories'] = df.get('categories', '').astype(str).str.lower()

# --- 4. Nettoyage des données ---
df = df.dropna(subset=["title", "final_price"])

# Créer le dossier output si nécessaire
os.makedirs("outputs", exist_ok=True)

# =============================
# 🔍 PARTIE 1 : Top 10 'Electronics'
# =============================
electronics_df = df[
    df['department'].str.contains("electronics", case=False, na=False) &
    (df['final_price'] >= 100) & (df['final_price'] <= 800)
]
electronics_df = electronics_df[['title', 'final_price', 'max_quantity_available', 'department']]
top10_electronics = electronics_df.sort_values(by='final_price', ascending=False).head(10)

print("\n📦 Top 10 produits 'Electronics' entre 100$ et 800$ :")
if not top10_electronics.empty:
    print(top10_electronics)
else:
    print("❌ Aucun produit 'Electronics' trouvé dans la plage de prix spécifiée.")

top10_electronics.to_csv("outputs/top10_electronics.csv", index=False)


