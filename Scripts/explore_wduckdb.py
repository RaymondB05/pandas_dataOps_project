import duckdb

# Lire le fichier Parquet
df = duckdb.query("SELECT title, final_price, rating FROM 'cleaned_shopee_products.parquet' LIMIT 10").to_df()

print("\n🔍 Top 10 produits :")
print(df)
