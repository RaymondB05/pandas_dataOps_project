import pandas as pd
import numpy as np

# Générer un DataFrame avec 1 million de lignes
df = pd.DataFrame({
    'customer_id': np.random.randint(1, 1_000_000, 1_000_000),
    'age': np.random.randint(18, 90, 1_000_000),
    'price': np.random.uniform(1, 500, 1_000_000),
    'city': np.random.choice(['Paris', 'London', 'Berlin', 'Madrid'], 1_000_000),
    'is_active': np.random.choice([True, False], 1_000_000)
})

# Rendre la colonne 'is_active' inefficace en mémoire
df['is_active'] = df['is_active'].astype(object)

# --- Affichage ---
print("Aperçu du DataFrame :")
print(df.head())

print("\nInfo mémoire :")
df.info(memory_usage='deep')

print("\nTop 3 des colonnes les plus gourmandes en mémoire :")
print(df.memory_usage(deep=True).sort_values(ascending=False).head(3))


