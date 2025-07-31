import pandas as pd

# --- Chemins absolus ---
input_csv = "C:\\Users\\user\\Desktop\\Arkx_projects\\pandas_project\\inputs\\Orders.csv"
output_csv = "C:\\Users\\user\\Desktop\\Arkx_projects\\pandas_project\\outputs\\central_sales.csv"  # Aussi modifié le nom du fichier de sortie

# --- Paramètres ---
chunksize = 500_000
total_revenue = 0
header_written = False

for chunk in pd.read_csv(input_csv, chunksize=chunksize, encoding='ISO-8859-1'):
    # Nettoyage des noms de colonnes
    chunk.columns = [col.strip().lower().replace(" ", "_") for col in chunk.columns]

    # Calcul du total
    chunk['total'] = chunk['quantity_ordered_new'] * chunk['unit_price']

    # Filtrer uniquement les lignes où region == 'Central'
    central_chunk = chunk[chunk['region'] == 'Central']

    # Ajouter au total
    total_revenue += central_chunk['total'].sum()

    # Écriture incrémentale
    central_chunk.to_csv(output_csv, mode='a', header=not header_written, index=False)
    header_written = True

# Affichage final
print(f"\n✅ Total revenue from Central region: €{total_revenue:,.2f}")
