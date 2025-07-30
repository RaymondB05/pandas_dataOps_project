import pandas as pd

# Définition d'un dictionnaire contenant les informations des utilisateurs
users = {
    'id': [1, 2, 3],
    'age': [25, 30, 35],
    'country': ['France', 'États-Unis', 'Canada']
}

# Création d'un DataFrame à partir du dictionnaire
df_users = pd.DataFrame(users)

# Affichage du DataFrame
print(df_users)