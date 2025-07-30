import pandas as pd

df = pd.read_csv(
    r'C:\Users\Redwan\OneDrive\Documents\Projet python\bank.csv',
    encoding='ISO-8859-1',
    sep=';',                  # Séparateur point-virgule
    skipinitialspace=True,    # Ignore les espaces après le ;
    quotechar='"'             # Si certains champs sont entre guillemets
)

print(df.columns)
print(df.head())

df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce')
import sqlite3

conn = sqlite3.connect(r'C:\Users\Redwan\OneDrive\Documents\Projet python\finance.db') 
df.to_sql('transactions', conn, if_exists='replace', index=False)
conn.close()