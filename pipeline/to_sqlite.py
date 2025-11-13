import sqlite3
from load_data import load_data
from clean_data import clean_data

def save_to_sqlite(df, db_path="data/processed/communes.db", table_name="comptes_communes"):
    
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    df = load_data()
    df_clean = clean_data(df)
    save_to_sqlite(df_clean)
    print("Données sauvegardées dans SQLite avec succès.")