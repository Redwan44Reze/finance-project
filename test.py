import requests
import sqlite3

URL = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/records?where=dep_name%20%3D%20'Loire-Atlantique'"

def fetch_data():
    response = requests.get(URL + "&limit=100&offset=0")
    response.raise_for_status()
    data1 = response.json().get("results", [])
    response = requests.get(URL + "&limit=100&offset=100")
    response.raise_for_status()
    data2 = response.json().get("results", [])
    response = requests.get(URL + "&limit=100&offset=200")
    response.raise_for_status()
    data3 = response.json().get("results", [])
    return data1 + data2 + data3

def create_table(conn, sample_record):
    columns = []
    for key, value in sample_record.items():
        if isinstance(value, int):
            col_type = "INTEGER"
        elif isinstance(value, float):
            col_type = "REAL"
        else:
            col_type = "TEXT"
        columns.append(f"'{key}' {col_type}")

    columns_sql = ", ".join(columns)
    sql = f"CREATE TABLE IF NOT EXISTS donnees_ofgl ({columns_sql});"
    conn.execute(sql)
    conn.commit()

def insert_records(conn, records):
    if not records:
        print("Aucun enregistrement à insérer.")
        return

    keys = records[0].keys()
    columns = ", ".join(f"'{k}'" for k in keys)
    placeholders = ", ".join("?" for _ in keys)
    sql = f"INSERT INTO donnees_ofgl ({columns}) VALUES ({placeholders})"

    values_list = [tuple(record.get(k) for k in keys) for record in records]
    conn.executemany(sql, values_list)
    conn.commit()
    print(f"{len(records)} enregistrements insérés.")

def main():
    print("Récupération des données depuis l'API...")
    records = fetch_data()
    print(len(records))
    if not records:
        print("Aucune donnée trouvée.")
        return

    conn = sqlite3.connect("ofgl_communes.db")
    print(records[0])
    create_table(conn, records[0])
    insert_records(conn, records)
    conn.close()
    print("Terminé.")

if __name__ == "__main__":
    main()
