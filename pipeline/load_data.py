import requests
import pandas as pd



def load_data():
    data1 = []
    for i in range(22,25):
        URL = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes-consolidee/records?where=dep_name%20%3D%20%27Loire-Atlantique%27%20and%20exer%20%3D%20date%2720"+str(i)+"%27%20and%20ptot%20%3E%201000&limit=100"
        offset = 0
        response = requests.get(URL)
        response.raise_for_status()
        total_row = response.json().get("total_count")
        while(offset<total_row):
            response = requests.get(URL + "&offset=" + str(offset))
            response.raise_for_status()
            data1 += response.json().get("results", [])
            offset += 100

    df = pd.DataFrame(data1)
    return df 

if __name__ == "__main__":
    df = load_data()
    print(df.head())
    print(f"Nombre de lignes : {len(df)}")