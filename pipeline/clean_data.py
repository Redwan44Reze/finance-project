import pandas as pd

COLUMNS_TO_RENAME = {
    "exer" : "annee_exercice",
    "outre_mer" : "zone_outre_mer",
    "reg_name" : "nom_region",       
    "dep_code" : "code_departement", 
    "dep_name" : "nom_departement",  
    "com_code" : "code_commune",     
    "com_name" : "nom_commune",
    "tranche_population" : "tranche_population_insee",
    "rural" : "commune_rurale",
    "montagne" : "zone_montagne",
    "touristique" : "commune_touristique",
    "tranche_revenu_imposable_par_habitant" : "tranche_revenu_par_habitant",
    "qpv" : "presence_qpv",
    "lbudg" : "libelle_budget",
    "agregat" : "poste_agrege",
    "montant" : "montant_euros",
    "montant_en_millions" : "montant_millions",
    "ptot" : "population_reference",
    "euros_par_habitant" : "euros_par_habitant"
}


def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.rename(columns=COLUMNS_TO_RENAME)

    numeric_cols = ["annee_exercice", 
                    "tranche_population_insee", 
                    "tranche_revenu_par_habitant", 
                    "montant_euros", 
                    "montant_millions", 
                    "population_reference",
                    "euros_par_habitant"]
    
    boolean_cols = ["zone_outre_mer", 
                    "commune_rurale", 
                    "zone_montagne", 
                    "commune_touristique", 
                    "presence_qpv"]
    
    string_cols = ["nom_region",       
                   "code_departement", 
                   "nom_departement",  
                   "code_commune",     
                   "nom_commune",
                   "libelle_budget",
                   "poste_agrege"]
    
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in boolean_cols:
        df[col] = df[col].map({"Oui": True, "Non": False})
    
    df[boolean_cols] = df[boolean_cols].astype(bool)
    df[string_cols] = df[string_cols].astype(str)

    df = df[numeric_cols + boolean_cols + string_cols]

    return df

if __name__ == "__main__":
    from load_data import load_data

    df = load_data()
    df_clean = clean_data(df)
    print(df_clean.head())
    print(df_clean.dtypes)
