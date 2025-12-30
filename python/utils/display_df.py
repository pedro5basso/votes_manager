import pandas as pd
import os

def mostrar_parquet(ruta_parquet):
    """
    """
    try:
        df = pd.read_parquet(ruta_parquet)
        print(df)
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")
        return None


if __name__ == "__main__":
    PATH = r'D:\dev\UCM-BD_DE\votes_manager\datalake\bronze\votes'
    FILE = r'part-00000-3fb86a45-c17e-4bdd-afdb-7579ba73394a-c000.snappy.parquet'
    filepath = os.path.join(PATH, FILE)
    mostrar_parquet(filepath)