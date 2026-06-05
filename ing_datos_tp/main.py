from dotenv import load_dotenv
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from incremental import extraer_aproximaciones
from full import extraer_metadatos
from storage import guardar_aproximaciones, guardar_metadatos

load_dotenv()

api_key = os.getenv("NASA_API_KEY")


def consultar_feed(api_key):
    """
    Consulta el endpoint de feed de la API de NASA NeoWs.
    Devuelve los datos crudos de asteroides para los últimos 7 días.
    Lanza una excepción si la consulta falla.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=6)

    print(
        f"Consultando feed del {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}..."
    )

    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "api_key": api_key,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Status code: {response.status_code}")
        print(f"Total asteroides encontrados: {data['element_count']}")
        return data
    except requests.exceptions.ConnectionError:
        raise SystemExit(
            "Error: no se pudo conectar a la API de NASA. Verificá tu conexión a internet."
        )
    except requests.exceptions.Timeout:
        raise SystemExit(
            "Error: la API de NASA tardó demasiado en responder. Intentá de nuevo más tarde."
        )
    except requests.exceptions.HTTPError as e:
        raise SystemExit(f"Error HTTP al consultar el feed: {e}")


# Ejecución principal
data = consultar_feed(api_key)

# Extracción incremental
df_aproximaciones = extraer_aproximaciones(data)

# Obtener IDs únicos para la extracción full
ids = df_aproximaciones["id"].tolist()

# Extracción full
df_metadatos = extraer_metadatos(ids, api_key)

# Prints finales
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

print("\n--- DataFrame Aproximaciones (incremental) ---")
print(df_aproximaciones)
print(f"\nFilas: {len(df_aproximaciones)} | Columnas: {len(df_aproximaciones.columns)}")
print(f"\nTipos de datos:")
print(df_aproximaciones.dtypes)

print("\n--- DataFrame Metadatos (full) ---")
print(df_metadatos)
print(f"\nFilas: {len(df_metadatos)} | Columnas: {len(df_metadatos.columns)}")
print(f"\nTipos de datos:")
print(df_metadatos.dtypes)

# Almacenamiento en Delta Lake
print("\nGuardando datos en MinIO...")
guardar_aproximaciones(df_aproximaciones)
guardar_metadatos(df_metadatos)
print("\nAlmacenamiento completado.")
