from dotenv import load_dotenv
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from deltalake import DeltaTable

from incremental import extraer_aproximaciones
from full import extraer_metadatos
from silver import transformar_aproximaciones, transformar_metadatos, validar_integridad
from storage import (
    guardar_aproximaciones,
    guardar_metadatos,
    guardar_aproximaciones_silver,
    guardar_metadatos_silver,
    storage_options,
    bronze_dir,
)

load_dotenv()

api_key = os.getenv("NASA_API_KEY")


def consultar_feed(api_key):
    """
    consulta el endpoint de feed de la api de nasa neows.
    devuelve los datos crudos de asteroides para los ultimos 7 dias.
    lanza una excepcion si la consulta falla.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=6)

    print(
        f"consultando feed del {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}..."
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
        print(f"status code: {response.status_code}")
        print(f"total asteroides encontrados: {data['element_count']}")
        return data
    except requests.exceptions.ConnectionError:
        raise SystemExit(
            "error: no se pudo conectar a la api de nasa. verifica tu conexion a internet."
        )
    except requests.exceptions.Timeout:
        raise SystemExit(
            "error: la api de nasa tardo demasiado en responder. intenta de nuevo mas tarde."
        )
    except requests.exceptions.HTTPError as e:
        raise SystemExit(f"error http al consultar el feed: {e}")


def ejecutar_extraccion_bronze():
    """
    ejecuta la extraccion completa: consulta el feed, extrae aproximaciones
    y metadatos, y los guarda en bronze.
    """
    data = consultar_feed(api_key)

    df_aproximaciones = extraer_aproximaciones(data)
    ids = df_aproximaciones["id"].tolist()
    df_metadatos = extraer_metadatos(ids, api_key)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)

    print("\n--- dataframe aproximaciones (incremental) ---")
    print(df_aproximaciones)

    print("\n--- dataframe metadatos (full) ---")
    print(df_metadatos)

    print("\nguardando datos en minio (bronze)...")
    guardar_aproximaciones(df_aproximaciones)
    guardar_metadatos(df_metadatos)
    print("almacenamiento en bronze completado.")


def ejecutar_transformacion_silver():
    """
    lee los datos ya guardados en bronze, aplica las transformaciones de
    silver, valida la integridad entre ambas tablas, y guarda el resultado
    en la capa silver. se lee desde bronze en lugar de reusar los dataframes
    en memoria de la extraccion, para que silver siempre parta del dato
    persistido (no de una variable de un paso anterior que podria no
    reflejar lo que realmente quedo guardado)
    """
    df_aproximaciones_bronze = DeltaTable(
        f"{bronze_dir}/aproximaciones", storage_options=storage_options
    ).to_pandas()
    df_metadatos_bronze = DeltaTable(
        f"{bronze_dir}/metadatos", storage_options=storage_options
    ).to_pandas()

    df_aproximaciones_silver = transformar_aproximaciones(df_aproximaciones_bronze)
    df_metadatos_silver = transformar_metadatos(df_metadatos_bronze)

    validar_integridad(df_aproximaciones_silver, df_metadatos_silver)

    print("\nguardando datos en minio (silver)...")
    guardar_aproximaciones_silver(df_aproximaciones_silver)
    guardar_metadatos_silver(df_metadatos_silver)
    print("almacenamiento en silver completado.")


if __name__ == "__main__":
    ejecutar_extraccion_bronze()
    ejecutar_transformacion_silver()
