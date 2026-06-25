from dotenv import load_dotenv
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from deltalake import DeltaTable

from incremental import extraer_aproximaciones
from full import extraer_metadatos
from silver import (
    transformar_aproximaciones,
    transformar_metadatos,
    validar_integridad,
    filtrar_aproximaciones_pendientes,
    filtrar_metadatos_pendientes,
)
from gold import calcular_resumen_diario
from storage import (
    guardar_aproximaciones,
    guardar_metadatos,
    guardar_aproximaciones_silver,
    guardar_metadatos_silver,
    storage_options,
    bronze_dir,
    silver_dir,
    guardar_resumen_diario,
    gold_dir,
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
    lee de bronze solo los registros que todavia no fueron procesados en
    silver, aplica las transformaciones, los guarda, y recien al final
    valida la integridad contra el estado completo y actualizado de silver
    """
    df_aproximaciones_bronze = DeltaTable(
        f"{bronze_dir}/aproximaciones", storage_options=storage_options
    ).to_pandas()
    df_metadatos_bronze = DeltaTable(
        f"{bronze_dir}/metadatos", storage_options=storage_options
    ).to_pandas()

    silver_aproximaciones_path = f"{silver_dir}/aproximaciones"
    silver_metadatos_path = f"{silver_dir}/metadatos"

    # filtramos solo lo que todavia no esta en silver, para no reprocesar
    # todo el historial acumulado en cada corrida
    df_aproximaciones_pendientes = filtrar_aproximaciones_pendientes(
        df_aproximaciones_bronze, silver_aproximaciones_path, storage_options
    )
    df_metadatos_pendientes = filtrar_metadatos_pendientes(
        df_metadatos_bronze, silver_metadatos_path, storage_options
    )

    print(f"aproximaciones pendientes de procesar: {len(df_aproximaciones_pendientes)}")
    print(f"metadatos pendientes de procesar: {len(df_metadatos_pendientes)}")

    # transformamos y guardamos solo lo nuevo
    if not df_aproximaciones_pendientes.empty:
        df_aproximaciones_silver = transformar_aproximaciones(
            df_aproximaciones_pendientes
        )
        guardar_aproximaciones_silver(df_aproximaciones_silver)

    if not df_metadatos_pendientes.empty:
        df_metadatos_silver = transformar_metadatos(df_metadatos_pendientes)
        guardar_metadatos_silver(df_metadatos_silver)

    if df_aproximaciones_pendientes.empty and df_metadatos_pendientes.empty:
        print("no hay datos nuevos para procesar en silver.")

    # la validacion de integridad se hace al final, leyendo silver completo
    # ya actualizado (no solo lo nuevo de esta corrida), para que sea un
    # chequeo real del estado completo del dataset
    print("\nvalidando integridad de silver...")
    df_aproximaciones_silver_completo = DeltaTable(
        silver_aproximaciones_path, storage_options=storage_options
    ).to_pandas()
    df_metadatos_silver_completo = DeltaTable(
        silver_metadatos_path, storage_options=storage_options
    ).to_pandas()

    validar_integridad(df_aproximaciones_silver_completo, df_metadatos_silver_completo)

    print("almacenamiento en silver completado.")


def ejecutar_agregacion_gold():
    """
    lee silver completo (ya actualizado), calcula el resumen diario y lo
    guarda en gold. se relee silver en vez de reusar variables en memoria,
    por el mismo motivo que silver relee bronze: garantizar que se parte
    de lo que realmente quedo persistido
    """
    df_aproximaciones_silver = DeltaTable(
        f"{silver_dir}/aproximaciones", storage_options=storage_options
    ).to_pandas()
    df_metadatos_silver = DeltaTable(
        f"{silver_dir}/metadatos", storage_options=storage_options
    ).to_pandas()

    df_resumen_diario = calcular_resumen_diario(
        df_aproximaciones_silver, df_metadatos_silver
    )

    print("\nguardando datos en minio (gold)...")
    guardar_resumen_diario(df_resumen_diario)
    print("almacenamiento en gold completado.")


if __name__ == "__main__":
    ejecutar_extraccion_bronze()
    ejecutar_transformacion_silver()
    ejecutar_agregacion_gold()
