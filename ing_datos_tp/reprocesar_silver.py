import os
from dotenv import load_dotenv
from deltalake import DeltaTable

from silver import transformar_aproximaciones, transformar_metadatos, validar_integridad
from storage import save_data_as_delta, storage_options, bronze_dir, silver_dir

load_dotenv()

# este script reprocesa TODO el historial de bronze hacia silver, sin pasar
# por el filtro de "solo lo pendiente" ni por el merge solo-insert que usa
# main.py. se usa cuando cambia la logica de transformacion (por ejemplo,
# una columna nueva) y hace falta que esa logica se aplique tambien a los
# datos que ya estaban en silver, no solo a los datos nuevos de la proxima
# corrida normal. por eso se usa overwrite: el objetivo es reemplazar todo
# silver con la version recalculada, no agregar lo que falte


def reprocesar_silver_completo():
    print("leyendo bronze completo...")
    df_aproximaciones_bronze = DeltaTable(
        f"{bronze_dir}/aproximaciones", storage_options=storage_options
    ).to_pandas()
    df_metadatos_bronze = DeltaTable(
        f"{bronze_dir}/metadatos", storage_options=storage_options
    ).to_pandas()

    print(f"aproximaciones a reprocesar: {len(df_aproximaciones_bronze)}")
    print(f"metadatos a reprocesar: {len(df_metadatos_bronze)}")

    df_aproximaciones_silver = transformar_aproximaciones(df_aproximaciones_bronze)
    df_metadatos_silver = transformar_metadatos(df_metadatos_bronze)

    validar_integridad(df_aproximaciones_silver, df_metadatos_silver)

    print("\nguardando silver reprocesado (overwrite completo)...")
    save_data_as_delta(
        df_aproximaciones_silver,
        f"{silver_dir}/aproximaciones",
        storage_options=storage_options,
        mode="overwrite",
        schema_mode="overwrite",
        partition_cols=["fecha"],
    )
    save_data_as_delta(
        df_metadatos_silver,
        f"{silver_dir}/metadatos",
        storage_options=storage_options,
        mode="overwrite",
        schema_mode="overwrite",
    )
    print("reprocesamiento completado.")


if __name__ == "__main__":
    reprocesar_silver_completo()
