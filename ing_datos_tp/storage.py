import os
import pyarrow as pa
from dotenv import load_dotenv
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

load_dotenv()

# Credenciales de MinIO
storage_options = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "secret_access_key": os.getenv("MINIO_SECRET_KEY"),
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

# Directorio base bronze
BUCKET = os.getenv("MINIO_BUCKET")
bronze_dir = f"s3://{BUCKET}/bronze"


def save_data_as_delta(
    df, path, storage_options, mode="overwrite", partition_cols=None
):
    """
    Guarda un DataFrame en formato Delta Lake en la ruta especificada.
    Soporta particionado por una o varias columnas.
    Por defecto el modo es overwrite.
    """
    write_deltalake(
        path,
        df,
        mode=mode,
        storage_options=storage_options,
        partition_by=partition_cols,
    )


def save_new_data_as_delta(
    new_data, data_path, predicate, storage_options, partition_cols=None
):
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    asegurando que no se guarden registros duplicados.
    Si la tabla no existe, la crea como nueva.
    """
    try:
        dt = DeltaTable(data_path, storage_options=storage_options)
        new_data_pa = pa.Table.from_pandas(new_data)
        dt.merge(
            source=new_data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate,
        ).when_not_matched_insert_all().execute()

    except TableNotFoundError:
        save_data_as_delta(
            new_data,
            data_path,
            storage_options=storage_options,
            partition_cols=partition_cols,
        )


def guardar_aproximaciones(df):
    """
    Guarda el DataFrame de aproximaciones en la capa bronze del data lake.
    Extracción incremental particionada por fecha.
    Usa MERGE para evitar duplicados.
    """
    # Agregar columna de fecha para particionado
    df["fecha"] = df["close_approach_date"].dt.date

    path = f"{bronze_dir}/aproximaciones"

    print("Guardando aproximaciones en bronze...")
    save_new_data_as_delta(
        df,
        path,
        predicate="target.id = source.id AND target.close_approach_date = source.close_approach_date",
        storage_options=storage_options,
        partition_cols=["fecha"],
    )
    print(f"Aproximaciones guardadas en: {path}")


def guardar_metadatos(df):
    """
    Guarda el DataFrame de metadatos en la capa bronze del data lake.
    Extracción full, sobreescribe en cada ejecución.
    """
    path = f"{bronze_dir}/metadatos"

    print("Guardando metadatos en bronze...")
    save_data_as_delta(df, path, storage_options=storage_options, mode="overwrite")
    print(f"Metadatos guardados en: {path}")
