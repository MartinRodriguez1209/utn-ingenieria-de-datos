import os
import pyarrow as pa
from dotenv import load_dotenv
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

load_dotenv()

# credenciales de MinIO
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
silver_dir = f"s3://{BUCKET}/silver"


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
    guarda el dataframe de metadatos en la capa bronze del data lake.
    los metadatos de un asteroide no cambian, pero distintas ejecuciones
    pueden traer asteroides distintos (segun que aparezca en el feed de esa
    semana). antes se usaba overwrite, lo cual borraba los metadatos de
    asteroides que no estaban en el feed mas reciente. se cambia a merge
    solo-insert para acumular metadatos sin perder los de corridas anteriores
    """
    path = f"{bronze_dir}/metadatos"
    print("guardando metadatos en bronze...")
    save_new_data_as_delta(
        df, path, predicate="target.id = source.id", storage_options=storage_options
    )
    print(f"metadatos guardados en: {path}")


def guardar_aproximaciones_silver(df):
    """
    guarda el dataframe de aproximaciones ya transformado en la capa silver.
    se particiona por fecha porque cada corrida trae una ventana de 7 dias
    que se solapa parcialmente con corridas anteriores, y particionar por
    fecha permite que el merge sea mas eficiente al no tener que escanear
    todo el historial. se usa merge solo-insert para evitar duplicar
    aproximaciones ya guardadas en corridas previas. se compara por id y
    el timestamp completo (no fecha+hora por separado) para no confundir
    dos aproximaciones distintas que caigan en la misma hora redondeada
    """
    path = f"{silver_dir}/aproximaciones"

    print("guardando aproximaciones en silver...")
    save_new_data_as_delta(
        df,
        path,
        predicate="target.id = source.id AND target.close_approach_date_full = source.close_approach_date_full",
        storage_options=storage_options,
        partition_cols=["fecha"],
    )
    print(f"aproximaciones guardadas en: {path}")


def guardar_metadatos_silver(df):
    """
    guarda el dataframe de metadatos ya transformado en la capa silver.
    no se particiona: no hay una columna de fecha natural en metadatos
    (son atributos fijos del asteroide, no eventos), y el volumen de datos
    no justifica particionar por ninguna otra columna. se usa merge
    solo-insert por el mismo motivo que en bronze: evitar perder metadatos
    de asteroides que no aparezcan en el feed de una corrida futura
    """
    path = f"{silver_dir}/metadatos"

    print("guardando metadatos en silver...")
    save_new_data_as_delta(
        df,
        path,
        predicate="target.id = source.id",
        storage_options=storage_options,
    )
    print(f"metadatos guardados en: {path}")
