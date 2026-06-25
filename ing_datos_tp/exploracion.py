# script de inspeccion final. se corre con: python explorar.py
# (no requiere el panel interactivo de vscode, usa prints normales)
from dotenv import load_dotenv
import os
import pandas as pd
from deltalake import DeltaTable
from silver import transformar_aproximaciones, transformar_metadatos, validar_integridad
from gold import calcular_resumen_diario

load_dotenv()

storage_options = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "secret_access_key": os.getenv("MINIO_SECRET_KEY"),
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}
bucket = os.getenv("MINIO_BUCKET")
bronze_dir = f"s3://{bucket}/bronze"
silver_dir = f"s3://{bucket}/silver"
gold_dir = f"s3://{bucket}/gold"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


# ---------------------------------------------------------------
# capa bronze: datos crudos, tal cual llegan de la api
# ---------------------------------------------------------------
df_bronze_aproximaciones = DeltaTable(
    f"{bronze_dir}/aproximaciones", storage_options=storage_options
).to_pandas()
print("\n=== bronze/aproximaciones —", len(df_bronze_aproximaciones), "filas ===")
print(df_bronze_aproximaciones.head())
print("\ntipos de datos:")
print(df_bronze_aproximaciones.dtypes)

df_bronze_metadatos = DeltaTable(
    f"{bronze_dir}/metadatos", storage_options=storage_options
).to_pandas()
print("\n=== bronze/metadatos —", len(df_bronze_metadatos), "filas ===")
print(df_bronze_metadatos.head())
print("\ntipos de datos:")
print(df_bronze_metadatos.dtypes)


# ---------------------------------------------------------------
# capa silver: datos ya guardados, limpios y tipados
# ---------------------------------------------------------------
df_silver_aproximaciones = DeltaTable(
    f"{silver_dir}/aproximaciones", storage_options=storage_options
).to_pandas()
print("\n=== silver/aproximaciones —", len(df_silver_aproximaciones), "filas ===")
print(df_silver_aproximaciones.head())
print("\ntipos de datos:")
print(df_silver_aproximaciones.dtypes)

df_silver_metadatos = DeltaTable(
    f"{silver_dir}/metadatos", storage_options=storage_options
).to_pandas()
print("\n=== silver/metadatos —", len(df_silver_metadatos), "filas ===")
print(df_silver_metadatos.head())
print("\ntipos de datos:")
print(df_silver_metadatos.dtypes)


# ---------------------------------------------------------------
# chequeo de integridad: aproximaciones vs metadatos en silver
# ---------------------------------------------------------------
print("\n=== validacion de integridad ===")
validar_integridad(df_silver_aproximaciones, df_silver_metadatos)


# ---------------------------------------------------------------
# capa gold: agregacion final, resumen diario
# ---------------------------------------------------------------
df_gold_resumen = DeltaTable(
    f"{gold_dir}/resumen_diario", storage_options=storage_options
).to_pandas()
print("\n=== gold/resumen_diario —", len(df_gold_resumen), "filas ===")
print(df_gold_resumen)
print("\ntipos de datos:")
print(df_gold_resumen.dtypes)


# ---------------------------------------------------------------
# vista comparativa rapida: cuantas filas hay en cada capa
# ---------------------------------------------------------------
print("\n=== resumen de filas por capa ===")
print(
    "bronze  aproximaciones:",
    len(df_bronze_aproximaciones),
    " | metadatos:",
    len(df_bronze_metadatos),
)
print(
    "silver  aproximaciones:",
    len(df_silver_aproximaciones),
    " | metadatos:",
    len(df_silver_metadatos),
)
print("gold    resumen_diario:", len(df_gold_resumen))
