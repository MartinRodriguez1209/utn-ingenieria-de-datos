# %%
from dotenv import load_dotenv
import os
import pandas as pd
from deltalake import DeltaTable
from silver import transformar_aproximaciones, transformar_metadatos, validar_integridad

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

# %%
# leemos bronze tal cual esta guardado
df_aproximaciones_bronze = DeltaTable(
    f"{bronze_dir}/aproximaciones", storage_options=storage_options
).to_pandas()
df_metadatos_bronze = DeltaTable(
    f"{bronze_dir}/metadatos", storage_options=storage_options
).to_pandas()

df_aproximaciones_bronze.dtypes

# %%
df_metadatos_bronze.dtypes

# %%
# aplicamos las transformaciones de silver
df_aproximaciones_silver = transformar_aproximaciones(df_aproximaciones_bronze)
df_aproximaciones_silver

# %%
df_aproximaciones_silver.dtypes

# %%
df_metadatos_silver = transformar_metadatos(df_metadatos_bronze)
df_metadatos_silver

# %%
df_metadatos_silver.dtypes

# %%
# chequeo de integridad entre ambas tablas ya transformadas
validar_integridad(df_aproximaciones_silver, df_metadatos_silver)

# %%
# %%
print("aproximaciones:", len(df_aproximaciones_bronze))
print("metadatos:", len(df_metadatos_bronze))

# %%

# %%
df_aproximaciones_bronze = DeltaTable(
    f"{bronze_dir}/aproximaciones", storage_options=storage_options
).to_pandas()
df_metadatos_bronze = DeltaTable(
    f"{bronze_dir}/metadatos", storage_options=storage_options
).to_pandas()

print("aproximaciones:", len(df_aproximaciones_bronze))
print("metadatos:", len(df_metadatos_bronze))

# %%
# %%
df_aproximaciones_silver = transformar_aproximaciones(df_aproximaciones_bronze)
df_metadatos_silver = transformar_metadatos(df_metadatos_bronze)

validar_integridad(df_aproximaciones_silver, df_metadatos_silver)
# %%
# %%
ids_aproximaciones = set(df_aproximaciones_silver["id"])
ids_metadatos = set(df_metadatos_silver["id"])
huerfanos = ids_aproximaciones - ids_metadatos
print(len(huerfanos), huerfanos)
# %%
