import os
from dotenv import load_dotenv
from deltalake import DeltaTable, write_deltalake

load_dotenv()

# este script migra los datos ya existentes desde la estructura vieja
# (sin distinguir fuente de datos) hacia la estructura nueva, que agrega
# un nivel de carpeta con el nombre de la fuente (nasa_neows). esto sigue
# el feedback de que un lakehouse aloja datos de varios sistemas, por lo
# que cada fuente debe estar identificada dentro de bronze/silver/gold.
# se corre una sola vez, antes de actualizar storage.py para que apunte
# de forma permanente a las rutas nuevas

storage_options = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "secret_access_key": os.getenv("MINIO_SECRET_KEY"),
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}
bucket = os.getenv("MINIO_BUCKET")
fuente = "nasa_neows"

tablas_a_migrar = [
    {
        "nombre": "bronze/aproximaciones",
        "path_viejo": f"s3://{bucket}/bronze/aproximaciones",
        "path_nuevo": f"s3://{bucket}/bronze/{fuente}/aproximaciones",
        "partition_cols": ["fecha"],
    },
    {
        "nombre": "bronze/metadatos",
        "path_viejo": f"s3://{bucket}/bronze/metadatos",
        "path_nuevo": f"s3://{bucket}/bronze/{fuente}/metadatos",
        "partition_cols": None,
    },
    {
        "nombre": "silver/aproximaciones",
        "path_viejo": f"s3://{bucket}/silver/aproximaciones",
        "path_nuevo": f"s3://{bucket}/silver/{fuente}/aproximaciones",
        "partition_cols": ["fecha"],
    },
    {
        "nombre": "silver/metadatos",
        "path_viejo": f"s3://{bucket}/silver/metadatos",
        "path_nuevo": f"s3://{bucket}/silver/{fuente}/metadatos",
        "partition_cols": None,
    },
    {
        "nombre": "gold/resumen_diario",
        "path_viejo": f"s3://{bucket}/gold/resumen_diario",
        "path_nuevo": f"s3://{bucket}/gold/{fuente}/resumen_diario",
        "partition_cols": None,
    },
]

for tabla in tablas_a_migrar:
    print(f"migrando {tabla['nombre']}...")
    df = DeltaTable(tabla["path_viejo"], storage_options=storage_options).to_pandas()
    print(f"  {len(df)} filas leidas desde {tabla['path_viejo']}")

    write_deltalake(
        tabla["path_nuevo"],
        df,
        mode="overwrite",
        partition_by=tabla["partition_cols"],
        storage_options=storage_options,
    )
    print(f"  guardado en {tabla['path_nuevo']}")

print("\nmigracion completada.")
