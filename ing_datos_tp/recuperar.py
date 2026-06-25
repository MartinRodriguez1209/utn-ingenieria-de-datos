import os
from dotenv import load_dotenv

from full import extraer_metadatos
from storage import guardar_metadatos

load_dotenv()

api_key = os.getenv("NASA_API_KEY")

# ids de aproximaciones que quedaron sin metadato asociado por el bug de
# overwrite en guardar_metadatos (ya corregido). se recuperan puntualmente
# antes de que la correccion del pipeline normal pueda alcanzarlos por si
# solos (esos asteroides probablemente no vuelvan a aparecer en el feed)
ids_huerfanos = [
    2001943,
    3720918,
    3744562,
    2478784,
    3304382,
    3644087,
    3892162,
    2662203,
    3639466,
    3639550,
    3719242,
    3752723,
    3799299,
    3774363,
    3795028,
    3836092,
    3781045,
    3838892,
    2141531,
    3410171,
    3428531,
    3553284,
    3579625,
    3678630,
    3884238,
    2437844,
    3698472,
    3836085,
    3843765,
    3426410,
    3672459,
    3673916,
    3837643,
    3843278,
    3511111,
    3555769,
    3626611,
    3673913,
    3824998,
]

print(f"recuperando metadatos de {len(ids_huerfanos)} asteroides huerfanos...")
df_metadatos_recuperados = extraer_metadatos(ids_huerfanos, api_key)

print("\nguardando metadatos recuperados en bronze...")
guardar_metadatos(df_metadatos_recuperados)
print("recuperacion completada.")
