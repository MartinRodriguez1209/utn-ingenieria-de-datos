import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

def transformar_aproximaciones(df):
    """
    aplica las transformaciones de la capa silver al dataframe de aproximaciones.
    recibe el dataframe crudo leído desde bronze y devuelve una versión limpia,
    tipada y enriquecida con columnas derivadas.
    """
    df = df.copy()

    # se vuelve a castear aunque ya se hizo en bronze: la escritura/lectura en
    # parquet no garantiza conservar exactamente los mismos tipos con los que
    # se escribió originalmente, asi que silver no deberia confiar en bronze
    df["close_approach_date_full"] = pd.to_datetime(df["close_approach_date_full"])
    df["relative_velocity_km_per_second"] = df[
        "relative_velocity_km_per_second"
    ].astype(float)
    df["miss_distance_km"] = df["miss_distance_km"].astype(float)

    # id es un identificador, no una cantidad para hacer matematica, pero se
    # unifica a int64 en ambos dataframes para que el join por id funcione bien
    df["id"] = df["id"].astype("int64")

    # orbiting_body siempre es "earth" en este endpoint, es una categoria fija
    # y no texto libre, así que category es el tipo correcto
    df["orbiting_body"] = df["orbiting_body"].astype("category")

    # close_approach_date es redundante: tiene el mismo dia que
    # close_approach_date_full pero siempre con hora 00:00:00. se descompone
    # close_approach_date_full en fecha y hora, y se elimina la columna vieja
    df["fecha"] = df["close_approach_date_full"].dt.date
    df["hora"] = df["close_approach_date_full"].dt.hour
    df = df.drop(columns=["close_approach_date"])

    # name e is_potentially_hazardous son atributos del asteroide en si, no
    # del evento de aproximacion puntual. la api los devuelve duplicados en
    # ambos endpoints, pero conceptualmente pertenecen a metadatos. se
    # eliminan aca para evitar redundancia entre tablas
    df = df.drop(columns=["name", "is_potentially_hazardous"])

    return df


def transformar_metadatos(df):
    """
    aplica las transformaciones de la capa silver al dataframe de metadatos.
    recibe el dataframe crudo leído desde bronze y devuelve una versión limpia,
    tipada y enriquecida con columnas derivadas.
    """
    df = df.copy()

    # mismo motivo que en aproximaciones: se vuelve a castear por las dudas,
    # no se confia en que bronze haya conservado los tipos correctos
    df["absolute_magnitude_h"] = df["absolute_magnitude_h"].astype(float)
    df["diameter_min_km"] = df["diameter_min_km"].astype(float)
    df["diameter_max_km"] = df["diameter_max_km"].astype(float)
    df["is_potentially_hazardous"] = df["is_potentially_hazardous"].astype(bool)
    df["is_sentry_object"] = df["is_sentry_object"].astype(bool)

    # se unifica a int64 para que el join con aproximaciones funcione bien
    df["id"] = df["id"].astype("int64")

    # nasa_jpl_url es 100% derivable a partir de id (siempre el mismo patron:
    # base fija + id, ya verificado contra los datos reales). se elimina para
    # evitar redundancia, se puede reconstruir cuando haga falta con:
    # "https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=" + str(id)
    df = df.drop(columns=["nasa_jpl_url"])

    # el nombre del asteroide mezcla dos formatos: con designacion numerica
    # oficial ("470310 (2007 lb15)") y sin ella ("(2015 ng13)"). el numero,
    # cuando existe, indica que el asteroide ya tiene una orbita confirmada
    # y catalogada oficialmente. se extrae a su propia columna y se deja
    # nulo cuando no aplica (caso real de nulo, no forzado)
    df["numero_oficial"] = df["name"].str.extract(r"^(\d+)\s")

    # columna booleana derivada de la misma logica, util para filtrar sin
    # tener que repetir el parseo del string cada vez
    df["tiene_designacion_oficial"] = df["name"].str.match(r"^\d+\s")

    # cuantifica la incertidumbre en la estimacion del diametro: la nasa no
    # mide el tamaño exacto, solo da un rango. se usa el delta absoluto en
    # km porque mantiene la misma unidad que el resto de las columnas de
    # tamaño y no se distorsiona cuando diameter_min_km es muy chico
    df["incertidumbre_diametro_km"] = df["diameter_max_km"] - df["diameter_min_km"]

    # is_potentially_hazardous (tamaño/orbita) e is_sentry_object (monitoreo
    # activo de probabilidad de impacto) son criterios distintos y
    # complementarios de riesgo. se combinan con or para identificar
    # asteroides bajo algun tipo de seguimiento, sea cual sea el motivo
    df["bajo_seguimiento_riesgo"] = (
        df["is_potentially_hazardous"] | df["is_sentry_object"]
    )

    return df


def validar_integridad(df_aproximaciones, df_metadatos):
    """
    cruza aproximaciones y metadatos por id para verificar que no haya
    aproximaciones sin metadato asociado. no devuelve nada, es solo un
    chequeo de calidad de datos antes de persistir en silver.
    """
    df_completo = pd.merge(df_aproximaciones, df_metadatos, on="id", how="left")

    # si el merge no encontro metadato para alguna aproximacion, las columnas
    # de metadatos van a quedar nulas para esa fila
    ids_sin_metadato = df_completo[df_completo["diameter_min_km"].isna()]["id"]

    if len(ids_sin_metadato) > 0:
        print(
            f"advertencia: {len(ids_sin_metadato)} aproximaciones sin metadato asociado: {ids_sin_metadato.tolist()}"
        )
    else:
        print(
            "integridad ok: todas las aproximaciones tienen su metadato correspondiente"
        )

def filtrar_metadatos_pendientes(df_bronze, silver_path, storage_options):
    """
    compara los ids que ya existen en silver contra los que hay en bronze,
    y devuelve solo las filas que todavia no se transformaron. esto evita
    reprocesar todo el historial acumulado de bronze en cada corrida, sin
    asumir que el script se ejecuta semana a semana sin saltos
    """
    try:
        df_silver = DeltaTable(silver_path, storage_options=storage_options).to_pandas()
        ids_ya_procesados = set(df_silver["id"])
    except TableNotFoundError:
        ids_ya_procesados = set()

    return df_bronze[~df_bronze["id"].isin(ids_ya_procesados)].copy()


def filtrar_aproximaciones_pendientes(df_bronze, silver_path, storage_options):
    """
    mismo concepto, pero la clave de comparacion es id + close_approach_date_full,
    ya que un mismo asteroide puede tener varias aproximaciones (filas) distintas
    """
    import pandas as pd

    df_bronze = df_bronze.copy()
    df_bronze["close_approach_date_full"] = pd.to_datetime(df_bronze["close_approach_date_full"])

    try:
        df_silver = DeltaTable(silver_path, storage_options=storage_options).to_pandas()
        claves_procesadas = set(
            zip(df_silver["id"], pd.to_datetime(df_silver["close_approach_date_full"]))
        )
    except TableNotFoundError:
        claves_procesadas = set()

    claves_bronze = list(zip(df_bronze["id"], df_bronze["close_approach_date_full"]))
    mascara_pendiente = [clave not in claves_procesadas for clave in claves_bronze]

    return df_bronze[mascara_pendiente]