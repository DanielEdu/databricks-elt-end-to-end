from pyspark.sql import DataFrame
from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, struct, md5


def set_storage_account_key(account: str, key: str):
    """Set storage account key into spark config for abfss access."""
    if not key:
        raise ValueError("Storage account key is empty. Use secrets or set spark.conf before running.")
    spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
    print(f"Configured storage key for account: {account}")


def read_parquet_landing(container: str, account: str, path: str) -> DataFrame:
    """Read parquet files from landing container path (folder)."""
    uri = f"abfss://{container}@{account}.dfs.core.windows.net/{path}"
    print(f"Reading parquet from: {uri}")
    return spark.read.parquet(uri)


def read_csv_ref(container: str, account: str, filename: str, sep: str=';') -> DataFrame:
    """Read reference csv from listas-referencia container. Default sep=';'."""
    uri = f"abfss://{container}@{account}.dfs.core.windows.net/{filename}"
    print(f"Reading CSV ref from: {uri} (sep='{sep}')")
    return (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .option("sep", sep)
             .csv(uri)
    )


def build_enriched(df_personales: DataFrame,
                   df_residencia: DataFrame,
                   df_generos: DataFrame,
                   df_tipos_identificacion: DataFrame,
                   df_municipios: DataFrame) -> DataFrame:
    """Perform the joins and return a flattened dataframe with final columns."""
    df = df_personales.alias('p').join(
        df_residencia.alias('r'), on='id_persona', how='left'
    )

    # Join with types and lookups, renaming ambiguous columns when needed
    df_gen = df_generos.withColumnRenamed('descripción', 'descripcion_genero')
    df_ti = df_tipos_identificacion.withColumnRenamed('tipo_identificacion', 'descripcion_identificacion')

    df = df.join(df_gen, on='codigo_genero', how='left')
    df = df.join(df_ti, on='codigo_identificacion', how='left')
    df = df.join(df_municipios, on='codigo_municipio', how='left')

    # Select and rename into a consistent final schema
    df_final = df.select(
        col('id_persona'),
        col('tipo_persona'),
        col('nombres'),
        col('apellidos'),
        col('numero_identificacion'),
        col('descripcion_identificacion').alias('tipo_identificacion'),
        col('telefono'),
        col('descripcion_genero').alias('genero'),
        col('direccion'),
        col('municipio'),
        col('departamento'),
        col('pais')
    )

    return df_final


def build_nested_json_df(df_final: DataFrame) -> DataFrame:
    """Build nested JSON structure and add 'id' as md5(id_persona).
    Returns a dataframe where each row is a JSON string column 'registro_persona_json' and also a flattened df ready for cosmos.
    """
    nested = struct(
        md5(col('id_persona').cast('string')).alias('id'),
        col('id_persona'),
        col('tipo_persona'),
        struct(
            col('nombres'),
            col('apellidos'),
            col('telefono'),
            struct(
                col('numero_identificacion').cast('string').alias('numero_identificacion'),
                col('tipo_identificacion').alias('tipo_identificacion')
            ).alias('identificacion_persona'),
            struct(
                col('genero')
            ).alias('genero'),
            struct(
                col('direccion'),
                col('municipio'),
                col('departamento'),
                col('pais')
            ).alias('datos_residencia'),
        ).alias('datos_personales'),

    )

    df_struct = df_final.select(nested.alias('registro'))

    df_flat = df_struct.select(
        col('registro.id').alias('id'),
        col('registro.id_persona'),
        col('registro.tipo_persona'),
        col('registro.datos_personales').alias('datos_personales')
    )

    return df_flat
