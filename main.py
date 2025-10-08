# Databricks notebook source
from utils import (
    set_storage_account_key,
    read_parquet_landing,
    read_csv_ref,
    build_enriched,
    build_nested_json_df
)
from cosmos_utils import write_to_cosmos, read_from_cosmos

# COMMAND ----------

ACCOUNT             = dbutils.widgets.get("account")
COSMOS_ENDPOINT     = dbutils.widgets.get("cosmos-endpoint") 
COSMOS_KEY          = dbutils.widgets.get("cosmos-key")
STORAGE_ACCOUNT_KEY = dbutils.widgets.get("storage-account-key") 

CONTAINER_LANDING   = '' #Azure container name
CONTAINER_REF       = '' #Azure container name


# COMMAND ----------

set_storage_account_key(ACCOUNT, STORAGE_ACCOUNT_KEY)

# COMMAND ----------


df_personales = read_parquet_landing(CONTAINER_LANDING, ACCOUNT, 'datos_personales/')
df_residencia = read_parquet_landing(CONTAINER_LANDING ,ACCOUNT, 'datos_residencia/')


# COMMAND ----------

df_generos              = read_csv_ref(CONTAINER_REF, ACCOUNT, 'generos.csv', sep=';')
df_municipios           = read_csv_ref(CONTAINER_REF, ACCOUNT, 'municipios.csv', sep=';')
df_tipos_identificacion = read_csv_ref(CONTAINER_REF, ACCOUNT, 'tipos_identificacion.csv', sep=';')

# COMMAND ----------

df_final = build_enriched(
    df_personales,
    df_residencia,
    df_generos,
    df_tipos_identificacion,
    df_municipios
)

df_flat = build_nested_json_df(df_final)


# COMMAND ----------

cosmos_config = {
    'spark.cosmos.accountEndpoint': COSMOS_ENDPOINT,
    'spark.cosmos.accountKey': COSMOS_KEY,
    'spark.cosmos.database': '', #database Name
    'spark.cosmos.container': '', #container Name
    'spark.cosmos.write.strategy': 'ItemOverwrite', # ItemOverwrite ensures if the id already exists it will be replaced
    'spark.cosmos.write.bulk.enabled': 'true'
}

# COMMAND ----------

write_to_cosmos(df_flat, cosmos_config, mode='append')

# COMMAND ----------

# quick validation
df_cosmos = read_from_cosmos(cosmos_config)
display(df_cosmos)