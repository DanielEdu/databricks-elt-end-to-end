from pyspark.sql import DataFrame
from databricks.sdk.runtime import spark
from typing import Dict

def write_to_cosmos(df: DataFrame, cosmos_conf: Dict[str,str], mode: str='append'):
    """Write a dataframe to Cosmos DB using the spark connector.
    The connector expects a flattened dataframe where a column 'id' exists.
    """
    # Validate cosmos_conf contains required keys
    required = ['spark.cosmos.accountEndpoint', 'spark.cosmos.accountKey', 'spark.cosmos.database', 'spark.cosmos.container']
    for k in required:
        if k not in cosmos_conf or not cosmos_conf[k]:
            raise ValueError(f"Missing Cosmos DB config key: {k}")

    print("Writing to Cosmos DB container:", cosmos_conf.get('spark.cosmos.container'))

    (df
     .write
     .format('cosmos.oltp')
     .options(**cosmos_conf)
     .mode(mode)
     .save()
    )


def read_from_cosmos(cosmos_conf: Dict[str,str]) -> DataFrame:
    """Read entire container from Cosmos DB into a DataFrame."""
    conf = cosmos_conf.copy()
    conf.setdefault('spark.cosmos.read.inferSchema.enabled', 'true')

    df = spark.read.format('cosmos.oltp').options(**conf).load()
    return df