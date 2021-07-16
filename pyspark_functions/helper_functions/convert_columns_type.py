from pyspark.sql.types import *

def convert_columns_type(df, names, newType):
    for name in names:
        df = df.withColumn(name, df[name].cast(newType))
    return df
