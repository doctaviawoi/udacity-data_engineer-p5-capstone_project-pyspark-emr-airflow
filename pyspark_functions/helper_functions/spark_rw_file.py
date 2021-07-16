import os
import glob
import pyspark.sql.functions as F
from pyspark.sql.types import *

from helper_functions.pyspark_column_rename import pyspark_column_rename

def spark_rw_file(spark, src_filepath, dest_filepath,\
                header="true", inferSchema="true", csv_delimiter=',',\
                xlsx_dataAddress="'Sheet1'!A1",\
                write_mode="append", n_repartition=1):

    """
    This function reads file in either csv, xlsx, sas7bdat or txt format in a source file path
    to a Spark dataframe, format column names to parquet file column name convention,
    and writes dataframe to parquet file format in a speficied target directory.

    Args:
        spark (SparkSession) : Current Spark session.
        src_filepath (string): File directory to read the file from.
        dest_filepath (string): File directory to store converted dataset in parquet file format to.
        header (Spark bool): Argument to pass onto useHeader option when reading csv and xlsx files, defaulted to "true".
        inferSchema (Spark bool): Argument to pass onto inferSchema option when reading csv and xlsx files, defaulted to "true".
        csv_delimiter (string): Argument to pass onto delimiter option when reading csv file, defaulted to ','.
        xlsx_dataAddress (string): Argument to pass onto dataAddress option to show which cell to start reading the xlsx file from,
                                    defaulted to "'Sheet1'!A1".
        write_mode (string): File saving pattern, either to 'append' or 'overwrite' existing file.
        n_repartition (integer): Number of repartition when saving dataframe.

    Returns:
        df (DataFrame): DataFrame saved in parquet format.
    """

    # get file extension and filename from src_filepath
    file_extension = src_filepath.split(".")[-1]
    df_name = src_filepath.split('/')[-1].split(".")[0]
    filename = src_filepath.replace(os.sep, '/').split('/')[-1]

    print(f"{df_name}")
    print("======================================================================")
    print(f"spark_rw_file - src_filepath: {src_filepath}.")
    print(f"spark_rw_file - dest_filepath: {dest_filepath}.")

    print(f"spark_rw_file - file_extension: {file_extension}.")
    print(f"spark_rw_file - filename: {filename}.")

    if file_extension == 'sas7bdat':
        df = spark.read.format('com.github.saurfang.sas.spark').load(src_filepath)

    elif file_extension == 'csv':
        df = spark.read.format('csv').option('inferSchema', inferSchema)\
                                    .option('header', header)\
                                    .option('delimiter', csv_delimiter)\
                                    .load(src_filepath)

    elif file_extension == 'xlsx':
        df = spark.read.format('com.crealytics.spark.excel')\
                                .option('inferSchema', inferSchema)\
                                .option('useHeader', header)\
                                .option('dataAddress',xlsx_dataAddress)\
                                .load(src_filepath)

    elif file_extension == 'txt':
        df = spark.read.text(src_filepath)


    # print number of rows and columns in df
    print(f"\nspark_rw_file - {df_name} raw dataframe contains {df.count()} rows and {len(df.columns)} columns.")

    # Rename column names that need fixing and write save as raw parquet file
    for c in df.columns:
        df = df.withColumnRenamed(c, pyspark_column_rename(c))

    print(f"spark_rw_file - Writing raw {df_name} to parquet type into {dest_filepath}...")
    df.repartition(n_repartition).write.mode(write_mode).parquet(dest_filepath)
    print(f"spark_rw_file - COMPLETED - Writing {df_name} to parquet type into {dest_filepath}.")

    parquet_path = os.path.join(dest_filepath, '*').replace(os.sep, '/')
    print(f"spark_rw_file - Reading {df_name} parquet files in {parquet_path}...")
    df = spark.read.parquet(parquet_path)
    print(f"\nspark_rw_file - {df_name} parquet dataframe contains {df.count()} rows and {len(df.columns)} columns.")
    print("spark_rw_file - Dataframe schema:")
    print(f"{df.printSchema()}")

    return df
