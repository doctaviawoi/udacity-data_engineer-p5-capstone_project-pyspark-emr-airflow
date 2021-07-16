import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from itertools import chain
import ast

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

from helper_functions.spark_rw_file import spark_rw_file
from helper_functions.standardise_i94countryname import standardise_countryname
from helper_functions.format_i94prtl import format_i94prtl
from helper_functions.convert_columns_type import convert_columns_type
from helper_functions.map_column_dict import map_column_dict



def transform_i94cntyl(spark, i94dicts_path, transformed_parq_path, output_path):
    """
    This function shows the ETL process of i94cntyl dataset.

    Args:
        spark (SparkSession): Current Spark session.
        i94dicts_path (string): Directory to i94dicts folder that contains i94cntyl.txt.
        transformed_parq_path (string): Directory folder in HDFS to read the
                                        transformed iban_iso3166, former_countries_iso3166_3
                                        and countries_codes_and_coordinates parquet files from.
        output_path (string): Directory folder in HDFS to write the transformed i94cnty dataframe to.
    """

    # get countries_ref_list and former_countries_dict
    sc = spark.sparkContext

    countries_ref_list_path = f"{transformed_parq_path}/countries_ref_list"
    countries_ref_list = sc.textFile(countries_ref_list_path).collect()

    former_countries_dict_path = f"{transformed_parq_path}/former_countries_dict"
    former_countries_dict = ast.literal_eval(sc.textFile(former_countries_dict_path).collect()[0])


    # import i94cntyl.txt
    i94cntyl_path = f'{i94dicts_path}/i94cntyl.txt'

    # convert python function standardise_countryname to pyspark udf
    def udf_standardise_countryname(countries_dict, ref_list):
        return F.udf(lambda c: standardise_countryname(c, countries_dict, ref_list))

    i94cntyl_df = sc.textFile(i94cntyl_path)\
                    .map(lambda line: line.split('='))\
                    .map(lambda p: Row(code=int(p[0]), country=p[1]))\
                    .toDF(["i94_cntyl_code", "country_i94"])\
                    .withColumn("country_iso3166", \
                                udf_standardise_countryname(former_countries_dict, countries_ref_list)(F.col("country_i94")))


    # join i94cntyl with joined iso3166_world table, and rename columns
    iso3166_world_path = f'{transformed_parq_path}/iso3166_world'
    iso3166_world = spark.read.parquet(iso3166_world_path)

    i94cntyl_df = i94cntyl_df.join(iso3166_world.select('iban_iso3', 'reference_country', 'ccc_lat', 'ccc_long'), \
                i94cntyl_df.country_iso3166==iso3166_world.reference_country, how='left')\
            .drop("reference_country")\
            .withColumnRenamed("iban_iso3", "country_code_iso3")\
            .withColumnRenamed("ccc_lat", "latitude")\
            .withColumnRenamed("ccc_long", "longitude")

    print("i94cntyl")
    print("======================================================================")
    print(f"i94cntyl parquet type dataframe contains {i94cntyl_df.count()} rows and {len(i94cntyl_df.columns)} columns.")
    print("\nSchema of transformed i94cntyl dataframe:")
    print(f"{i94cntyl_df.printSchema()}")
    i94cntyl_df.show(10, False)
    print("================================END===================================\n\n\n")

    i94cntyl_write_folder = f'{output_path}/i94cntyl'
    i94cntyl_df.repartition(1).write.mode('overwrite').parquet(i94cntyl_write_folder)

def transform_i94prtl(spark, i94dicts_path, transformed_parq_path, output_path):
    """
    This function shows the ETL process of i94prtl dataset.

    Args:
        spark (SparkSession): Current Spark session.
        i94dicts_path (string): Directory to i94dicts folder that contains i94prtl.txt.
        transformed_parq_path (string): Directory folder in HDFS to read the
                                        transformed airport-codes and us-port-codes
                                        parquet files from.
        output_path (string): Directory folder in HDFS to write the transformed i94cnty dataframe to.
    """
    # import i94prtl.txt
    sc = spark.sparkContext
    i94prtl_path = f'{i94dicts_path}/i94prtl.txt'

    i94prtl_df = sc.textFile(i94prtl_path)\
                    .map(lambda line: line.split('='))\
                    .map(lambda p: Row(code=p[0].replace("'","").strip(), country=p[1].replace("'", "").strip()))\
                    .toDF(["i94_prtl_code", "portloc_state_i94"])

    # split the rows with invalid labels such as 'No PORT Code', 'Collapsed'
    inv_i94prtl = i94prtl_df.filter((i94prtl_df.portloc_state_i94.like('No PORT Code%')) | \
                                (i94prtl_df.portloc_state_i94.like('Collapsed%')))

    # Find possible port location in airport dataset, columns iata_code and gps_code
    airport_trfpath = f'{transformed_parq_path}/airport-codes'

    airport_cols = ["gps_code", "iata_code", "name", "municipality", "iso2_country", "iso2_region", "latitude", "longitude"]
    airport = spark.read.parquet(airport_trfpath)\
                    .select(airport_cols)

    # Join airport with iban_iso3166
    iban_iso3166_trfpath = f'{transformed_parq_path}/iban_iso3166'
    iban_countries = spark.read.parquet(iban_iso3166_trfpath)

    airport_iban_df = airport.join(iban_countries.select("iban_iso2", "iban_country"), \
                                 airport.iso2_country==iban_countries.iban_iso2,\
                                 how="left")\
                            .withColumn("airport_loc_state", \
                                        F.when(F.col("iso2_country")=='US', F.concat(F.col("municipality"), F.lit(', '), F.split(F.col("iso2_region"), '-')[1]))\
                                        .otherwise(F.concat(F.col("municipality"), F.lit(', '), F.col("iban_country"))))\
                            .drop("iban_iso2")

    # subset gps_code and airport_loc_state columns
    airport_gps = airport_iban_df.select("gps_code", "airport_loc_state")\
                                        .na.drop(how="any")\
                                        .dropDuplicates()\
                                        .withColumnRenamed("airport_loc_state", "air_gps_loc")\
                                        .orderBy("gps_code")

    # subset iata_code and airport_loc_state columns
    airport_iata = airport_iban_df.select("iata_code", "airport_loc_state")\
                                        .na.drop(how="any")\
                                        .filter(airport_iban_df.iata_code != '0')\
                                        .dropDuplicates()\
                                         .withColumnRenamed("airport_loc_state", "air_iata_loc")\
                                        .orderBy("iata_code")

    # Find possible location in us-port-codes parquet
    locode_path = f'{transformed_parq_path}/us-port-codes'
    locode_port_codes = spark.read.parquet(locode_path)

    # Join inv_i94prtl with airport_gps, airport_iata, locode_port_codes
    inv_i94prtl = inv_i94prtl.join(airport_gps, \
                                inv_i94prtl.i94_prtl_code==airport_gps.gps_code, \
                                how="left")\
                            .drop("gps_code")\
                            .join(airport_iata, \
                                inv_i94prtl.i94_prtl_code==airport_iata.iata_code, \
                                how="left")\
                            .drop("iata_code")\
                            .join(locode_port_codes, \
                                inv_i94prtl.i94_prtl_code==locode_port_codes.us_port_city_code, \
                                how="left")\
                            .drop("us_port_city_code")

    # Combine values in column us_locode_loc with 'air_gps_loc' when 'us_locode_loc' is None.
    inv_i94prtl = inv_i94prtl.withColumn("reference_port_loc", \
                                    F.when(F.col("us_locode_loc").isNull(), F.col("air_gps_loc"))\
                                    .otherwise(F.col("us_locode_loc")))\
                            .drop("air_gps_loc", "air_iata_loc", "us_locode_loc")\
                            .dropDuplicates(["i94_prtl_code"])

    # Remove row that is null in 'reference_port_loc' column
    inv_i94prtl = inv_i94prtl.filter(inv_i94prtl.reference_port_loc.isNotNull())

    # Now clean up column i94prtl with valid port location
    valid_i94prtl_df = i94prtl_df.filter(~((i94prtl_df.portloc_state_i94.like('No PORT Code%')) | \
                                (i94prtl_df.portloc_state_i94.like('Collapsed%'))))\
                            .orderBy("portloc_state_i94")

    # get countries_ref_list and former_countries_dict
    countries_ref_list_path = f"{transformed_parq_path}/countries_ref_list"
    countries_ref_list = sc.textFile(countries_ref_list_path).collect()

    former_countries_dict_path = f"{transformed_parq_path}/former_countries_dict"
    former_countries_dict = ast.literal_eval(sc.textFile(former_countries_dict_path).collect()[0])


    # convert python function to spark udf
    format_i94prtl_udf = F.udf(lambda s, d, l: format_i94prtl(s, d, l), StringType())

    # apply format_i94prtl_udf to column 'portloc_state_i94'
    valid_i94prtl_df = valid_i94prtl_df.withColumn("reference_port_loc", \
                                                    format_i94prtl_udf(F.col("portloc_state_i94"),\
                                                                        F.create_map([F.lit(x) for x in chain(*former_countries_dict.items())]),\
                                                                        F.array([F.lit(i) for i in countries_ref_list])))

    # Append valid_i94prtl_df and inv_i94prtl
    i94prtl_clean = valid_i94prtl_df.union(inv_i94prtl)

    print("i94prtl")
    print("======================================================================")
    print(f"i94prtl parquet type dataframe contains {i94prtl_clean.count()} rows and {len(i94prtl_clean.columns)} columns.")
    print("\nSchema of transformed i94prtl dataframe:")
    print(f"{i94prtl_clean.printSchema()}")
    i94prtl_clean.show(10, False)
    print("================================END===================================\n\n\n")

    i94prtl_write_folder = f'{output_path}/i94prtl'
    i94prtl_clean.repartition(1).write.mode('overwrite').parquet(i94prtl_write_folder)


def transform_us_immi(spark, s3_bucket, us_immi_raw_path, us_immi_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of I94 US International Arrival dataset.

    Args:
        spark (SparkSession): Current Spark session.
        s3_bucket (string): Name of S3 bucket to check which I94 US immigration files already existed.
        us_immi_raw_path (string): Directory of I94 US immigration sas7bdat files in HDFS.
        us_immi_parquet_path (string): Directory of parquet converted I94 US immigration file in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """

    # read parquet file
    us_immi_df = spark_rw_file(spark, us_immi_raw_path, us_immi_parquet_path,\
                                write_mode="append", n_repartition=10)

    # list of columns to process
    us_immi_cols = ['cicid', 'i94yr', 'i94mon', 'biryear', 'gender',
                  'i94res', 'i94cit', 'arrdate', 'i94mode', 'i94port',
                  'airline', 'fltno', 'i94visa', 'i94addr', 'depdate']

    # convert sas format date
    us_immi_df = us_immi_df.select(us_immi_cols)\
                            .dropDuplicates()\
                            .withColumn("sas_date", F.lit("1960-01-01"))\
                            .withColumn("arrival_date", F.expr("date_add(sas_date, arrdate)"))\
                            .withColumn("depdate", F.when(us_immi_df["depdate"].isNull(), '0').otherwise(us_immi_df['depdate']))\
                            .withColumn('departure_date', F.expr("date_add(sas_date, depdate)"))\
                            .drop("arrdate", "depdate", "sas_date")

    # cast cicid, i94yr, i94mon, biryear, 'i94res', 'i94cit' to integer type
    double_to_int_cols = ["cicid", "i94yr", "i94mon", "biryear", "i94res", "i94cit"]

    us_immi_df = convert_columns_type(us_immi_df, double_to_int_cols, IntegerType())


    # map values in columns i94mode and i94visa according to its label description

    #### transport modes
    global transport_mode_dict
    transport_mode_dict = {}

    sc = spark.sparkContext

    i94model_df = sc.textFile(f"hdfs:///home/hadoop/{s3_bucket}/data/rawdata/i94dicts/i94model.txt")\
                    .map(lambda line: line.split('='))\
                    .map(lambda p: Row(code=int(p[0]), transport_mode=p[1].strip().replace("'", "")))\
                    .toDF(["code", "transport_mode"])

    transport_mode_dict = i94model_df.toPandas().set_index("code").to_dict()["transport_mode"]


    #### visa category
    global visa_num_dict
    visa_num_dict = {}

    i94visa_df = sc.textFile(f"hdfs:///home/hadoop/{s3_bucket}/data/rawdata/i94dicts/i94visa.txt")\
                    .map(lambda line: line.split('='))\
                    .map(lambda p: Row(code=int(p[0]), visa=p[1].replace("'", "").strip()))\
                    .toDF(["code", "visa"])

    visa_num_dict = i94visa_df.toPandas().set_index("code").to_dict()["visa"]


    #### map dictionaries to us_immi_df
    dict_kw = {
           'i94mode': [transport_mode_dict, 'transport_mode'],
           'i94visa': [visa_num_dict, 'visa_category']
          }

    us_immi_df = us_immi_df.select("*",\
                                *(map_column_dict(old_col, d_new_col[0], d_new_col[1]) \
                                        for old_col, d_new_col in dict_kw.items()))\
                            .drop("i94mode", "i94visa")

    # Rename us_immi_df
    rename_map = dict(zip(["i94yr", 'i94mon', 'biryear', 'i94res',
                            'i94cit', 'i94port', 'fltno', 'i94addr'],
                            ['year', 'month', 'birth_year', 'residence_country',
                            'citizenship_country', 'port_of_entry', 'flight_no', 'address_state']))

    us_immi_df = us_immi_df.select([F.col(c).alias(rename_map.get(c, c)) for c in us_immi_df.columns])


    print(f"I94 US International Arrival parquet type dataframe contains {us_immi_df.count()} rows and {len(us_immi_df.columns)} columns.")
    print("\nSchema of transformed I94 US International Arrival dataframe:")
    print(f"{us_immi_df.printSchema()}")
    us_immi_df.show(10, False)
    print("================================END===================================\n\n\n")

    us_immi_df.repartition(10).write.mode('append').partitionBy("year", "month").parquet(clean_parquet_output)

def transform_time(spark, i94_immi_trfpath, clean_parquet_output):
    """
    This function shows the ETL process of time dimension of arrival_date column in I94 US immigration dataset.

    Args:
        spark (SparkSession): Current Spark session.
        i94_immi_trfpath (string): Directory where the transformed US immigration parquet file is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    arr_time_df = spark.read.parquet(i94_immi_trfpath)\
                        .select("arrival_date", "year", "month")\
                        .distinct()\
                        .withColumnRenamed("arrival_date", "date")\
                        .orderBy("date")

    arr_time_df = arr_time_df.withColumn("day_of_month", F.dayofmonth(F.col("date")))\
                            .withColumn("day_of_week", F.dayofweek(F.col("date")))\
                            .withColumn("week_of_year", F.weekofyear(F.col("date")))

    print("time")
    print("======================================================================")
    print(f"time parquet type dataframe contains {arr_time_df.count()} rows and {len(arr_time_df.columns)} columns.")
    print("\nSchema of time dataframe:")
    print(f"{arr_time_df.printSchema()}")
    arr_time_df.show(10, False)
    print("================================END===================================\n\n\n")

    arr_time_df.repartition(1).write.mode('append').partitionBy("year", "month").parquet(clean_parquet_output)
