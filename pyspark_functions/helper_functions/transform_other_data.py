import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import ast

import pyspark.sql.functions as F
from pyspark.sql.types import *

from helper_functions.spark_rw_file import spark_rw_file
from helper_functions.standardise_i94countryname import standardise_countryname
from helper_functions.map_column_dict import map_column_dict



def transform_us_state(spark, us_state_raw_path, clean_parquet_output):
    """
    This function shows the ETL process of us-state-boundaries dataset.

    Args:
        spark (SparkSession): Current Spark session.
        us_state_raw_path (string): Directory where the us-state-boundaries.csv file is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    us_state_cols = ["stusab", "name", "centlat", "centlon"]

    us_state_df = spark.read.format('csv').option('inferSchema', 'true')\
                                    .option('header', 'true')\
                                    .option('delimiter', ';')\
                                    .load(us_state_raw_path)\
                                    .select(us_state_cols)

    rename_map = dict(zip(["stusab", "name", "centlat", "centlon"],
                            ["state_code", "state_name", "latitude", "longitude"]))

    us_state_df = us_state_df.select(us_state_cols)\
                            .dropDuplicates()\
                            .select([F.col(c).alias(rename_map.get(c, c)) for c in us_state_cols])

    print("\n\nus-state-boundaries")
    print("======================================================================")
    print(f"us-state-boundaries parquet type dataframe contains {us_state_df.count()} rows and {len(us_state_df.columns)} columns.")
    print("\nSchema of transformed us-state-boundaries dataframe:")
    print(f"{us_state_df.printSchema()}")
    us_state_df.show(10, False)
    print("================================END===================================\n\n\n")

    us_state_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)


def transform_us_demo(spark, us_demo_raw_path, us_demo_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of us-cities-demographics dataset.

    Args:
        spark (SparkSession): Current Spark session.
        us_demo_raw_path (string): Directory where the us-cities-demographics.csv file is stored in HDFS.
        us_demo_parquet_path (string): Directory where the parquet converted us-cities-demographics dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    us_demo_df = spark_rw_file(spark, us_demo_raw_path, us_demo_parquet_path,\
                            csv_delimiter=';', write_mode="overwrite", n_repartition=1)

    us_demo_cols = ["state_code", "state", "city", "male_population",
                    "female_population", "median_age"]

    us_demo_df = us_demo_df.select(us_demo_cols)\
                    .dropDuplicates()\
                    .na.drop()\
                    .orderBy("state", "city")


    print(f"us-cities-demographics parquet type dataframe contains {us_demo_df.count()} rows and {len(us_demo_df.columns)} columns.")
    print("\nSchema of transformed us-cities-demographics dataframe:")
    print(f"{us_demo_df.printSchema()}")
    us_demo_df.show(10, False)
    print("================================END===================================\n\n\n")

    us_demo_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)


def transform_iban_iso3166(spark, iban_iso3166_raw_path, iban_iso3166_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of iban_iso3166 dataset.

    Args:
        spark (SparkSession): Current Spark session.
        iban_iso3166_raw_path (string): Directory where the iban_iso3166.csv file is stored in HDFS.
        iban_iso3166_parquet_path (string): Directory where the parquet converted iban_iso3166 dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    iban_iso3166_df = spark_rw_file(spark, iban_iso3166_raw_path, iban_iso3166_parquet_path,\
                                    write_mode="overwrite", n_repartition=1)


    rename_map = dict(zip(["country", "alpha-2_code", "alpha-3_code"],
                            ["iban_country", "iban_iso2", "iban_iso3"]))

    iban_iso3166_df = iban_iso3166_df\
                        .select([F.col(c).alias(rename_map.get(c, c)) for c in list(rename_map.keys())])

    print(f"iban_iso3166 parquet type dataframe contains {iban_iso3166_df.count()} rows and {len(iban_iso3166_df.columns)} columns.")
    print("\nSchema of transformed iban_iso3166 dataframe:")
    print(f"{iban_iso3166_df.printSchema()}")
    iban_iso3166_df.show(10, False)
    print("================================END===================================\n\n\n")

    iban_iso3166_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)

def transform_cc_coordinates(spark, cc_coordinates_raw_path, cc_coordinates_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of countries_codes_and_coordinates dataset.

    Args:
        spark (SparkSession): Current Spark session.
        cc_coordinates_raw_path (string): Directory where the countries_codes_and_coordinates.csv file is stored in HDFS.
        cc_coordinates_parquet_path (string): Directory where the parquet converted countries_codes_and_coordinates dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    cc_coordinates_df = spark_rw_file(spark, cc_coordinates_raw_path, cc_coordinates_parquet_path,\
                                    write_mode="overwrite", n_repartition=1)

    rename_map = dict(zip(["alpha-3_code", "alpha-2_code", "country", "latitude_average", "longitude_average"],
                            ["ccc_iso3", "ccc_iso2", "ccc_country", "ccc_lat", "ccc_long"]))

    cc_coordinates_df = cc_coordinates_df.withColumn("alpha-2_code", F.regexp_replace(F.col("alpha-2_code"), '[ "]', ''))\
            .withColumn("alpha-3_code", F.regexp_replace(F.col("alpha-3_code"), '[ "]', ''))\
            .withColumn("latitude_average", F.regexp_replace(F.col("latitude_average"), '[ "]', '').cast("float"))\
            .withColumn("longitude_average", F.regexp_replace(F.col("longitude_average"), '[ "]', '').cast("float"))\
            .select([F.col(c).alias(rename_map.get(c, c)) for c in list(rename_map.keys())])

    print(f"countries_codes_and_coordinates parquet type dataframe contains {cc_coordinates_df.count()} rows and {len(cc_coordinates_df.columns)} columns.")
    print("\nSchema of transformed countries_codes_and_coordinates dataframe:")
    print(f"{cc_coordinates_df.printSchema()}")
    cc_coordinates_df.show(10, False)
    print("================================END===================================\n\n\n")

    cc_coordinates_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)


def transform_former_countries(spark, former_countries_raw_path, former_countries_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of former_countries_iso3166_3 dataset.

    Args:
        spark (SparkSession): Current Spark session.
        former_countries_raw_path (string): Directory where the former_countries_iso3166_3.csv file is stored in HDFS.
        former_countries_parquet_path (string): Directory where the parquet converted former_countries_iso3166_3 dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    former_countries_df = spark_rw_file(spark, former_countries_raw_path, former_countries_parquet_path,\
                                    write_mode="overwrite", n_repartition=1)

    print(f"former_countries_iso3166_3 parquet type dataframe contains {former_countries_df.count()} rows and {len(former_countries_df.columns)} columns.")
    print("\nSchema of transformed former_countries_iso3166_3 dataframe:")
    print(f"{former_countries_df.printSchema()}")
    former_countries_df.show(10, False)
    print("================================END===================================\n\n\n")

    former_countries_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)


def write_countriesnamelist_formercountriesdict(spark, transformed_parq_path):
    """
    This function writes the reference list of country names in ISO3166-1 obtained
    from iban_iso3166 parquet and countries_codes_and_coordinates, and dictionary
    of former country name and its adapted country name(s) from former_countries_iso3166_3
    parquet, to HDFS.

    Args:
        spark (SparkSession): Current Spark session.
        transformed_parq_path (string): Directory where to read the transformed iban_iso3166,
                                        countries_codes_and_coordinates, and
                                        former_countries_iso3166_3 parquet files from in HDFS,
                                        and to write pickle files of countries_ref_list and
                                        former_countries_dict to in HDFS.
    """

    # Get country names reference list
    # 1. read iban_iso3166 and country_codes_and_coordinates parquets

    iban_iso3166_trfpath = f'{transformed_parq_path}/iban_iso3166'
    cc_coordinates_trfpath = f'{transformed_parq_path}/countries_codes_and_coordinates'

    iban_countries = spark.read.parquet(iban_iso3166_trfpath)
    cc_coordinates = spark.read.parquet(cc_coordinates_trfpath)

    # 2. join iban_countries and cc_coordinates
    iso3166_world = iban_countries.join(cc_coordinates, \
                                         iban_countries['iban_iso3']==cc_coordinates['ccc_iso3'],\
                                         how="left")\
                                    .select('*', \
                                           F.when(F.col("ccc_country").isNull(), F.col("iban_country"))\
                                               .otherwise(F.col("ccc_country")).alias("reference_country"))\
                                    .orderBy("iban_iso3")

    print("iso3166_world")
    print("======================================================================")
    print(f"iso3166_world parquet type dataframe contains {iso3166_world.count()} rows and {len(iso3166_world.columns)} columns.")
    print("\nSchema of transformed iso3166_world dataframe:")
    print(f"{iso3166_world.printSchema()}")
    iso3166_world.show(10, False)
    print("================================END===================================\n\n\n")

    iso3166_world_write_folder = f'{transformed_parq_path}/iso3166_world'
    iso3166_world.repartition(1).write.mode('overwrite').parquet(iso3166_world_write_folder)

    # 3. get countries_ref_list from the joined table
    countries_ref = iso3166_world.select(F.col("reference_country"))\
                                        .na.drop()\
                                        .dropDuplicates()\
                                        .orderBy("reference_country")\
                                        .toPandas()

    countries_ref_list = list(countries_ref.loc[:, "reference_country"])
    countries_ref_list_path = f"{transformed_parq_path}/countries_ref_list"

    sc = spark.sparkContext
    sc.parallelize(countries_ref_list).coalesce(1).map(lambda row: str(row)).saveAsTextFile(countries_ref_list_path)


    # Get former countries dictionary
    # 1. read former_countries_iso3166_3 parquet
    former_countries_trfpath = f'{transformed_parq_path}/former_countries_iso3166_3'
    former_countries = spark.read.parquet(former_countries_trfpath)

    # convert to dictionary
    former_countries_dict = former_countries.toPandas().set_index("former_country_name")\
                                .to_dict()["new_country_names"]

    former_countries_dict_path = f"{transformed_parq_path}/former_countries_dict"
    sc.parallelize([former_countries_dict]).coalesce(1).saveAsTextFile(former_countries_dict_path)




def transform_airport(spark, airport_raw_path, airport_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of airport-codes dataset.

    Args:
        spark (SparkSession): Current Spark session.
        airport_raw_path (string): Directory where the airport-codes.csv file is stored in HDFS.
        airport_parquet_path (string): Directory where the parquet converted airport-codes dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    airport_df = spark_rw_file(spark, airport_raw_path, airport_parquet_path,\
                                write_mode="overwrite", n_repartition=1)

    rename_map = dict(zip(['iso_country', 'iso_region'], ['iso2_country', 'iso2_region']))

    airport_df = airport_df.select([F.col(c).alias(rename_map.get(c, c)) for c in airport_df.columns])\
                                .dropDuplicates()\
                                .filter(airport_df.coordinates != '0, 0')\
                                .withColumn("latitude", F.split("coordinates", ', ')[0].cast("float"))\
                                .withColumn("longitude", F.split("coordinates", ', ')[1].cast("float"))\
                                .drop("local_code", "coordinates")

    print(f"airport-codes parquet type dataframe contains {airport_df.count()} rows and {len(airport_df.columns)} columns.")
    print("\nSchema of transformed airport-codes dataframe:")
    print(f"{airport_df.printSchema()}")
    airport_df.show(10, False)
    print("================================END===================================\n\n\n")

    airport_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)

def transform_us_port(spark, us_port_raw_path, us_port_parquet_path, clean_parquet_output):
    """
    This function shows the ETL process of us-port-codes dataset.

    Args:
        spark (SparkSession): Current Spark session.
        us_port_raw_path (string): Directory where the us-port-codes.csv file is stored in HDFS.
        us_port_parquet_path (string): Directory where the parquet converted us-port-codes dataframe is stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    us_port_df = spark_rw_file(spark, us_port_raw_path, us_port_parquet_path,\
                                header='false', xlsx_dataAddress="'Sheet1'!A2",\
                                write_mode="overwrite", n_repartition=1)

    us_port_df = us_port_df.withColumn("us_port_city_code", F.substring("_c0", 3,3))\
                            .withColumn("us_locode_loc", F.split(us_port_df["_c0"], ': ')[1])\
                            .drop("_c0")\
                            .dropDuplicates()

    print(f"us-port-codes parquet type dataframe contains {us_port_df.count()} rows and {len(us_port_df.columns)} columns.")
    print("\nSchema of transformed us-port-codes dataframe:")
    print(f"{us_port_df.printSchema()}")
    us_port_df.show(10, False)
    print("================================END===================================\n\n\n")

    us_port_df.repartition(1).write.mode('overwrite').parquet(clean_parquet_output)


def transform_globalTemp(spark, globalTemp_raw_path, globalTemp_parquet_path, transformed_parq_path, clean_parquet_output):
    """
    This function shows the ETL process of GlobalLandTemperaturesByCity dataset.

    Args:
        spark (SparkSession): Current Spark session.
        globalTemp_raw_path (string): Directory where the GlobalLandTemperaturesByCity.csv file is stored in HDFS.
        globalTemp_parquet_path (string): Directory where the parquet converted
                                          GlobalLandTemperaturesByCity dataframe is stored in HDFS.
        transformed_parq_path (string): Directory where the transformed iban_iso3166,
                                        countries_codes_and_coordinates, and
                                        former_countries_iso3166_3 parquet files are stored in HDFS.
        clean_parquet_output (string): Directory where the transformed parquet file will be written to in HDFS.
    """
    globalTemp_df = spark_rw_file(spark, globalTemp_raw_path, globalTemp_parquet_path,\
                                    write_mode="overwrite", n_repartition=1)

    globalTemp_cols = ['date', 'averagetemperature', 'averagetemperatureuncertainty',
            'city', 'country', 'latitude', 'longitude']

    convert_coordinate = F.udf(lambda x: float(x[:-1])*-1 if x[-1] in ['S', 'W'] else float(x[:-1]), DoubleType())

    globalTemp_df = globalTemp_df.withColumn("date", F.split('dt', ' ')[0].cast(DateType()))\
                        .drop("dt")\
                        .na.drop(subset=["averagetemperature"])\
                        .withColumn("latitude", convert_coordinate(globalTemp_df.latitude))\
                        .withColumn("longitude", convert_coordinate(globalTemp_df.longitude))\
                        .select(globalTemp_cols)\
                        .dropDuplicates()\
                        .orderBy("date", "country", "city")

    # Get distinct country in globalTemp_df
    gT_country = globalTemp_df.select("country").distinct().toPandas()


    # get countries_ref_list and former_countries_dict
    sc = spark.sparkContext

    countries_ref_list_path = f"{transformed_parq_path}/countries_ref_list"
    countries_ref_list = sc.textFile(countries_ref_list_path).collect()

    former_countries_dict_path = f"{transformed_parq_path}/former_countries_dict"
    former_countries_dict = ast.literal_eval(sc.textFile(former_countries_dict_path).collect()[0])

    # Get a dictionary of country name in globalTemp_df as key, and standardised country name as value
    gT_country_dict = {country: standardise_countryname(country, former_countries_dict, countries_ref_list) for country in list(gT_country["country"])}

    # Map the ISO-3166 standardised country name
    globalTemp_df = globalTemp_df.select("*", \
                                        map_column_dict("country", gT_country_dict, "country_iso3166"))\
                                .drop("country")\
                                .select("date", "country_iso3166", "city", "averagetemperature", "averagetemperatureuncertainty", "latitude", "longitude")

    print(f"GlobalLandTemperaturesByCity parquet type dataframe contains {globalTemp_df.count()} rows and {len(globalTemp_df.columns)} columns.")
    print("\nSchema of transformed GlobalLandTemperaturesByCity dataframe:")
    print(f"{globalTemp_df.printSchema()}")
    globalTemp_df.show(10, False)
    print("================================END===================================\n\n\n")

    globalTemp_df.repartition("country_iso3166").write.partitionBy("country_iso3166").mode('overwrite').parquet(clean_parquet_output)
