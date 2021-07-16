from datetime import datetime
import os
import glob
from subprocess import Popen, PIPE

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

#from airflow.contrib.hooks.aws_hook import AwsHook

from helper_functions.create_spark_session import create_spark_session
from helper_functions.get_s3_keys_list import get_s3_keys_list

from helper_functions.transform_other_data import transform_us_state, transform_us_demo
from helper_functions.transform_other_data import transform_iban_iso3166, transform_cc_coordinates
from helper_functions.transform_other_data import transform_former_countries, transform_globalTemp
from helper_functions.transform_other_data import write_countriesnamelist_formercountriesdict
from helper_functions.transform_other_data import transform_airport, transform_us_port
from helper_functions.transform_immi_data import transform_i94cntyl, transform_i94prtl
from helper_functions.transform_immi_data import transform_us_immi, transform_time

# note: find a way to soft code s3_bucket name
s3_bucket = 'udacity-dend-capstone-project'


def main():
    # get AWS profile access key
    #aws_hook = AwsHook("aws_credentials")
    #aws_credentials = aws_hook.get_credentials()

    # create spark session
    spark = create_spark_session(AppName='Capstone Project - US Tourism and Climate Analysis (Airflow-EMR-PySpark)'
                                )


    ############## Main files directory in HDFS
    hdfs_raw_data_path = f'hdfs:///home/hadoop/{s3_bucket}/data/rawdata'
    hdfs_raw_parquet_path = f'hdfs:///home/hadoop/{s3_bucket}/data/rawdata_parquet'
    hdfs_trf_parquet_path = f'hdfs:///home/hadoop/{s3_bucket}/data/transformed_parquet'
    hdfs_output_path = f'hdfs:///home/hadoop/{s3_bucket}/output_to_s3'

    # transform and clean raw data to parquet file
    ##### us-state-boundaries dataset
    us_state_file = 'us-state-boundaries.csv'
    us_state_parq_folder = us_state_file.split('.')[0]

    us_state_filepath = f'{hdfs_raw_data_path}/{us_state_file}'
    us_state_cleanparq_filepath = f'{hdfs_output_path}/{us_state_parq_folder}'

    transform_us_state(spark, us_state_filepath, us_state_cleanparq_filepath)


    ##### US-demographic-2015 dataset
    us_demo_file = 'us-cities-demographics.csv'
    us_demo_parq_folder = us_demo_file.split('.')[0]

    us_demo_filepath = f'{hdfs_raw_data_path}/{us_demo_file}'
    us_demo_parq_filepath = f'{hdfs_raw_parquet_path}/{us_demo_parq_folder}'
    us_demo_cleanparq_filepath = f'{hdfs_output_path}/{us_demo_parq_folder}'

    transform_us_demo(spark, us_demo_filepath, us_demo_parq_filepath, us_demo_cleanparq_filepath)


    ##### iban_iso3166 dataset
    iban_iso3166_file = 'iban_iso3166.csv'
    iban_iso3166_parq_folder = iban_iso3166_file.split('.')[0]

    iban_iso3166_filepath = f'{hdfs_raw_data_path}/{iban_iso3166_file}'
    iban_iso3166_parq_filepath = f'{hdfs_raw_parquet_path}/{iban_iso3166_parq_folder}'
    iban_iso3166_cleanparq_filepath = f'{hdfs_trf_parquet_path}/{iban_iso3166_parq_folder}'

    transform_iban_iso3166(spark, iban_iso3166_filepath, iban_iso3166_parq_filepath, iban_iso3166_cleanparq_filepath)


    ##### countries_codes_and_coordinates dataset
    cc_coordinates_file = 'countries_codes_and_coordinates.csv'
    cc_coordinates_parq_folder = cc_coordinates_file.split('.')[0]

    cc_coordinates_filepath = f'{hdfs_raw_data_path}/{cc_coordinates_file}'
    cc_coordinates_parq_filepath = f'{hdfs_raw_parquet_path}/{cc_coordinates_parq_folder}'
    cc_coordinates_cleanparq_filepath = f'{hdfs_trf_parquet_path}/{cc_coordinates_parq_folder}'

    transform_cc_coordinates(spark, cc_coordinates_filepath, cc_coordinates_parq_filepath, cc_coordinates_cleanparq_filepath)


    ##### former countries dataset
    former_countries_file = 'former_countries_iso3166_3.csv'
    former_countries_parq_folder = former_countries_file.split('.')[0]

    former_countries_filepath = f'{hdfs_raw_data_path}/{former_countries_file}'
    former_countries_parq_filepath = f'{hdfs_raw_parquet_path}/{former_countries_parq_folder}'
    former_countries_cleanparq_filepath = f'{hdfs_trf_parquet_path}/{former_countries_parq_folder}'

    transform_former_countries(spark, former_countries_filepath, former_countries_parq_filepath, former_countries_cleanparq_filepath)

    # write countries_ref_list and former_countries_dict
    write_countriesnamelist_formercountriesdict(spark, hdfs_trf_parquet_path)

    ##### airport-codes
    airport_file = 'airport-codes.csv'
    airport_parq_folder = airport_file.split('.')[0]

    airport_filepath = f'{hdfs_raw_data_path}/{airport_file}'
    airport_parq_filepath = f'{hdfs_raw_parquet_path}/{airport_parq_folder}'
    airport_cleanparq_filepath = f'{hdfs_trf_parquet_path}/{airport_parq_folder}'

    transform_airport(spark, airport_filepath, airport_parq_filepath,airport_cleanparq_filepath)


    ##### US seaport-codes
    us_port_file = 'us-port-codes.xlsx'
    us_port_parq_folder = us_port_file.split('.')[0]

    us_port_filepath = f'{hdfs_raw_data_path}/{us_port_file}'
    us_port_parq_filepath = f'{hdfs_raw_parquet_path}/{us_port_parq_folder}'
    us_port_cleanparq_filepath = f'{hdfs_trf_parquet_path}/{us_port_parq_folder}'

    transform_us_port(spark, us_port_filepath, us_port_parq_filepath, us_port_cleanparq_filepath)


    ##### Kaggle global land temperature
    globalTemp_file = 'GlobalLandTemperaturesByCity.csv'
    globalTemp_parq_folder = globalTemp_file.split('.')[0]

    globalTemp_filepath = f'{hdfs_raw_data_path}/{globalTemp_file}'
    globalTemp_parq_filepath = f'{hdfs_raw_parquet_path}/{globalTemp_parq_folder}'
    globalTemp_cleanparq_filepath = f'{hdfs_output_path}/{globalTemp_parq_folder}'

    transform_globalTemp(spark, globalTemp_filepath, globalTemp_parq_filepath, hdfs_trf_parquet_path, globalTemp_cleanparq_filepath)


    ###### i94cntyl and i94prtl
    i94dicts_path = f'{hdfs_raw_data_path}/i94dicts'
    transform_i94cntyl(spark, i94dicts_path, hdfs_trf_parquet_path, hdfs_output_path)
    transform_i94prtl(spark, i94dicts_path, hdfs_trf_parquet_path, hdfs_output_path)


    ###### US Immigration 2016
    print("                      I94 US International Arrival in 2016")
    print("======================================================================================================")

    # get list of *.sas7bdat files and only process file that has not been read.
    us_immi_files_format = '*.sas7bdat'

    s3_clean_immi_key = '/data/processedData/us_immi'

    # check us_immi_parquet in s3
    _, s3_clean_immi_files = get_s3_keys_list(s3_bucket, s3_clean_immi_key, access_source='ec2')
    print(f"s3_clean_immi_files: \n\t{s3_clean_immi_files}\n")

    # check us_immi raw files in hdfs. Note: IN HDFS os module does not work.
    #hdfs_raw_immi_path = os.path.join(hdfs_raw_data_path, "immi", us_immi_files_format).replace(os.sep, '/')
    hdfs_raw_immi_path = f"{hdfs_raw_data_path}/immi"
    print(f"hdfs_raw_immi_path: {hdfs_raw_immi_path}")

    #hdfs_raw_immi_files = [f.replace(os.sep, '/').split('/')[-1][4:9] for f in glob.glob(hdfs_raw_immi_path)]
    process = Popen(f'hadoop fs -ls {hdfs_raw_immi_path}', shell=True, stdout=PIPE, stderr=PIPE)
    std_out, std_err = process.communicate()
    print(f"std_out: \n\t{std_out}\n")
    print(f"std_err: \n\t{std_err}\n")
    hdfs_raw_immi_files = [f.split(' ')[-1].split('/')[-1] for f in std_out.decode().split('\n')[1:-1] if f.find('.') != -1]
    hdfs_raw_immi_files_fullpath = [f.split(' ')[-1] for f in std_out.decode().split('\n')[1:-1] if f.find('.') != -1]
    print(f"hdfs_raw_immi_files: \n\t{hdfs_raw_immi_files}\n")
    print(f"hdfs_raw_immi_files_fullpath: \n\t{hdfs_raw_immi_files_fullpath}\n")

    print(f"In HDFS, sas7bdat immigration files with these following dates are found:\n\t{hdfs_raw_immi_files}.\n")

    if s3_clean_immi_files != []:
        s3_clean_immi_date = [i[-5:] for i in s3_clean_immi_files]
        print(f"In S3, clean immigration files with these following dates are found:\n\t{s3_clean_immi_date}.\n")

        immi_dates_to_process = list(set(hdfs_raw_immi_files)-set(s3_clean_immi_date))

        immi_files_to_process = [f'{hdfs_raw_immi_path}/i94_{x}_sub.sas7bdat' for x in immi_dates_to_process]
        print(f"US Immigration files to process:\n\t{immi_files_to_process}.\n")


    else:
        #immi_files_to_process = [j.replace(os.sep, '/') for j in glob.glob(hdfs_raw_immi_path)]
        immi_files_to_process = hdfs_raw_immi_files_fullpath
        print(f"No clean US Immigration found in S3.\nAll sas7bdat files in HDFS will be processed:\n\t{immi_files_to_process}.\n")

    for f in immi_files_to_process:
        #us_immi_parq_folder = 'us_immi_' + f.split('i94_')[1][:5]
        us_immi_parq_filepath = f'{hdfs_raw_parquet_path}/us_immi'
        us_immi_cleanparq_filepath = f'{hdfs_output_path}/us_immi'

        #print(f"us_immi_parq_folder: {us_immi_parq_folder}")
        print(f"us_immi_parq_filepath: {us_immi_parq_filepath}\n")
        print(f"us_immi_cleanparq_filepath: {us_immi_cleanparq_filepath}\n")

        transform_us_immi(spark, s3_bucket, f, us_immi_parq_filepath, us_immi_cleanparq_filepath)

    ##### Time dimension
    us_immi_trffolder = f'{hdfs_output_path}/us_immi'
    time_cleanparq_filepath = f'{hdfs_output_path}/time'

    transform_time(spark, us_immi_trffolder, time_cleanparq_filepath)

if __name__ == '__main__':
    main()
