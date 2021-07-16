import os

class LocalFilepaths:
    """
    A class used to represent source folder directory in a local machine and target S3 key in a S3 bucket to move files to.
    """
    #filepath relative to directory of g/airflow_home/dags/udacity_dend_p5capstone_project
    local_helpers = '../../plugins/helpers'.replace(os.sep, '/')
    s3_helpers_key = 'helpers'
    local_operators = '../../plugins/operators'.replace(os.sep, '/')
    s3_operators_key = 'operators'
    local_pyspark_functions = '../../pyspark_functions'.replace(os.sep, '/')
    s3_pyspark_functions_key = 'pyspark_functions'

    local_i94dicts = '../../data/2_udacityDEND_p5capstone_project/rawData/I94_dicts'.replace(os.sep, '/')
    s3_i94dicts_key = 'data/rawdata/i94dicts'
    local_immi_raw_data = '../../data/2_udacityDEND_p5capstone_project/rawData/raw_18-83510-I94-Data-2016'.replace(os.sep, '/')
    s3_immi_raw_data_key = 'data/rawdata/immi'
    local_other_raw_data = '../../data/2_udacityDEND_p5capstone_project/rawData'.replace(os.sep, '/')
    s3_other_raw_data_key = 'data/rawdata'

    local_bootstrap = './configs/bootstrap'
    s3_bootstrap_key = 'emr_bootstrap'
    local_config_jars = '../../config_jars'.replace(os.sep, '/')
    s3_config_jars_key = 'config_jars'
