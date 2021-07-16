# Import packages
### GENERAL
import os
from datetime import datetime, timedelta
import logging
import json
import glob
import boto3

### Airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from operators.zip_operator_plugin import ZipOperator
from operators.data_quality_s3 import DataQualityS3Operator


####### EMR
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.utils.dates import days_ago


### subdag scripts
from udacity_dend_p5capstone_project.subdags import upload_files_to_s3_subdag


# Configurations for files loading to S3 storage
### working directory on cmd wsl is g:/airflowhome/dags/udacity_dend_p5capstone_project
s3_bucket = Variable.get("s3_bucket")


# HDFS file path
#hdfs_output_path = '/home/hadoop/output_to_s3'

### Define set of default arguments for DAG
default_args = {
    'owner': 'doctavia',
    'email': ['doctaviawoi@gmail.com'],
    'start_date': datetime(2021,5,24,0,0,0,0),
    'depends_on_past': False,
    'retries': 1,
    'retries_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
    }

### Instantiate a DAG object
dag = DAG('Udacity_DEND-Capstone_Project',
        default_args=default_args,
        description='Load and transform data in EMR Spark with Airflow',
        schedule_interval='@monthly'
		)

### Begin execution
start_operator = DummyOperator(task_id='begin_data_pipeline', dag=dag)


# Zip helper_functions folder
folder_to_zip_path = "../../pyspark_functions/helper_functions"
zip_folder_path = "../../pyspark_functions/helper_functions.zip"

zip_task = ZipOperator(
            task_id='zip_helper_functions_folder',
            dag=dag,
            path_to_file_to_zip=folder_to_zip_path,
            path_to_save_zip=zip_folder_path
            )

# Upload raw data, helpers, operators, and dictionaries etc to s3 bucket
upload_localfiles_to_s3_task = SubDagOperator(
    task_id='load_localfiles_to_s3',
    subdag=upload_files_to_s3_subdag(
            parent_dag_name='Udacity_DEND-Capstone_Project',
            child_dag_name='load_localfiles_to_s3',
            aws_credentials_id="aws_credentials",
            s3_bucket=s3_bucket,
            schedule_interval='@monthly',
            default_args=default_args
            ),
            dag=dag,
)


# Spin up EMR cluster
job_flow_overrides_path = './configs/job_flow_overrides.json'
job_flow_overrides = open(job_flow_overrides_path, 'r')

create_EMR_cluster = EmrCreateJobFlowOperator(
                        task_id='create_emr_cluster',
                        dag=dag,
                        job_flow_overrides = json.load(job_flow_overrides),
                        aws_conn_id="aws_credentials",
                        emr_conn_id="emr_default",
						region_name='us-west-2'
)

# Add steps to the EMR cluster
spark_steps_path  = './configs/emr_steps.json'

with open(spark_steps_path, 'r') as f:
    spark_steps = json.load(f)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    dag=dag,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=spark_steps,
	params={
			"s3_bucket": s3_bucket,
			#"s3_raw_data": s3_raw_data,
			#"s3_operators_key": s3_operators_key,
			#"hdfs_output_path": hdfs_output_path
	})


last_step = len(spark_steps) - 1 # this value will let the sensor know the last step to watch
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    dag=dag,
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_credentials")


# Task - Perform row count and null value checks against parquet files in S3
run_quality_checks = DataQualityS3Operator(
    task_id='run_data_quality_checks',
    dag=dag,
    s3_bucket="udacity-dend-capstone-project",
    s3_key='data/processedData',
    access_source="local"
)

# Terminate the EMR cluster
terminate_EMR_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    dag=dag,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials")


### End execution
end_operator = DummyOperator(task_id='end_data_pipeline', dag=dag)

##### Order of task dependencies
start_operator >> zip_task
zip_task >> upload_localfiles_to_s3_task
upload_localfiles_to_s3_task >> create_EMR_cluster
create_EMR_cluster >> step_adder
step_adder >> step_checker >> run_quality_checks
run_quality_checks >> terminate_EMR_cluster
terminate_EMR_cluster >> end_operator
