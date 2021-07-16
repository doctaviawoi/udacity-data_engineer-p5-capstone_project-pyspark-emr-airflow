from airflow import DAG
from operators.upload_local_to_s3 import UploadLocalToS3Operator

from helpers import LocalFilepaths

def upload_files_to_s3_subdag(
        parent_dag_name,
        child_dag_name,
        aws_credentials_id,
        s3_bucket,
        schedule_interval,
        default_args,
        *args, **kwargs):

        """
        This functions generates a DAG to be used as a subDAG that runs
        UploadLocalToS3Operator on data files and Python scripts to S3.

        Args:
            parent_dag_name (str): name specified for the parent DAG.
            child_dag_name (str): task name specified for the subdag.
            aws_credentials_id (str): AWS credentials to connect to S3, specified in Airflow Admin Connection.
            s3_bucket (str): name of S3 bucket to load the files to.
            schedule_interval (str): time interval to trigger subDAG in cron time string format.
            default_args (dict): set of arguments for task created. They can be overriden during operator initialisation.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            dag_subdag (airflow.models.DAG): DAG to use as a subdag.
        """


        dag_subdag = DAG(
                        f"{parent_dag_name}.{child_dag_name}",
                        schedule_interval=schedule_interval,
                        start_date=default_args["start_date"]
        )

        object_dict = {
            'helpers': ['helpers', LocalFilepaths.local_helpers, False, LocalFilepaths.s3_helpers_key, 'overwrite'],
            'pyspark_functions': ['pyspark_functions', LocalFilepaths.local_pyspark_functions, False, LocalFilepaths.s3_pyspark_functions_key, 'overwrite'],
            'operators': ['operators', LocalFilepaths.local_operators, False, LocalFilepaths.s3_operators_key, 'overwrite'],
            'i94dicts': ['I94_dictionaries', LocalFilepaths.local_i94dicts, False, LocalFilepaths.s3_i94dicts_key, 'overwrite'],
            'immi_raw_data': ['us_immigration_raw_data', LocalFilepaths.local_immi_raw_data, False, LocalFilepaths.s3_immi_raw_data_key, 'append'],
            'other_raw_data': ['other_raw_data', LocalFilepaths.local_other_raw_data, False, LocalFilepaths.s3_other_raw_data_key, 'overwrite'],
            'bootstrap': ['bootstrap_file', LocalFilepaths.local_bootstrap, False, LocalFilepaths.s3_bootstrap_key, 'overwrite'],
            'config_jars':['config_jars', LocalFilepaths.local_config_jars, False, LocalFilepaths.s3_config_jars_key, 'overwrite']
            }

        for obj, obj_list in object_dict.items():
            UploadLocalToS3Operator(
                    task_id=f"load_{obj_list[0]}_to_s3",
                    dag=dag_subdag,
                    aws_credentials_id=aws_credentials_id,
            		files_path= obj_list[1],
                    recursive = obj_list[2],
            		s3_bucket=s3_bucket,
            		s3_key=obj_list[3],
            		uploading_type=obj_list[4]
            )

        return dag_subdag
