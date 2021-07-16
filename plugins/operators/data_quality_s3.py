from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import glob
import configparser
from pyspark.sql import SQLContext
from helpers import get_s3_keys_list
from helpers import get_filesPath
from helpers import create_spark_session

class DataQualityS3Operator(BaseOperator):
	"""
	This operator performs checks against transformed dataframes in parquet files in S3,
	whether the dataframe contains any rows and null values in specific columns.
	"""

	ui_color = '#89DA59'

	row_count_table = """
        SELECT COUNT(*) AS COUNT
        FROM {table}
    """

	null_count_table = """
        SELECT COUNT(*) AS COUNT
        FROM {table}
        WHERE {table_pkey} IS NULL
    """

	template_fields = ("s3_key",)

	@apply_defaults
	def __init__(self,
				 s3_bucket="",
				 s3_key="",
                 access_source="local",
				 *args, **kwargs):

		"""Initialises the parameters of DataQualityS3Operator

		Args:
			s3_bucket (str): name of the S3 bucket where the parquet files are stored.
			s3_key (str): name of the S3 key where the parquet files are stored.
			access_source (str): accessing s3 from, defaulted to "local". Other option
				of the value is 'ec2'.
			access_source (str): Argument to pass on to `get_s3_keys_list` function.
								 It indicates where the S3 bucket is accessed from,
								 defaulted to 'local'. When "local" is passed as an argument,
								 which indicates accessing S3 from a local machine,
								 AWS credentials will be extracted from `dl.cfg` file
								 in `configs` folder. Other option is 'ec2'.
			*args: Variable length argument list.
			***kwargs: Arbitrary keyword arguments.
		"""
		super(DataQualityS3Operator, self).__init__(*args, **kwargs)
		self.s3_bucket = s3_bucket.lower()
		self.s3_key = s3_key
		self.access_source = access_source.lower()

	def execute(self, context):
		"""
		Start a Spark session, get object in S3 URI using `get_s3_keys_list` function,
		and count number of rows and null values in each S3 objects (transformed data set).
		"""
		# Initialise spark session
		spark = create_spark_session("Capstone Project - US Tourism and Climate Analysis (Airflow-EMR-PySpark)")

		# parse aws credentials in dl.cfg file for reading parquet file in S3
		# and doing row counts and null values check.
		config = configparser.ConfigParser()
		config.read(os.path.join('../../dags/udacity_dend_p5capstone_project', \
								'configs', 'dl.cfg').replace(os.sep, '/'))

		aws_profile = 'default'
		AWS_ACCESS_KEY_ID=config.get(aws_profile, 'AWS_ACCESS_KEY_ID')
		AWS_SECRET_ACCESS_KEY=config.get(aws_profile, 'AWS_SECRET_ACCESS_KEY')

		sc = spark.sparkContext
		sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
		sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

		sqlContext = SQLContext(sc)

		# Combine specified S3 bucket and keys to a full S3 URI.
		rendered_key = self.s3_key.format(**context)
		print(f"rendered_key: {rendered_key}")
		s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
		print(f"s3_path: {s3_path}")

        # create list or dictionary for each key (eg. us_immi, airport-codes, us-cities-demographics, us-state-boundaries)
		#s3_keys_to_check = ['iban_iso3166', 'GlobalLandTemperaturesByCity', 'i94prtl',
		#					'former_countries_iso3166_3', 'countries_codes_and_coordinates',
		#					'airport-codes', 'i94cntyl', 'us-port-codes', 'us-state-boundaries',
		#					'us-cities-demographics', 'us_immi', 'time']

		# Specify S3 keys that contains parquet files and each columns to check for null values in a dictionary format.
		tables_dict = {'us-state-boundaries': 'state_code',
                        'us-cities-demographics': ('city', 'state_code'),
                        'i94cntyl': 'i94_cntyl_code',
                        'i94prtl': 'i94_prtl_code',
                        'us_immi': ('cicid', 'year', 'month'),
						'GlobalLandTemperaturesByCity': ('date', 'city', 'country'),
						'time': ('date', 'year', 'month', 'day_of_month', 'day_of_week', 'week_of_year')}

		for table, table_pkey in tables_dict.items():

            #### Check for row counts

			s3_full_key = f"{rendered_key}/{table}"
			print(f"s3_full_key: {s3_full_key}")
			s3_path = f"s3a://{self.s3_bucket}/{s3_full_key}"
			print(f"s3_path: {s3_path}")

			self.log.info(f'DataQualityS3Operator is executing checks on S3 URI: {s3_path}.')

			self.log.info(f"DataQualityS3Operator is counting row numbers in {table}...")

			s3_keys, s3_files = get_s3_keys_list(self.s3_bucket, s3_full_key, self.access_source)

			if s3_keys == [] and s3_files != []:
				object_path = f'{s3_path}/*'

			elif s3_keys != [] and s3_files != []:
				slash_n = s3_files[0].count('/')
				object_path = os.path.join(s3_path,'*/'*(slash_n+1), '*').replace(os.sep, '/')

			elif s3_keys == [] and s3_files == []:
				raise ValueError(f"Test failed. Table {table} does not exist.\n\n")
		        #continue

			print(f"object_path: {object_path}")
			df = sqlContext.read.parquet(object_path)

			table = table.replace('-', '_')
			df.createOrReplaceTempView(table)

			row_counts = spark.sql(DataQualityS3Operator.row_count_table.format(table=table)).collect()[0]['COUNT']

			self.log.info(f"Row counts of {table} table: {row_counts}\n")

			if row_counts > 0:
				self.log.info(f"Test passed. Table {table} contains {row_counts} rows. \n")

			elif row_counts == 0:
				raise ValueError(f"Test failed. Table {table} is empty\n")


			#### Check for null values

			self.log.info(f"DataQualityS3Operator is counting null values in {table}...")

			null_counts = spark.sql(DataQualityS3Operator.null_count_table.format(table=table, table_pkey=table_pkey)).collect()[0]['COUNT']

			if null_counts == 0:
				self.log.info(f'Test passed. Primary key columns in {table} do not contain null values.')
			else:
				raise ValueError(f'Test failed. Primary keys in {table} contain {null_counts} null values.')

			self.log.info(f'Success! DataQualityS3Operator has completed checks on {table}.\n\n')
