from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import glob

from helpers import get_s3_keys_list, get_filesPath

class UploadLocalToS3Operator(BaseOperator):
	"""
	This operator gets key from S3 bucket and uploads files in a local directory to S3
	"""

	ui_color = '#c2daf6'

	template_fields = ("s3_key",)

	@apply_defaults
	def __init__(self,
				 aws_credentials_id="",
				 files_path ="",
				 recursive=False,
				 s3_bucket="",
				 s3_key="",
				 uploading_type='append',
				 *args, **kwargs):

		"""Initialises the parameters of UploadLocalToS3Operator

		Args:
			aws_credentials_id (str): AWS credentials details specified in Airflow Admin Connection.
			files_path (str): directory of files in local machine to be uploaded to S3.
			recursive (bool): Argument to pass onto `get_filesPath` function, whether
								to scan folder recursively, defaulted to False.
			s3_bucket (str): S3 bucket to store the files to.
			s3_key (str): object path in a specific S3 bucket.
			uploading_type (str): file uploading pattern, defaulted to 'append'. Other option
				of the value is 'overwrite'
			#replace(bool): a flag whether or not to overwrite file if it already exists.
			#	If False and key exists, an error will be raised.
			*args: Variable length argument list.
			***kwargs: Arbitrary keyword arguments.
		"""
		super(UploadLocalToS3Operator, self).__init__(*args, **kwargs)
		self.aws_credentials_id = aws_credentials_id
		self.files_path = files_path
		self.recursive = recursive
		self.s3_bucket = s3_bucket.lower()
		self.s3_key = s3_key
		self.uploading_type = uploading_type.lower()

	def execute(self, context):
		"""
		If uploading style is `overwrite`, upload files onto specified S3 URI,
		else if uploading style is `append`, get keys in specified bucket and key, and
		only upload file that is not already in target S3 URI.
		"""
		s3 = S3Hook(self.aws_credentials_id)

		print(f"upload_local_to_s3 - Current working directory is: {os.getcwd()}.")
		local_file_folder = self.files_path.split('*')[0]
		print(f"upload_local_to_s3 - local_file_folder = {local_file_folder}.")

		rendered_key = self.s3_key.format(**context)
		s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

		if self.uploading_type.lower() not in ("append", "overwrite"):
			self.log.error("Hello! Please specify uploading_type. Options are 'append' or 'overwrite'.")


		if self.uploading_type == 'overwrite':
			self.log.info(f"upload_local_to_s3 - UploadLocalToS3Operator is overwriting files from local machine {local_file_folder} to {s3_path}.")

			filesPath_list = list(get_filesPath(self.files_path, recursive=self.recursive).values())
			print(f"upload_local_to_s3 - filesPath_list : {filesPath_list}")

			for file in filesPath_list:
				print(f"Processing {file}...")
				fname = file.split('/')[-1]
				s3_full_uri = s3_path + '/' + fname
				s3_full_key = rendered_key + '/' + fname
				self.log.info(f"\tupload_local_to_s3 - Loading {fname} to {s3_full_uri}...")

				s3.load_file(filename=file,
							bucket_name=self.s3_bucket,
							key=s3_full_key,
							replace=True
							)

				self.log.info(f"\t{fname} uploaded to {s3_full_uri}.")
				self.log.info("Success.")


		elif self.uploading_type == 'append':
			# get list of files in source directory in local machine

			local_filesDict = get_filesPath(self.files_path, recursive=self.recursive)
			local_files = list(local_filesDict.keys())
			print(f"upload_local_to_s3 - local_files : {local_files}")

			# get list of files in destination path in S3
			_, s3_files = get_s3_keys_list(self.s3_bucket,
										rendered_key,
										access_source='local'
										)
			print(f"upload_local_to_s3 - s3_files: {s3_files}")

			# Find the files to upload from local to s3.
			files_to_upload = []
			if s3_files is not None:
				files_to_upload = list(set(local_files)-set(s3_files))
			else:
				files_to_upload = local_files

			print(f"upload_local_to_s3 - files_to_upload: {files_to_upload}")
			#except:
			#	print("Hello! Please check local files_path and s3 bucket details.")

			if files_to_upload == []:
				self.log.info(f"upload_local_to_s3 - All files in {self.files_path} already exist in {s3_path}.")
			else:
				self.log.info(f"upload_local_to_s3 - UploadLocalToS3Operator is appending files from local machine {self.files_path} to {s3_path}.")

				for file_to_upload in files_to_upload:
					self.log.info(f"\tupload_local_to_s3 - Loading {file_to_upload} to S3...")
					fname = local_filesDict[file_to_upload]
					s3_full_key = self.s3_key + '/' + file_to_upload

					self.log.info(f"\tupload_local_to_s3 - Loading {file_to_upload} into {s3_path}...")

					try:
						s3.load_file(filename=fname,
									 bucket_name=self.s3_bucket,
									 key=s3_full_key,
									 replace=False
									 )

						self.log.info(f"\tupload_local_to_s3 - {file_to_upload} uploaded into {s3_path}.")
						self.log.info("Success.")
					except ValueError as e:
						self.log.info(e)
						self.log.info("upload_local_to_s3 - Move on to loading the next file to S3...")
						pass
