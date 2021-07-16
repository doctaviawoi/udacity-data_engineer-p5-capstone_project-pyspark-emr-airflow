from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.UploadLocalToS3Operator,
        operators.ZipOperator,
        operators.DataQualityS3Operator
    ]
    helpers = [
        helpers.get_s3_keys_list,
        helpers.get_filesPath,
        helpers.LocalFilepaths,
        helpers.create_spark_session
    ]
