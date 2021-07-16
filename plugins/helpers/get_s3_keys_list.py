import boto3
import configparser

def get_s3_keys_list(s3_bucket, prefix, access_source='ec2'):
    """
    This function returns list of S3 keys and list of S3 objects in an S3 URI provided.

    Args:
        s3_bucket (str): name of the S3 bucket to get lists of keys and objects from.
        prefix (str): name of S3 key to get lists of keys and objects from.
        access_source (str): Where the S3 bucket is accessed from, defaulted to 'ec2'.
                            Other option is "local", which indicates accessing S3 from
                            a local machine and AWS credentials will be extracted
                            from `dl.cfg` file in `configs` folder.

    Returns:
        keys (list): List of keys (folders) in S3 URI provided.
        files (list): List of files in S3 URI provided.
    """

    if access_source=='local':
        print("get_s3_keys_list - Accessing S3 bucket from local machine.")

        # Get aws access key and secret access key
        config = configparser.ConfigParser()
        config.read('./configs/dl.cfg')
        print("dl.cfg read")

        aws_profile = "default"
        AWS_ACCESS_KEY_ID = config[aws_profile]['AWS_ACCESS_KEY_ID']
        AWS_SECRET_ACCESS_KEY = config[aws_profile]['AWS_SECRET_ACCESS_KEY']
        print("AWS access keys details loaded.")

        session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                                )
        print("boto3.Session called.")

        s3 = session.client('s3')
        print("boto3.Session.resource called.")

        response = s3.list_objects_v2(Bucket=s3_bucket,
                                        Prefix = prefix
                                        )
        print("list_objects_v2 function called.")

        try:
            keys = []
            files = []
            for obj in response['Contents'][1:]:
                if obj["Key"][-1] == '/':
                    key = obj["Key"][:-1].replace(prefix, "")
                    if key[0] == '/':
                        keys.append(key[1:])
                    else:
                        keys.append(key)
                else:
                    file = obj['Key'].split('/')[-1]
                    files.append(file)

            keys = list(set(keys))
            files = list(set(files))

            print(f"get_s3_keys_list - 'local'- keys:\n {keys}\n")
            print(f"get_s3_keys_list - 'local'- files:\n {files}\n")
            return keys, files
        except:
            print(f"get_s3_keys_list - S3 key {prefix} does not exist.")
            print(f"get_s3_keys_list - keys:\n {keys}")
            return keys, files

    elif access_source=='ec2':
        print("get_s3_keys_list - Accessing S3 bucket from EC2 instances")
        s3 = boto3.client('s3')

        response = s3.list_objects_v2(Bucket=s3_bucket,
                                        Prefix = prefix
                                        )
        try:
            keys = []
            files = []
            for obj in response['Contents'][1:]:
                if obj["Key"][-1] == '/':
                    key = obj["Key"][:-1].replace(prefix, "")
                    if key[0] == '/':
                        keys.append(key[1:])
                    else:
                        keys.append(key)
                else:
                    file = obj['Key'].split('/')[-1]
                    files.append(file)

            keys = list(set(keys))
            files = list(set(files))

            print(f"get_s3_keys_list - 'EC2'- keys:\n {keys}\n")
            print(f"get_s3_keys_list - 'EC2'- files:\n {files}\n")
            return keys, files
        except:
            print(f"get_s3_keys_list - S3 key {prefix} does not exist.")
            print(f"get_s3_keys_list - keys:\n {keys}")
            return keys, files
