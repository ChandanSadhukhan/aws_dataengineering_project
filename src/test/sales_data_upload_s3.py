import os
from resource.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *

s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()

s3_directory = "sales_data/"
s3_bucket = "youtube-project-testing"
local_file_path = config.local_directory + "sales_partition_data"

def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
    except Exception as e:
        raise e


upload_to_s3(s3_directory, s3_bucket, local_file_path)