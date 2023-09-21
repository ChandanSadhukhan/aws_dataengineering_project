import boto3
import traceback
import os
from resource.dev import config
from src.main.read.aws_read import *
from src.main.utility import encrypt_decrypt, s3_client_object
from src.main.utility.logging_config import *


class S3FileDownloader:
    def __init__(self, s3_client, bucket_name, local_directory):
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, list_files):
        logger.info("Running download files for these files %s", list_files)
        for key in list_files:
            file_name = os.path.basename(key)
            logger.info(f"File name %s '{file_name}'")
            download_file_path = os.path.join(self.local_directory, file_name)
            try:
                self.s3_client.download_file(self.bucket_name, key, download_file_path)
                logger.info()
            except Exception as e:
                logger.error(f"Error downloading file '{key}': {str(e)}")
                logger.info(traceback.format_exc())
                raise e


######---- Test wheatedr there is any file in S3 bucket -------#####

# aws_access_key = encrypt_decrypt. \
#     decrypt(config.aws_access_key)
#
# aws_secret_key = encrypt_decrypt. \
#     decrypt(config.aws_secret_key)
#
# s3_client = s3_client_object.S3ClientProvider(aws_access_key, aws_secret_key).get_client()
# bucket_name = config.bucket_name
# local_directory = config.sales_team_data_mart_partitioned_local_file
# list_of_csv_files = S3Reader.list_files(s3_client, bucket_name)
# print(list_of_csv_files)
# #
# S3FileDownloader(s3_client, bucket_name, local_directory).download_files(list_of_csv_files)
