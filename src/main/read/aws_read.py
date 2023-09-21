import logging
import traceback
from resource.dev import config
from src.main.utility import logging_config, encrypt_decrypt, s3_client_object

class S3Reader:

    def list_files(s3_client=None, bucket_name=None):
        try:
            csvfiles = []
            # get response
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            logging.info(response)

            # Iterate through the objects and print their keys (filenames)
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                  csvfiles.append(obj['Key'])

            logging_config.logger.info(f"Got some CSV file:'{csvfiles}'")
            return csvfiles

        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logging_config.logger.error("Got this error : %s", error_message)
            print(traceback_message)
            raise


######---- Test wheatedr there is any file in S3 bucket -------#####

# aws_access_key = encrypt_decrypt. \
#     decrypt(config.aws_access_key)
#
# aws_secret_key = encrypt_decrypt. \
#     decrypt(config.aws_secret_key)
# #
# s3_client = s3_client_object.S3ClientProvider(aws_access_key, aws_secret_key).get_client()
# bucket_name = config.bucket_name
# sales_data_folder_path = config.s3_sales_datamart_directory
#
# print(S3Reader.list_files(s3_client, bucket_name))
