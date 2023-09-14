import traceback
from resource.dev import config
from src.main.utility import logging_config, encrypt_decrypt, s3_client_object



class S3Reader:

    def list_files(s3_client=None, bucket_name=None, folder_path=None):
        try:
            delimiter = '/'

            # List folders in the bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter=delimiter)
            print(response)

            # Extract the common prefixes, which represent folders
            folders = [prefix.get('Prefix') for prefix in response.get('CommonPrefixes', [])]

            # Print the list of folders
            for folder in folders:
                print("Folder:", folder)

            if 'Contents' in response:
                logging_config.logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, bucket_name,
                            response)
                files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]
                return files
            else:
                return []
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
#
# s3_client = s3_client_object.S3ClientProvider(aws_access_key,aws_secret_key).get_client()
# bucket_name = config.bucket_name
# sales_data_folder_path = config.s3_sales_datamart_directory
#
# S3Reader.list_files(s3_client, bucket_name, sales_data_folder_path)


