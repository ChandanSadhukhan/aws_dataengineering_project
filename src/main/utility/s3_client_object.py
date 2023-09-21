import logging
from src.main.utility.logging_config import *
import boto3
from src.main.utility import encrypt_decrypt
from resource.dev import config

class S3ClientProvider:

    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        logger.info("got the decripted assess key")
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.s3_client = self.session.client('s3')
        logger.info("Logged in to Amazon S3 directory")

    def get_client(self):
        return self.s3_client



######## ---- To check the connection with S3 -------##########

# aws_access_key = encrypt_decrypt. \
#     decrypt(config.aws_access_key)
#
# aws_secret_key = encrypt_decrypt. \
#     decrypt(config.aws_secret_key)
# s3= S3ClientProvider(aws_access_key, aws_secret_key).get_client()


