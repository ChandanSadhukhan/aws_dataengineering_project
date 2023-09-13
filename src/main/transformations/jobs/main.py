import boto3
from src.main.utility import encrypt_decrypt
from resource.dev import config

aws_access_key = encrypt_decrypt. \
    decrypt(config.aws_access_key)

aws_secret_key = encrypt_decrypt. \
    decrypt(config.aws_secret_key)


class S3ClientProvider:

    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.s3_client = self.session.client('s3').list_buckets()

    def get_client(self):
        return self.s3_client

print(S3ClientProvider(aws_access_key,aws_secret_key).get_client())