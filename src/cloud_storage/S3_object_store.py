import boto3
import boto3.s3
import boto3.s3.inject
from botocore.exceptions import ClientError
from botocore.client import Config
from dotenv import load_dotenv
from src.logger import logging
import os




load_dotenv(dotenv_path='.env',override=True)


class S3Client:
    def __init__(self):
        self.s3client = boto3.client(
            's3',
            endpoint_url = os.getenv('ENDPOINT_URL'),
            aws_access_key_id = os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key = os.getenv('MINIO_SECRET_KEY'),
            config = Config(signature_version='s3v4')
        )
        
    
    def create_bucket(self,bucketname):
        '''
        Create an S3 bucket 
        
        Args:
            - bucketname: Name of the bucket
            '''
        try:
            # create bucket
            self.s3client.create_bucket(Bucket=bucketname)
            logging.info(f'Create a Minio S3 bucket with name: {bucketname}')
        except ClientError as e:
            logging.info(f'Could not create bucket {bucketname} because {e}')
            return False
        return True

    def upload_file(self,filename,bucket,object_name=None):
        ''''
        upload files into S3 bucket 
        
        Args:
            - flename: name of the file to upload
            - Object_name :  name of the object to be store in s3 bucket, if None, filename is used
            - bucket : name of the bucket to upload file
            '''
        
        if object_name == None:
            object_name = os.path.basename(filename)

        try:
            self.s3client.upload_file(filename,
                                      bucket,
                                      object_name,
                                      )
            logging.info(f'Uploaded {filename} successfully as {object_name} into {bucket} S3 bucket')

            os.remove(filename)

        except ClientError as e:
            logging.info(f'could not upload file into bucket because : {e}')

    def download_file(self,bucket,object_name,filename):
        '''
        Function to download files from s3 bucket
        
        Args:
            - bucket : Name of the bucket
            - object_name: name of the object
            - filename : file name to save the object'''
        
        try:
            self.s3client.download_file(bucket,object_name,filename)
            logging.info(f'successfully downloaded file {object_name} from S3 bucket to {filename}')
        
        except ClientError as e:
            logging.info(f'Unable to download files from S3 because : {e}')