import boto3
import boto3.s3
import boto3.s3.inject
from botocore.exceptions import ClientError
from botocore.client import Config
from dotenv import load_dotenv
from src.logger import logging
import os
import time




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

    def upload_folder(self, folder_path, bucket, object_name_prefix=""):
        """
        Upload all files from a folder (and subfolders) to an S3 bucket.
        
        Args:
            folder_path: Path to the folder to upload
            bucket: Name of the S3 bucket
            object_name_prefix: Optional prefix for the S3 object names (folder structure)
        """
        # Walk through the folder and upload each file
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Construct the S3 key (preserve folder structure)
                relative_path = os.path.relpath(file_path, folder_path)
                s3_key = os.path.join(object_name_prefix, relative_path).replace("\\", "/")

                try:
                    # Upload the file
                    self.s3client.upload_file(file_path, bucket, s3_key)
                    logging.info(f"Uploaded {file_path} as {s3_key} into {bucket} S3 bucket")
                    

                except ClientError as e:
                    logging.error(f"Could not upload file {file_path} to S3 bucket because: {e}")


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

    def download_folder(self, bucket, folder_prefix, local_dir):
        """
        Download all files from a folder (prefix) in an S3 bucket to a local directory.

        Args:
            bucket: Name of the S3 bucket.
            folder_prefix: Prefix of the folder in the S3 bucket.
            local_dir: Local directory to save the downloaded files.
        """
        try:
            # List objects in the folder (prefix)
            objects = self.s3client.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)

            if 'Contents' not in objects:
                logging.warning(f"No files found in the folder {folder_prefix} in bucket {bucket}")
                return

            for obj in objects['Contents']:
                s3_key = obj['Key']
                relative_path = os.path.relpath(s3_key, folder_prefix)
                local_file_path = os.path.join(local_dir, relative_path)

                # Create local directories if needed
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # Download the file
                self.s3client.download_file(bucket, s3_key, local_file_path)
                logging.info(f"Downloaded {s3_key} to {local_file_path}")
                

        except ClientError as e:
            logging.error(f"Could not download folder {folder_prefix} from bucket {bucket} because: {e}")