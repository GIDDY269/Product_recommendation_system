from src.utils.commons import read_yaml,create_directories
from src.cloud_storage.S3_object_store import S3Client
import os
from datetime import datetime
from src.logger import logging
import pandas  as pd
from src.constants import *
import time
from datetime import datetime
from src.utils.commons import unzip_files
from glob import glob
from src.entity.config_entity import DataIngestionConfig
from botocore.exceptions import ClientError
import shutil






class DataIngestion:
    def __init__(self,config:DataIngestionConfig):

        self.config = config

    def initiate_data_ingestion(self):

        start_time = time.time()
        start_timestamp = datetime.now().strftime("%m_%d_%Y %H:%M:%S")

        try:

            logging.info(f'Unziping file from {self.config.local_path} into {self.config.root_dir} ')
            unzip_folder = os.path.join(self.config.root_dir,'unzipped_data') # folder to extract data to

            if not os.path.exists(unzip_folder) or not os.listdir(unzip_folder) :
                unzip_files(self.config.local_path,unzip_folder)
                logging.info(f'Data from {self.config.local_path} unzippeded into {unzip_folder}')
            else:
                logging.info(f'data from {self.config.local_path} already unzipped')
                pass


            s3 = S3Client()
            logging.info('Creating s3 bucket')
            s3.create_bucket(bucketname='cosmetic-store-1')
            logging.info('uploading files into s3 bucket')
            s3.upload_folder(folder_path=unzip_folder,bucket=self.config.bucket,object_name_prefix=self.config.object_name_prefix)
            logging.info('downloadin to local machine')
            s3.download_folder(bucket=self.config.bucket,folder_prefix=self.config.object_name_prefix,local_dir=self.config.target_path)

            # move to artifacts folder
            shutil.move(src=self.config.target_path,dst=self.config.root_dir)

        


            number_of_files = len(os.listdir(unzip_folder))
            logging.info(f'Number of ingested files : {number_of_files}')


            # save metadata
            end_time = time.time()
            end_timestamp = datetime.now().strftime("%m_%d_%Y %H:%M:%S")
            duration = end_time - start_time
            metadata = {
                'start_time' : start_timestamp,
                'end_time' : end_timestamp,
                'duration' : duration,
                'Number of files loaded to workspace' : number_of_files,
                'data_source' : self.config.local_path,
                'target_path' : self.config.target_path,
                'Project name' : 'Data ingestion'
            }

        
            metadata_path = os.path.join(self.config.root_dir,'Data_ingestion_metadata.json')
            pd.Series(metadata).to_json(metadata_path)
    
            logging.info(f'saved ingestion pipeline metadata into {metadata_path}')
            
        except ClientError as e:             
            logging.info(f'error occured {e}')