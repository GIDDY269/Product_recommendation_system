from src.utils.commons import read_yaml,create_directories
from src.cloud_storage.S3_object_store import S3Client
from botocore.exceptions import ClientError
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






class DataIngestion:
    def __init__(self,config:DataIngestionConfig):

        self.config = config

    def initiate_data_ingestion(self):

        start_time = time.time()
        start_timestamp = datetime.now().strftime("%m_%d_%Y %H:%M:%S")

        try:
            # download file from landing s3 bucket

            s3_client = S3Client()

            s3_client.download_file(bucket=self.config.bucket_name,
                                    object_name=self.config.object_name,
                                    filename=self.config.filename)
            # unzip ingested data
            output_dir = os.path.join(self.config.root_dir,'Raw_ingested_data')
            unzip_files(self.config.filename,output_dir)

            total_files = len(glob(output_dir+'\*csv'))
            logging.info(f'Number of ingested files : {total_files}')


            # save metadata
            end_time = time.time()
            end_timestamp = datetime.now().strftime("%m_%d_%Y %H:%M:%S")
            duration = end_time - start_time
            metadata = {
                'start_time' : start_timestamp,
                'end_time' : end_timestamp,
                'duration' : duration,
                'Number of files loaded' : total_files,
                'data_source' : self.config.bucket_name,
                'output_path' : output_dir
            }
            metadata_path = os.path.join(self.config.root_dir,'metadat.json')
            pd.Series(metadata).to_json(metadata_path)
            logging.info(f'saved ingestion pipeline metadat into {metadata_path}')


            # monitoring metrics
            ingestion_speed = total_files / duration
            logging.info(f'Ingestion speed: {ingestion_speed} files/second')
            
        except ClientError as e:
            logging.info(f'error occured {e}')
            