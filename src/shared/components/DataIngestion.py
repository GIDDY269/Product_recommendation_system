from src.utils.commons import read_yaml,create_directories
from src.cloud_storage.azure_blob_storage import AzureDatastore
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

            logging.info(f'Unziping file from {self.config.local_path} into {self.config.root_dir} ')
            unzip_folder = os.path.join(self.config.root_dir,'ingested_data') # folder to extract data to

            if not os.path.exists(unzip_folder) or not os.listdir(unzip_folder) :
                unzip_files(self.config.local_path,unzip_folder)
                logging.info(f'Data from {self.config.local_path} unzippeded into {unzip_folder}')
            else:
                logging.info(f'data from {self.config.local_path} already unzipped')
                pass


            Azure_ws = AzureDatastore()

            Azure_ws.load_local_data_to_Azure_datastore(
                src_dir = unzip_folder,
                target_path = self.config.target_path,
                registered_name = self.config.registered_name
            )

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
                'target_path_from_datastore' : self.config.target_path,
                'registered_name' : self.config.registered_name,
                'Project name' : 'Data ingestion'
            }
            metadata_path = os.path.join(unzip_folder,'metadata.json')
            pd.Series(metadata).to_json(metadata_path)
    
            logging.info(f'saved ingestion pipeline metadata into {metadata_path}')
            
        except Exception as e:             
            logging.info(f'error occured {e}')
            