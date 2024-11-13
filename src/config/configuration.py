from src.utils.commons import read_yaml,create_directories
from src.entity.config_entity import DataIngestionConfig
from src.logger import logging
from src.constants import *





class ConfigurationManager:
    def __init__(self,config_filepath=CONFIG_FILE_PATH,
                 params_filepath=PARAMS_FILE_PATH,
                 schema_filepath=SCHEMA_FILE_PATH):
        '''
        Initiating the configuration manager
        '''
        # read YAML configuration files to initatiate configuration parameters
        self.config = read_yaml(str(config_filepath))
        self.params = read_yaml(str(params_filepath))
        self.schema = read_yaml(str(schema_filepath))

        logging.info(f'configuration: {self.config}')


        

    #configuration manager
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        '''
        Function to get configuration settings
        '''

        # read data ingestion configuration section from config.yaml
        config = self.config.DATA_INGESTION

        # create a new artifacts folder to store ingested data
        create_directories([config.root_dir])

        # create and return dataingestion configuration object

        config_object = DataIngestionConfig(
            root_dir=config.root_dir,
            bucket_name= config.bucket_name,
            filename=config.filename,
            object_name = config.object_name
        )

        return config_object