from src.utils.commons import read_yaml,create_directories
from src.entity.config_entity import DataIngestionConfig, DataValidationConfig
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


        

    #data ingestion configuration manager
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        '''
        Function to get configuration settings
        '''

        # read data ingestion configuration section from config.yaml
        config = self.config.DATA_INGESTION

        # create a new artifacts folder to store ingested data if doesn't exist already 
        create_directories([config.root_dir])

        # create and return dataingestion configuration object

        config_object = DataIngestionConfig(
            root_dir=config.root_dir,
            local_path= config.local_path,
            target_path=config.target_path,
            registered_name= config.registered_name
        )

        return config_object
    
    # data validation configuration manager
    def get_data_validation_config(self) -> DataValidationConfig:

        config = self.config.DATA_VALIDATION

        create_directories([config.root_dir])

        data_validation_config = DataValidationConfig(
                        status_file = config.status_file,
                        root_dir = config.root_dir
        )

        return data_validation_config