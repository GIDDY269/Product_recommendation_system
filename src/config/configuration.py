from src.utils.commons import read_yaml,create_directories
from src.entity.config_entity import DataIngestionConfig, DataValidationConfig,DataTransformationConfig
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
            bucket = config.bucket,
            object_name_prefix= config.object_name_prefix
           
        )

        return config_object
    
    # data validation configuration manager
    def get_data_validation_config(self):

        config = self.config.DATA_VALIDATION

        create_directories([config.root_dir])

        data_validation_config = DataValidationConfig(
                        status_file = config.status_file,
                        root_dir= config.root_dir,
                        source_folder = config.source_folder,
                        data_directory = config.data_directory,
                        data_source_name = config.data_source_name,
                        asset_name = config.asset_name,
                        batch_definition_name = config.batch_definition_name,
                        expectation_suite_name = config.expectation_suite_name,
                        validation_definition_name = config.validation_definition_name
        )

        return data_validation_config
    

    def get_data_transformation_config(self) -> DataTransformationConfig:

        config = self.config.DATA_TRANSFORMATION
        schema = self.schema['schema']

        config_object = DataTransformationConfig(
            source_datapath=config.source_datapath,
            source_parquetpath = config.source_parquetpath,
            schema=schema,
            featurestore_path = config.feature_store_path,
            train_datapath = config.train_datapath,
            val_datapath = config.val_datapath,
            test_datapath = config.test_datapath,
            train_transformed_datapath = config.train_transformed_datapath,
            val_transformed_datapath = config.val_transformed_datapath,
            test_transformed_datapath = config.test_transformed_datapath
        )

        return config_object