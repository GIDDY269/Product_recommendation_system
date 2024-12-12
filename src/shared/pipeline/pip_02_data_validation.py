import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.configuration import ConfigurationManager
from src.shared.components.DataValidation import DataValidation
from src.logger import logging
from glob import glob
import json
from src.utils.commons import spark_session


PIPELINE_NAME = 'SHARED DATA VALIDATION PIPELINE'


class DatavalidationPipeline:

    def __init__(self):
        pass

    def run(self):
        try:

            config = ConfigurationManager()
            logging.info('Getting data validation configuration')
            data_validation_config = config.get_data_validation_config()
            data_validation = DataValidation(config=data_validation_config)
            validate = data_validation.validate()
            logging.info('Data validation completed sucessfully')

        except Exception as e:
            logging.error(f' data validation failed {e}')
            raise e
                   
if __name__ == '__main__':
    try:
        logging.info(f'##================================== Starting {PIPELINE_NAME} pipeline ========================================== ##')
        data_validation_pipeline = DatavalidationPipeline()
        data_validation_pipeline.run()
        logging.info(f'## ====================================={PIPELINE_NAME} Terminated sucessfully =============================== ##')
    except Exception as e :
        logging.error(f'Data ingestion pipeline failed: {e}')
        raise e