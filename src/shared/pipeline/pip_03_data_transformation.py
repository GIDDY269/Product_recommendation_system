import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.configuration import ConfigurationManager
from src.shared.components.DataTransformation import DataTransformation
from src.logger import logging
import json
from src.utils.commons import spark_session


PIPELINE_NAME = 'SHARED DATA TRANSFORMATION PIPELINE'


class DatavalidationPipeline:

    def __init__(self):
        pass

    def run(self):
        try:

            logging.info('Getting data transformation configuration')
            config_manager = ConfigurationManager()
            config = config_manager.get_data_transformation_config()
            data_transform = DataTransformation(config)
            # load data
            data_path = data_transform.load_data()
            # split into train, val and test
            train_path,val_path,test_path = data_transform.train_val_test_split(data_path)
            # perform feature engineering
            data_transform.feature_engineering(train_path)
            data_transform.feature_engineering(val_path)
            data_transform.feature_engineering(test_path)
            # perform feature transformation
            data_transform.feature_transformation(train_path=train_path,val_path=val_path,
                                                  test_path=test_path)
            logging.info('Data transformation completed sucessfully')

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
        logging.error(f'Data Transformation pipeline failed: {e}')
        raise e