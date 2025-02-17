import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.configuration import ConfigurationManager
from src.shared.components.DataTransformation import DataTransformation
from src.logger import logging
import json
from src.utils.commons import spark_session
import pyspark.sql.functions as F
import pandas as pd
from src.cloud_storage.featurestore import Featurestore

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

            # perform feature engineering
           # data = data_transform.feature_engineering()
            # perform feature transformation
            spark = data_transform.spark
            data = spark.read.parquet('Artifacts\FeatureStore\\train_data')
            data_transform.feature_transformation(data)
            #logging.info('Data transformation completed sucessfully')

            
            #logging.info('uploading data into feature store')
            Fs = Featurestore()

            feature_refs = Fs.get_feature_references()

            
            entity_rows = [ {
            "user_id": '384702486',
            "product_id":   '5738998',
            "user_session": 'd9bcbaa1-15e7-41ec-a49d-73f43e9ac769',
            'category_id':'1487580013858390233'
                }]
            feature_dict = Fs.get_online_features(
                feature_refs=feature_refs,
                entity_rows=entity_rows)
            print(feature_dict)

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