from src.config.configuration import ConfigurationManager
from src.shared.components.DataIngestion import DataIngestion
from src.logger import logging


PIPELINE_NAME = 'SHARED DATA INGESTION PIPELINE'


class DataIngestionPipeline:

    def __init__(self):
        pass

    def run(self):
        try:
            config = ConfigurationManager()
            data_ingestion_config = config.get_data_ingestion_config()
            data_ingestion = DataIngestion(config=data_ingestion_config)
            data_ingestion.initiate_data_ingestion()
            logging.info('DATA INGESTION FROM S3 BUCKET COMPLETED')
        except Exception as e :
            logging.error(f'Data ingestion process failed: {e}')
            raise

if __name__ == '__main__':
    try:
        logging.info(f'##================================== Starting {PIPELINE_NAME} pipeline ========================================== ##')
        data_ingestion_pipeline = DataIngestionPipeline()
        data_ingestion_pipeline.run()
        logging.info(f'## ====================================={PIPELINE_NAME} Terminated sucessfully =============================== ##')
    except Exception as e :
        logging.error(f'Data ingestion pipeline failed: {e}')
        raise e