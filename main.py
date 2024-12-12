from src.shared.pipeline.pip_01_data_ingestion import DataIngestionPipeline
from src.shared.pipeline.pip_02_data_validation import DatavalidationPipeline
from src.logger import logging

component_name = 'DATA INGESTION COMPONENTS'

try:
    logging.info(f'##================================== Starting {component_name} pipeline ========================================== ##')
    data_ingestion_pipeline = DataIngestionPipeline()
    data_ingestion_pipeline.run()
    logging.info(f'## ====================================={component_name} Terminated sucessfully =============================== ##')
except Exception as e :
    logging.error(f'Data ingestion pipeline failed: {e}')
    raise e

component_name = 'DATA VALIDATION COMPONENTS'

try:
        logging.info(f'##================================== Starting {component_name} pipeline ========================================== ##')
        data_validation_pipeline = DatavalidationPipeline()
        data_validation_pipeline.run()
        logging.info(f'## ====================================={component_name} Terminated sucessfully =============================== ##')
except Exception as e :
        logging.error(f'Data validation pipeline failed: {e}')
        raise e