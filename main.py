from src.shared.pipeline.pip_01_data_ingestion import DataIngestionPipeline
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