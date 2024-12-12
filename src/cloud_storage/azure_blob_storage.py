from azureml.core import Workspace , Datastore ,Dataset
from src.logger import logging
import os
from src.utils.commons import unzip_files
from glob import glob


class AzureDatastore:

    '''
    This class is used for handling azure machine learning workspace data, like interaction with the default storage container for the workspace without connection strings
    Datastore  is not a storage account but serves as a link to the blob storage.
    '''
    def __init__(self):
        try:
            logging.info('configuring workspace')
            self.workspace = Workspace.from_config()
            self.datastore = self.workspace.get_default_datastore()
        except Exception as e:
            logging.info(f'Error coccure while setting up workspace : {e}')




    def load_local_data_to_Azure_datastore(self,src_dir,target_path,registered_name) :

        '''
        This function is to load the data from local machine to azure datastore
        
        Args:
            - src_dir : path to the data in local machine
            - target_path :  distination of the file in the workspace blob storage
            - name : how to register the data in workspace
            '''
        try :

            logging.info('Uploading data from local machine into blob storage using datastore')
            self.datastore.upload(
                src_dir= src_dir,
                target_path=target_path,
                overwrite=True
            )

            logging.info('Files uploaded into blob store sucessesfully')

            # registering the the dataset into workspace

            logging.info('Registering dataset into workspace')
            # create file dataset from all csv files in the directory
            file_dataset_path = os.path.join(target_path, '*.csv')
            
            logging.info(f'File path for dataset: {file_dataset_path}')

            dataset = Dataset.File.from_files(path=(self.datastore,file_dataset_path))
            logging.info(f'Registering dataset with name: {registered_name}')
            dataset.register(workspace=self.workspace,name=registered_name)

            # Check if the dataset exists in the workspace
            datasets = self.workspace.datasets
            if registered_name in datasets:
                logging.info(f'Dataset {registered_name} is registered successfully.')
            else:
                logging.error(f'Dataset {registered_name} is not registered.')


        except Exception as e:
            logging.info(f'Error occured : could not upload data to blob storage: {e}')

    
    def download_from_datastore(self,registered_name,target_path):
        '''
        Streams datafrom the default blobe storage using the datastore
        
        Args : 
            - registered_name : name of how the dataset wat registed in the workspace
            
        outs:
            list of all the filepath of each data streams'''
        
        if registered_name in self.workspace.datasets:

            dataset = Dataset.get_by_name(workspace=self.workspace,name=registered_name)

            dataset.download(
                target_path=target_path,
                overwrite=True
            )
        

        


