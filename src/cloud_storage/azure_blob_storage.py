from azureml.core import Workspace , Datastore ,Dataset
from src.logger import logging
import os
from src.utils.commons import unzip_files


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


    def load_local_data_to_Azure_datastore(self,local_path,target_path,registered_name) :

        '''
        This function is to load the data from local machine to azure datastore
        
        Args:
            - local_path : path to the data in local machine
            - target_path :  distination of the file in the workspace
            - name : how to register the data in workspace
            '''
        try :

            logging.info('Uploading data from local machine into blob storage using datastore')
            self.datastore.upload_files(
                files=[local_path],
                target_path=target_path,
                overwrite=True
            )

            # registering the the dataset into workspace

            logging.info('Registering dataset into workspace')
            dataset = Dataset.File.from_files(path=(self.datastore,local_path))
            dataset.register(Workspace=Workspace,name=registered_name)

        except Exception as e:
            logging.info('Error occured : could not upload data to blob storage')

    
    def stream_from_datastore(self,registered_name):
        '''
        Streams datafrom the default blobe storage using the datastore
        
        Args : 
            - name : name of how the dataset wat registed in the workspace
            
        outs:
            pandas dataframe'''

        dataset = Dataset.get_by_name(Workspace,name=registered_name)
        # unzip the zip folder

        unzip_files(dataset,)

        return dataset
        


