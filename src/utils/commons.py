import os
# Exception for handling Box value errors
import boto3.resources
import boto3.session
from box.exceptions import BoxValueError
import sys
import yaml
from src.logger import logging
import json
import joblib
import numpy as np
# Decorator for runtime type checking
from ensure import ensure_annotations
# Enhanced dictionary that allows for dot notation access
from box import ConfigBox
from pathlib import Path
from typing import Any,List
import zipfile
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from dotenv import load_dotenv
from src.logger import logging

load_dotenv(dotenv_path='.env',override=True)


class S3Client:
    def __init__(self):
        self.s3client = boto3.client(
            's3',
            endpoint_url = os.getenv('ENDPOINT_URL'),
            aws_access_key_id = os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key = os.getenv('MINIO_SECRET_KEY'),
            config = Config(signature_version='s3v4')
        )
        
    
    def create_bucket(self,bucketname):
        '''
        Create an S3 bucket 
        
        Args:
            - bucketname: Name of the bucket
            '''
        try:
            # create bucket
            self.s3client.create_bucket(Bucket='new')
            logging.info(f'Create a Minio S3 bucket with name: {bucketname}')
        except ClientError as e:
            logging.info(f'Could not create bucket {bucketname} because {e}')
            return False
        return True

    def upload_file(self,filename,bucket,object_name=None):
        ''''
        upload files into S3 bucket 
        
        Args:
            - flename: name of the file to upload
            - Object_name :  name of the s3 bucket, if None, filename is used
            - bucket : name of the bucket to upload file
            '''
        
        if object_name == None:
            object_name = os.path.basename(filename)

        try:
            self.s3client.upload_file(filename,
                                      bucket,
                                      object_name,
                                      )
            logging.info(f'Uploaded {filename} successfully as {object_name} into {bucket} S3 bucket')

        except ClientError as e:
            logging.info(f'could not upload file into bucket because : {e}')

    def download_file(self,bucket,object_name,filename):
        '''
        Function to download files from s3 bucket
        
        Args:
            - bucket : Name of the bucket
            - object_name: name of the object
            - filename : file name to save the object'''
        
        try:
            self.s3client.download_file(bucket,object_name,filename)
            logging.info(f'successfully downloaded file {object_name} from S3 bucket to {filename}')
        
        except ClientError as e:
            logging.info(f'Unable to download files from S3 because : {e}')

        




@ensure_annotations
def read_yaml(filepath:str) -> ConfigBox :

    '''
    Function to read YAML files and return its content as configbox


    Args:
        - filepath : file location of the YAML file

    Outputs:
        - Configbox : returns the content of the YAML file as configbox object
    '''
    try:
        with open(filepath,'r') as yaml_obj:
            # load the content from yaml file
            content = yaml.safe_load(yaml_obj)
            logging.info(f'Yaml file:  {filepath} loaded suscessfully')

    except BoxValueError:
        raise ValueError('YAML file is empty')
    
    except Exception as e : # catch any other error
        raise e
    

@ensure_annotations
def create_directories(path_to_directories: list,verbose=True) :
    '''
    Function to create directories from the the list provided

    Args:
        - path_to_directories : List pf filepath to create directories from

    '''
    for path in path_to_directories :
        os.makedirs(path,exist_ok=True)

        if verbose:
            logging.info(f'File directory create at : {path}')



@ensure_annotations
def save_object(filepath:str,obj):
    '''
    Function to save objects to a filepath

    Args:
        - filepath : path to save object
    '''

    # get  directory name
    directory_path = os.path.dirname(filepath)
    #create directory if it doesn't exust
    os.makedirs(directory_path,exist_ok=True)
    # save to file path using joblib
    with open(filepath,'wb') as file_obj:
        joblib.dump(obj,file_obj)

@ensure_annotations
def load_object(filepath):
    '''
    Function to load object from filepath
    
    Args :
        - filepath : path to load object from

    returns:
        - an
    '''
    with open(filepath,'rb') as file_obj:
        logging.info(f'File loaded suscessfully from {filepath}')
        return joblib.load(file_obj)
    

@ensure_annotations
def save_json(filepath: Path, data: dict):
    """
    Saves a dictionary to a JSON file.

    Args:
        path (Path): Path where the JSON file will be saved.
        data (dict): Dictionary to save as JSON.
    """
    with open(filepath, "w") as f:
        # Dump the dictionary to a JSON file
        json.dump(data, f, indent=4)
    logging.info(f"json file saved at: {filepath}")

@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """
    Loads a JSON file and returns its contents as a ConfigBox object.

    Args:
        path (Path): Path of the JSON file to load.

    Returns:
        ConfigBox: The contents of the JSON file as a ConfigBox object.
    """
    with open(path) as f:
        # Load the JSON file content
        content = json.load(f)
    logging.info(f"json file loaded successfully from: {path}")
    return ConfigBox(content)

@ensure_annotations
def save_bin(data: Any, path: Path):
    """
    Saves data to a binary file using joblib.

    Args:
        data (Any): Data to save.
        path (Path): Path where the binary file will be saved.
    """
    joblib.dump(value=data, filename=path)
    logging.info(f"binary file saved at: {path}")

@ensure_annotations
def load_bin(path: Path) -> Any:
    """
    Loads data from a binary file using joblib.

    Args:
        path (Path): Path of the binary file to load.

    Returns:
        Any: The loaded data.
    """
    data = joblib.load(path)
    logging.info(f"binary file loaded from: {path}")
    return data

@ensure_annotations
def get_size(path: Path) -> str:
    """
    Gets the size of the file at the given path in kilobytes.

    Args:
        path (Path): Path of the file.

    Returns:
        str: Size of the file in kilobytes, rounded to the nearest whole number.
    """
    size_in_kb = round(os.path.getsize(path) / 1024)
    return f"~ {size_in_kb} KB"



def unzip_files(zip_file: str, output_dir: str) -> List[str]:
    """
    Unzips a given zip file to a specified directory and returns a list of file paths.

    Args:
        zip_file (str): The path to the zip file.
        output_dir (str): The directory to unzip the file to.

    Returns:
        List[str]: A list of file paths of the extracted files.
    """
    extracted_files = []
    try:
        # Make sure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Open and extract the zip file
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        
        logging.info(f"Files extracted successfully to {output_dir}")
    except Exception as e:
        logging.info(f"Error during extraction: {e}")






