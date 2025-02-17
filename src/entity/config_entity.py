from dataclasses import dataclass
from pathlib import Path



@dataclass
class DataIngestionConfig:
    root_dir : Path
    bucket : str
    local_path : Path
    target_path : str
    object_name_prefix : str


@dataclass
class DataValidationConfig:
    status_file : Path
    root_dir : Path
    source_folder : str
    data_directory : str
    data_source_name : str
    asset_name : str
    batch_definition_name : str
    expectation_suite_name : str
    validation_definition_name : str

@dataclass
class DataTransformationConfig:
    source_datapath : Path
    source_parquetpath :Path
    schema : dict
    featurestore_path : Path
    train_datapath : Path
    test_datapath : Path
    train_transformed_datapath : Path
    test_transformed_datapath : Path
    trans_pipeline_model_path : Path
