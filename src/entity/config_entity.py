from dataclasses import dataclass
from pathlib import Path



@dataclass
class DataIngestionConfig:
    root_dir : Path
    local_path : Path
    target_path : str
    registered_name : str


@dataclass
class DataValidationConfig:
    root_dir : Path
    status_file : Path
