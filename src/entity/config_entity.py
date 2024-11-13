from dataclasses import dataclass
from pathlib import Path



@dataclass
class DataIngestionConfig:
    root_dir: Path
    bucket_name : str
    filename: str
    object_name: str