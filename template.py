import os 
from pathlib import Path




candidate_generation = 'CandidateGeneration'
Reranker = 'Reranker'

list_of_files = [
    Path('.github') / 'workflows'/ '.gitkeep',
    f'src/__init__.py',
    f'src/{candidate_generation}/__init__.py',
    f'src/{candidate_generation}/Components/c_01_DataIngestion.py',
    f'src/{candidate_generation}/Components/c_02_DataValidation.py',
    f'src/{candidate_generation}/Components/c_03_DataTransformation.py',
    f'src/{candidate_generation}/Components/c_04_ModelTrainer.py',
    f'src/{candidate_generation}/Components/c_05_ModelEvaluation.py',
    f'src/{candidate_generation}/utils/__init__.py',
    f'src/{candidate_generation}/utils/commons.py',
    f'src/{candidate_generation}/config/__init__.py',
    f'src/{candidate_generation}/config/configuration.py',
    f'src/{candidate_generation}/pipelines/__init__.py',
    f'src/{candidate_generation}/pipelines/pip_01_DataIngestion.py',
    f'src/{candidate_generation}/pipelines/pip_02_DataValidation.py',
    f'src/{candidate_generation}/pipelines/pip_03_DataTransformation.py',
    f'src/{candidate_generation}/pipelines/pip_04_ModelTrainer.py',
    f'src/{candidate_generation}/pipelines/pip_05_ModelEvaluation.py',
    f'src/{candidate_generation}/pipelines/pip_06_ModelValidationPipeline.py',
    f'src/{candidate_generation}/pipelines/pip_07_PredictionPipeline.py',
    f'src/{candidate_generation}/entity/__init__.py',
    f'src/{candidate_generation}/entity/config_entity.py',
    f'src/{candidate_generation}/constants/__init__.py',
    f'src/{candidate_generation}/exception.py',
    f'src/{candidate_generation}/logger.py',
    f'src/{Reranker}/__init__.py',
    f'src/{Reranker}/Components/c_01_DataIngestion.py',
    f'src/{Reranker}/Components/c_02_DataValidation.py',
    f'src/{Reranker}/Components/c_03_DataTransformation.py',
    f'src/{Reranker}/Components/c_04_ModelTrainer.py',
    f'src/{Reranker}/Components/c_05_ModelEvaluation.py',
    f'src/{Reranker}/utils/__init__.py',
    f'src/{Reranker}/utils/commons.py',
    f'src/{Reranker}/config/__init__.py',
    f'src/{Reranker}/config/configuration.py',
    f'src/{Reranker}/pipelines/__init__.py',
    f'src/{Reranker}/pipelines/pip_01_DataIngestion.py',
    f'src/{Reranker}/pipelines/pip_02_DataValidation.py',
    f'src/{Reranker}/pipelines/pip_03_DataTransformation.py',
    f'src/{Reranker}/pipelines/pip_04_ModelTrainer.py',
    f'src/{Reranker}/pipelines/pip_05_ModelEvaluation.py',
    f'src/{Reranker}/pipelines/pip_06_ModelValidationPipeline.py',
    f'src/{Reranker}/pipelines/pip_07_PredictionPipeline.py',
    f'src/{Reranker}/entity/__init__.py',
    f'src/{Reranker}/entity/config_entity.py',
    f'src/{Reranker}/constants/__init__.py',
    f'src/{Reranker}/exception.py',
    f'src/{Reranker}/logger.py',
    'config/config.yaml',
    'metrics_threshold.yaml',
    'params.yaml',
    'schema.yaml',
    'main.py',
    'app.py',
    'setup.py',
    'streamlit_app.py',
    'Dockerfile',
    'requirements.txt',
    'requirements_dev.txt',
    'notebooks/trial_01_DataIngestion.ipynb',
    'notebooks/trial_02_DataValidation.ipynb',
    'notebooks/trial_03_DataTransformation.ipynb',
    'notebooks/trial_04_ModelTrainer.ipynb',
    'notebooks/trial_05_ModelEvaluation.ipynb',
    'notebooks/trial_06_ModelValidation.ipynb',
]


for filepath in list_of_files:
    filepath = Path(filepath)

    filedir, filename  = os.path.split(filepath) #split filepath by directory and filename

    if filedir != '':
        os.makedirs(filedir,exist_ok=True) #create file directory

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0) :

        with open(filepath,'w') as f :
            pass 



