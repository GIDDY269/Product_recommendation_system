from setuptools import find_packages,setup
from typing import List


HYPEN_DOT_E = '-e .'


def get_requirements(filepath:str) -> List[str] :

    '''
    functions to get the requirements
    '''

    requirements = []

    with open(filepath,'r') as fileobj:

        requirements = fileobj.readlines()
        requirements = [obj.replace('\n','') for obj in requirements]

        if HYPEN_DOT_E in requirements:
            requirements.remove(HYPEN_DOT_E)


__version__ = '0.0.1'
src_repo = 'src'

setup(
    name=src_repo,
    version=__version__,
    license='MIT',
    url='https://github.com/GIDDY269/Product_recommendation_system.git',
    download_url='https://github.com/GIDDY269/Product_recommendation_system',
    description='This is a product recommendation system with candidate generation and reranking',
    author='gideon',
    author_email='oviemunooboro@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)