from setuptools import find_packages,setup
from typing import List

hypen_e_dot='-e.'

def get_requirements(file_path:str)-> list[str]:
    requirements=[]
    with open(file_path) as file_obj:
        requirements=file_obj.readlines()
        requirements=[req.replace("\n","") for req in requirements]
        
        if hypen_e_dot in requirements:
            requirements.remove(hypen_e_dot)

setup(name="CustomerChurn",
    version='1.0.3',
    author='Shahid Hussain M',
    author_email='hussainibe22@gmail.com',
    packages=find_packages(),
    install__requires=get_requirements('requirements.txt'))
