from prefect import flow, task
import subprocess
from src.logger import logging



@task(retries=3, retry_delay_seconds=5)
def data_ingestion():
    try:
        flow_name="Data Ingestion"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\data_ingestion\data_ingestion.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure
    return flow_name

@task(retries=3, retry_delay_seconds=5)
def raw_data_storage(flow_name):
    try:
        flow_name="Raw Data Storage"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\rawdata\raw_data_Storage.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure
    
@task(retries=3, retry_delay_seconds=5)
def data_preparation(flow_name):
    try:
        flow_name="Data Preparation"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\datavalidation\data_validation.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure
    
@task(retries=3, retry_delay_seconds=5)
def data_transformation(flow_name):
    try:
        flow_name="Data Transformation"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\datatransformation\data_transformation.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure 
    
@task(retries=3, retry_delay_seconds=5)
def data_validation(flow_name):
    try:
        flow_name="Data Validation"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\datavalidation\data_validation.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure 

@task(retries=3, retry_delay_seconds=5)
def feature_store(flow_name):
    try:
        flow_name="Feature Store"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\featurestore\feature_store.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure 


@task(retries=3, retry_delay_seconds=5)
def model_trainer(flow_name):
    try:
        flow_name="Model Trainer"
        result = subprocess.run(["python", r"D:\Ml-Projects\Customer-Churn\src\components\modeltrainer\model_trainer.py"], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure 


@flow
def churn_prediction_pipeline():
    df = data_ingestion()
    df = raw_data_storage(df)
    df = data_preparation(df)
    df = data_transformation(df)
    df = data_validation(df)
    df = feature_store(df)
    df = model_trainer(df)
    logging.info(df)
    
    # You can add more tasks here, e.g., data transformation, model training, etc.

if __name__ == "__main__":
    churn_prediction_pipeline()
