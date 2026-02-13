import os
import pandas as pd
from dataclasses import dataclass
from src.logger import logging
from src.exception import CustomException
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from src.components.datatransformation.data_transformation import DataTransformation
from src.components.data_validation.data_preparation import DataPreparation

import joblib
from prefect import flow
import logging


logging.basicConfig(
    filename="prep.log",
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

log = logging.getLogger()
log.setLevel(logging.INFO)  # Set the logging level to INFO



@dataclass
class modelBuildingConfig:
    raw_data_path: str = os.path.join('artifacts', "data.csv")



class modelBuilding:
    def __init__(self):
        self.ingestion_config = modelBuildingConfig()

    @staticmethod
    def evaluate_model(model, X_train, X_test, y_train, y_test):
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        return accuracy, precision, recall, f1

if __name__ == "__main__":
    # Data Preparation
    prep_obj = DataPreparation()
    cleaned_data = prep_obj.call_prep() 

    # Data Transformation
    trans_obj = DataTransformation()
    trans = trans_obj.transformedData(cleaned_data)

    if trans is not None:
        log.info("Data ingestion successful! Here are the first few rows:")
        log.info(trans.head())  # logging.info sample data
        log.info(trans.shape)
        log.info("Success")
    else:
        log.info("Data ingestion failed.")

    # Model Training
    df = cleaned_data
    df['churn'] = df['churn'].map({'True': 1, 'False': 0}).astype(int)
    X = df.drop(['customerid', 'churn'], axis=1)  
    y = df['churn']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Models
    models = {
        "Logistic Regression": LogisticRegression(),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "XGBoost": XGBClassifier(use_label_encoder=False, eval_metric='logloss')
    }

    # Model Evaluation
    results = {}
    for name, model in models.items():
        acc, prec, rec, f1 = modelBuilding.evaluate_model(model, X_train, X_test, y_train, y_test)
        results[name] = {
            "Accuracy": round(acc, 4),
            "Precision": round(prec, 4),
            "Recall": round(rec, 4),
            "F1 Score": round(f1, 4)
        }

    # Display Results
    results_df = pd.DataFrame(results).T
    logging.info(results_df)
    print(results_df)




    # Save the best model
    joblib.dump(model, "customer_churn_model.pkl")
