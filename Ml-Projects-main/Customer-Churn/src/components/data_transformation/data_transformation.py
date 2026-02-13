from dataclasses import dataclass
import numpy as np 
import pandas as pd
from src.components.data_validation.data_preparation import DataPreparation
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate
import os
from google.cloud import secretmanager
import json
import snowflake.connector
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
class DataTransformationConfig:
    raw_data_path: str = os.path.join('artifacts', "data.csv")



class DataTransformation:
    def __init__(self):
        self.config = DataTransformationConfig()

    def transformedData(self,df):
        # Feature Engineering
        df["Avg Spend Per Tenure"] = df["total spend"] / df["tenure"].replace(0, 1)  # Avoid division by zero

        # Convert 'Last Interaction' to a relative recency score (higher means more recent)
        df["Recency Score"] = df["last interaction"].max() - df["last interaction"]

        #Churn Risk Score (Higher risk if more support calls, delays, and lower tenure)
        df['Churn Risk Score'] = (df['support calls'] * 0.3) + (df['payment delay'] * 0.5) - (df['tenure'] * 0.2)

        #Customer Lifetime Value (CLV) Approximation
        df['CLV'] = df['total spend'] / df['tenure']
        output_csv_path = r"D:\Ml-Projects\Customer-Churn\notebook\data\customer_data_transformed.csv"
        df.to_csv(output_csv_path, index=False)
        return df

    def datastore(self,df):
        
        client=secretmanager.SecretManagerServiceClient()
        secret_id="SnowflakeConnection"
        project_id="Personal"
        request={"name":f"projects/597348541614/secrets/SnowflakeConnection/versions/3"}
        response=client.access_secret_version(request)
        secret_string=response.payload.data
        sec=json.loads(secret_string)
        conn = snowflake.connector.connect(
                user='SHAHHUSSAIN',
                password='Shahid22Sherin',
                account='TIQIOCR-KY36542',
                database='PROJECT',
                schema='PUBLIC',
                session_parameters={
                    'QUERY_TAG': 'EndOfMonthFinancials',
                }
)

        cursor = conn.cursor()

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS CustomerChurnData (
            CustomerID            INT,
            Age                   FLOAT,
            Gender                INT,
            Tenure                FLOAT,
            Usage_Frequency       FLOAT,
            Support_Calls         FLOAT,
            Payment_Delay         FLOAT,
            Subscription_Type     INT,
            Contract_Length       INT,
            Total_Spend           FLOAT,
            Last_Interaction      FLOAT,
            Churn                 STRING,
            Avg_Spend_Per_Tenure  FLOAT,
            Recency_Score         FLOAT,
            Churn_Risk_Score      FLOAT,
            CLV                   FLOAT
        );
        """)
        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO CustomerChurn VALUES (
                    {row['CustomerID']}, {row['Age']}, '{row['Gender']}', {row['Tenure']},
                    {row['Usage Frequency']}, {row['Support Calls']}, {row['Payment Delay']},
                    '{row['Subscription Type']}', '{row['Contract Length']}', {row['Total Spend']},
                    {row['Last Interaction']}, '{row['Churn']}', {row['Avg Spend Per Tenure']},
                    {row['Recency Score']}, {row['Churn Risk Score']}, {row['CLV']}
                );
            """)

        conn.commit()
        cursor.close()
        conn.close()

    def call(self):
        prep_obj = DataPreparation()
        cleaned_data = prep_obj.call_prep()  # Call the function
        trans_obj=DataTransformation()
        trans=trans_obj.transformedData(cleaned_data)
        if trans is not None:
            logging.info("Data ingestion successful! Here are the first few rows:")
            logging.info(trans.head())  # logging.info sample data
            logging.info(trans.shape)
            logging.info("success")
        else:
            logging.info("Data ingestion failed.")


if __name__ == "__main__":
    prep_obj = DataPreparation()
    cleaned_data = prep_obj.call_prep()  # Call the function
    trans_obj=DataTransformation()
    trans=trans_obj.transformedData(cleaned_data)

    
    
    trans_obj.call()