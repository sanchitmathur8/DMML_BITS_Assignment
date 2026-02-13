import os
import pandas as pd
import snowflake.connector
from dataclasses import dataclass
from src.logger import logging
from src.exception import CustomException
import sys
from prefect import flow



@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join('artifacts', "data.csv")


class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def fetch_data_from_snowflake(self,query):
        """
        Connects to a Snowflake database using environment variables,
        executes a given SQL query, and returns the result as a Pandas DataFrame.
        
        :param query: SQL query as a string
        :return: Pandas DataFrame containing query results
        """
        try:

            # Load Snowflake credentials from environment variables
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


            # Execute the query and load results into a DataFrame
            df = pd.read_sql_query(query, conn)

            logging.info(f"Successfully fetched {df.shape[0]} records from Snowflake.")
            logging.info(f" {df.head(2)} ")
            
            
            # Close the connection
            conn.close()
            return df

        except Exception as e:
            logging.info(f"Error fetching data from Snowflake: {e}")
            raise CustomException(e, sys)


    def ingest_csv_data(self, file_path):
        try:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                logging.info(f"CSV file loaded successfully from '{file_path}', shape: {df.shape}")
                logging.info(f"{df.head(2)}")
                return df
            else:
                logging.error(f"File not found: {file_path}")
                return None
        except pd.errors.EmptyDataError:
            logging.error("CSV file is empty.")
            raise CustomException("CSV file is empty.", sys)
        except pd.errors.ParserError:
            logging.error("Error parsing CSV file.")
            raise CustomException("Error parsing CSV file.", sys)
        except Exception as e:
            logging.error(f"Unexpected error while loading CSV: {e}")
            raise CustomException(e, sys)

    def scheduled_ingestion(self, csv_path, data):
        try:
            df1 = self.ingest_csv_data(csv_path)  # Load CSV data
            df2 = data  # Data from Snowflake

            if df1 is not None and df2 is not None:
                # Standardize column names (lowercase, strip spaces)
                df1.columns = df1.columns.str.strip().str.lower()
                df2.columns = df2.columns.str.strip().str.lower()

                # Get union of all columns
                all_columns = list(set(df1.columns).union(set(df2.columns)))

                # Ensure both dataframes have the same columns
                df1 = df1.reindex(columns=all_columns, fill_value=None)
                df2 = df2.reindex(columns=all_columns, fill_value=None)

                # Merge the two datasets
                combined_data = pd.concat([df1, df2], ignore_index=True)

                logging.info(f"Combined dataset shape: {combined_data.shape}")
                return combined_data

            elif df1 is not None:
                return df1
            elif df2 is not None:
                return df2
            else:
                logging.error("No valid data found for ingestion.")
                return None

        except Exception as e:
            logging.error(f"Error during scheduled ingestion: {e}")
            raise CustomException(e, sys)

    def call(self):
        sql_query = "SELECT * FROM CUSTOMERCHURN;"  # Modify as per need
        try:
            data = self.fetch_data_from_snowflake(sql_query)

            # File paths
            csv_path = r'D:\Ml-Projects\Customer-Churn\notebook\data\customerChurn.csv'
            # Load and combine data
            df = self.scheduled_ingestion(csv_path, data)
        
            if df is not None:
                # Save DataFrame as CSV
                output_csv_path = r"D:\Ml-Projects\Customer-Churn\notebook\data\raw_ingested.csv"
                df.to_csv(output_csv_path, index=False)
                logging.info(f"Final dataset saved to: {output_csv_path}")
                return df  # Return the dataframe

        except Exception as e:
            logging.error(f"Critical error in data ingestion: {e}")
            return None

if __name__ == "__main__":
            obj = DataIngestion()
            obj.call()