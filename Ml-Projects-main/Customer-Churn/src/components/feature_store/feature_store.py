import snowflake.connector
import pandas as pd
from prefect import flow
import logging

logging.basicConfig(
    filename="prep.log",
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

log = logging.getLogger()
log.setLevel(logging.INFO)  # Set the logging level to INFO




def get_customer_features(customer_id):
    """Fetch features for a given customer from Snowflake."""
    conn = snowflake.connector.connect(
                user='SHAHHUSSAIN',
                password='Shahid22Sherin',
                account='TIQIOCR-KY36542',
                database='PROJECT',
                schema='PUBLIC',
                session_parameters={
                    'QUERY_TAG': 'EndOfMonthFinancials',
                })
    query = f"SELECT * FROM CUSTOMERCHURNTRANSFORMED WHERE CustomerID = '{customer_id}';"
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df.to_dict(orient='records')

# Example Usage
if __name__ == "__main__":
    customer_features = get_customer_features("1")
    log.info(customer_features)
    print(customer_features)
