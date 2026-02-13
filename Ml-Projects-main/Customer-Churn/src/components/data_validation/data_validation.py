from dataclasses import dataclass
import numpy as np 
import pandas as pd
from src.components.data_ingestion.data_ingestion import DataIngestion
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from src.logger import logging
from src.exception import CustomException
from prefect import flow


# Data Quality Report Function
def generate_data_quality_report(df):
    report = {}

    # Missing Values Check
    missing_values = df.isnull().sum()
    report["Missing Values (%)"] = (missing_values / len(df)) * 100

    # Data Type Validation
    expected_types = {
        "customerid": "int64",
        "age": "int64",  
        "gender": "object",
        "tenure": "int64",
        "usage frequency": "int64",
        "support calls": "int64",
        "payment delay": "int64",
        "subscription type": "object",
        "contract length": "object",
        "total spend": "int64",
        "last interaction": "int64",
        "churn": "object",
    }

    actual_types = df.dtypes
    report["Incorrect Data Types"] = {col: str(actual_types[col]) for col in expected_types if str(actual_types[col]) != expected_types[col]}

    # Outlier Detection (Numeric Fields)
    numeric_cols = df.select_dtypes(include=['number']).columns

    # Exclude CustomerID
    numeric_cols = numeric_cols.drop('customerid', errors='ignore')


    outliers = {}
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outlier_count = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
        if outlier_count > 0:
            outliers[col] = outlier_count
    report["Outliers Detected"] = outliers

    # Duplicate Records Check
    duplicate_count = df.duplicated().sum()
    report["Duplicate Records"] = duplicate_count

    # Logging Report
    logging.info("\nData Quality Report Generated")
    logging.info(report)

    return report

@flow
# PDF Export Function
def export_data_quality_report_to_pdf(report, file_name="data_quality_report.pdf"):
    doc = SimpleDocTemplate(file_name, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()

    # Title
    elements.append(Paragraph("Data Quality Report", styles['Title']))

    # Add report details
    for section, data in report.items():
        elements.append(Paragraph(f"<b>{section}</b>", styles['Heading2']))

        # Handle dictionary data (e.g., Incorrect Data Types or Outliers)
        if isinstance(data, dict):
            table_data = [["Column", "Details"]] + list(data.items())
        else:
            table_data = [[section, data]]

        # Table Formatting
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige)
        ]))
        elements.append(table)

    # Build PDF
    doc.build(elements)
    logging.info(f"Data Quality Report successfully exported to '{file_name}'")



if __name__ == "__main__":
    ingestion_obj = DataIngestion()
    combined_data = ingestion_obj.call()  # Call the function

    if combined_data is not None:
        logging.info("Data ingestion successful! Here are the first few rows:")
        logging.info(combined_data.head())  # logging.info sample data
        logging.info(combined_data.shape)
    else:
        logging.info("Data ingestion failed.")
# Run Validation
    data_quality_report = generate_data_quality_report(combined_data)
    export_data_quality_report_to_pdf(data_quality_report)

