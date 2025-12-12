import logging
import os
import pandas as pd
import numpy as np
import redshift_connector
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", 5439))
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "nishant")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "dw620LFVkx1Y")

GSPREAD_CREDENTIALS_PATH = '/opt/airflow/secrets/credential.json'

REPORT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1msg2xxl6TRs0rYtbfzse0eUBTEY1d3IJZ3s0vRAdobM/edit?usp=sharing"
REPORT_WORKSHEET_NAME = "Form responses 1"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

def get_redshift_connection():
    try:
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            database=REDSHIFT_DATABASE,
            port=REDSHIFT_PORT,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        logger.info("Connection to Redshift established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {e}")
        raise

def get_google_sheet_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_CREDENTIALS_PATH, scope)
        gc = gspread.authorize(credentials)
        logger.info("Google Sheet client authorized successfully.")
        return gc
    except Exception as e:
        logger.error(f"Failed to authorize Google Sheet client: {e}")
        raise

def run_refund_status_etl():
    conn = None
    gc = None
    
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Starting Refund Status ETL for date: {current_date}")

        conn = get_redshift_connection()
        gc = get_google_sheet_client()

        logger.info(f"Fetching source data from sheet: {REPORT_WORKSHEET_NAME}")
        ws = gc.open_by_url(REPORT_SHEET_URL).worksheet(REPORT_WORKSHEET_NAME)
        df_values = ws.get_all_values()
        
        if not df_values:
            logger.warning("Source Google Sheet is empty. Exiting.")
            return

        headers = df_values.pop(0)
        df = pd.DataFrame(df_values, columns=headers)
        logger.info(f"Fetched {len(df)} rows from Google Sheet.")
        
        redshift_query = '''select distinct tx_id, status from uainventory.leads_leadpayment'''
        cur = conn.cursor()
        cur.execute(redshift_query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        tx_data_raw = pd.DataFrame(rows, columns=cols)
        cur.close()
        logger.info(f"Fetched {len(tx_data_raw)} transaction records from Redshift.")

        pf_refund_data = tx_data_raw.copy()
        deposit_refund_data = tx_data_raw.copy()
        
        pf_refund_data.rename({'tx_id':'Platform Fee Transaction ID'}, axis=1, inplace=True)
        deposit_refund_data.rename({'tx_id':'Deposit Transaction ID', 'status':'dep_status'}, axis=1, inplace=True)
        
        pf_refund_data = pf_refund_data[pf_refund_data['status'].astype(str).str.lower() == 'refunded']
        deposit_refund_data = deposit_refund_data[deposit_refund_data['dep_status'].astype(str).str.lower() == 'refunded']
        
        mer = pd.merge(df, pf_refund_data, on='Platform Fee Transaction ID', how='left')
        mer = pd.merge(mer, deposit_refund_data, on='Deposit Transaction ID', how='left')

        mer['PF Refund Status'] = mer['PF Refund Status'].astype(str)
        mer['Deposit Refund Status'] = mer['Deposit Refund Status'].astype(str)
        mer['PF Refund Status'] = np.where(
            (mer['PF Refund Status'] == '') & (mer['status'].astype(str).str.lower() == 'refunded'),
            mer['status'],mer['PF Refund Status'])
        
        mer['Deposit Refund Status'] = np.where(
            (mer['Deposit Refund Status'] == '') & (mer['dep_status'].astype(str).str.lower() == 'refunded'),
            mer['dep_status'], mer['Deposit Refund Status'])

        mer['PF Refund Date'] = mer['PF Refund Date'].astype(str)
        mer['Deposit Refund Date'] = mer['Deposit Refund Date'].astype(str)
        
        mer['PF Refund Date'] = np.where(
            (mer['PF Refund Date'] == '') & (mer['PF Refund Status'].astype(str).str.lower() == 'refunded'),
            current_date, mer['PF Refund Date'])
        
        mer['Deposit Refund Date'] = np.where(
            (mer['Deposit Refund Date'] == '') & (mer['Deposit Refund Status'].astype(str).str.lower() == 'refunded'),
            current_date, mer['Deposit Refund Date'])

        mer = mer.drop(columns=['status','dep_status'], errors='ignore')

        logger.info(f"Writing updated {len(mer)} rows back to Google Sheet: {REPORT_WORKSHEET_NAME}")
        
        gd.set_with_dataframe(worksheet=ws, dataframe=mer, row=2, col=1, include_index=False, include_column_header=False, resize=False)
        
        logger.info("🏁 Refund Status Update ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Refund Status Update ETL.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
