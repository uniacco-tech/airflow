import logging
import os
import pandas as pd
import redshift_connector
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Tuple

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", 5439))
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "nishant")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "dw620LFVkx1Y")

GSPREAD_CREDENTIALS_PATH = '/opt/airflow/secrets/credential.json'

SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1DfYQJI0IMCmmlGdhE0anzeo-VwcVQmjwGBSRiW1KQZw/edit?usp=sharing"
SOURCE_WORKSHEET_NAME = "Data Dump - Kajal Garg"
REPEATED_WORKSHEET_NAME = "Repeated Leads"
NON_REPEATED_WORKSHEET_NAME = "Non Repeated Leads"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

def get_redshift_connection():
    """Establishes and returns a Redshift connection."""
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
    """Authorizes and returns the gspread client."""
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

def fetch_data(conn, gc):
    """Fetches data from Redshift and the source Google Sheet."""
    
    redshift_query = '''select distinct phone, email from uainventory.leads_lead'''
    cur = conn.cursor()
    try:
        logger.info("Executing Redshift query to fetch existing leads (phone, email)...")
        cur.execute(redshift_query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        existing_leads_df = pd.DataFrame(rows, columns=cols)
        logger.info(f"Fetched {len(existing_leads_df)} existing leads from Redshift.")
    except Exception as e:
        logger.error(f"Redshift query failed: {e}")
        raise
    finally:
        cur.close()

    try:
        logger.info(f"Fetching data from Google Sheet: {SOURCE_WORKSHEET_NAME}")
        ws = gc.open_by_url(SOURCE_SHEET_URL).worksheet(SOURCE_WORKSHEET_NAME)
        # Use get_all_records() for better type inference if possible, otherwise use get_all_values()
        # Sticking close to original, using get_all_values()
        data_values = ws.get_all_values()
        
        if not data_values:
            logger.warning("Source Google Sheet is empty.")
            return pd.DataFrame(), pd.DataFrame() # Return empty DFs
            
        headers = data_values.pop(0)
        source_df = pd.DataFrame(data_values, columns=headers)
        logger.info(f"Fetched {len(source_df)} records from Google Sheet.")
    except Exception as e:
        logger.error(f"Failed to fetch data from Google Sheet: {e}")
        raise
        
    return source_df, existing_leads_df

def dedupe_leads(source_df: pd.DataFrame, existing_leads_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Applies the deduping logic to separate repeated and non-repeated leads."""
    if source_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    logger.info("Starting lead deduping process...")
    
    source_df['phone_clean'] = source_df['phone'].astype(str).str[-10:].str.replace('[^0-9]', '', regex=True)
    source_df['email_lower'] = source_df['email'].astype(str).str.lower()
    
    existing_leads_df['phone_clean'] = existing_leads_df['phone'].astype(str).str[-10:].str.replace('[^0-9]', '', regex=True)
    existing_leads_df['email_lower'] = existing_leads_df['email'].astype(str).str.lower()
    
    existing_phones = set(existing_leads_df['phone_clean'].dropna())
    existing_emails = set(existing_leads_df['email_lower'].dropna())
    
    is_repeated_phone = source_df['phone_clean'].isin(existing_phones)
    is_repeated_email = source_df['email_lower'].isin(existing_emails)
    is_repeated = is_repeated_phone | is_repeated_email

    repeated = source_df[is_repeated].copy()
    non_repeated = source_df[~is_repeated].copy()
    
    repeated = repeated.drop(columns=['phone_clean', 'email_lower'], errors='ignore')
    non_repeated = non_repeated.drop(columns=['phone_clean', 'email_lower'], errors='ignore')

    logger.info(f"Deduping complete. Repeated: {len(repeated)} leads. Non-Repeated: {len(non_repeated)} leads.")
    return repeated, non_repeated

def write_to_google_sheet(gc, df: pd.DataFrame, worksheet_name: str):
    """Writes the DataFrame to a specific Google Sheet worksheet."""
    if df.empty:
        logger.warning(f"No data to write to '{worksheet_name}'. Skipping sheet update.")
        return

    try:
        logger.info(f"Writing {len(df)} rows to worksheet: {worksheet_name}")
        ws1 = gc.open_by_url(SOURCE_SHEET_URL).worksheet(worksheet_name)
        ws1.clear()

        gd.set_with_dataframe(worksheet=ws1, dataframe=df, row=1, col=1, include_index=False, include_column_header=True, resize=True)
        logger.info(f"Successfully updated Google Sheet '{worksheet_name}'.")
    except Exception as e:
        logger.error(f"Failed to write to Google Sheet '{worksheet_name}': {e}")
        raise

def run_lead_dedupe_etl():
    conn = None
    gc = None
    
    try:
        conn = get_redshift_connection()
        gc = get_google_sheet_client()

        source_df, existing_leads_df = fetch_data(conn, gc)

        if source_df.empty:
            logger.info("Source data dump was empty. Exiting ETL gracefully.")
            return

        repeated_df, non_repeated_df = dedupe_leads(source_df, existing_leads_df)

        write_to_google_sheet(gc, repeated_df, REPEATED_WORKSHEET_NAME)
        write_to_google_sheet(gc, non_repeated_df, NON_REPEATED_WORKSHEET_NAME)
        
        logger.info("🏁 Lead Deduping ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Lead Deduping ETL.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
