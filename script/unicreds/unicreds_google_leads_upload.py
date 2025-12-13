import logging
import os
import pandas as pd
import redshift_connector
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", 5439))
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "nishant")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "dw620LFVkx1Y")

GSPREAD_CREDENTIALS_PATH = os.environ.get("GSPREAD_CREDENTIALS_PATH", '/opt/airflow/secrets/credential.json')

EC_QUALIFIED_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ALCc9SVmgqKPyM0bPe1HBvi2x_sNyb5UgZx1lq5iX0E/edit?usp=sharing"
EC_CATERABLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Plrg50IREhGAk-o5SG7U5EnmamcAGipDMe9He58pFRk/edit?usp=sharing"
WORKSHEET_NAME = "Sheet1"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

SQL_QUERY_EC_QUALIFIED = '''
SELECT 
    sf.email "Email",
    sf.phone "Phone Number",
    'EC Qualified' AS "Conversion Name",
    TO_CHAR((sf.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') || ' Asia/Calcutta' AS "Conversion Time 1"
FROM unicredsnew.site_forms_lead sf
LEFT JOIN 
    (SELECT lead_id, DATE(MIN(created_at + INTERVAL '330' MINUTE)) AS processing_date 
     FROM unicredsnew.crm_historicalleadinfo
     WHERE LOWER(status) = 'processing' GROUP BY 1) AS ss ON sf.id = ss.lead_id
LEFT JOIN unicredsnew.crm_leadinfo AS aa ON sf.id = aa.lead_id
WHERE 
    LOWER(sf.utm_source) = 'google' 
    AND DATE(sf.created_at + INTERVAL '330' MINUTE) >= DATE '2025-06-06'
    AND aa.is_repeated = 0
    AND DATE(processing_date) >= DATE '2025-06-06' -- CRITICAL: Must have reached processing status
    AND sf.gclid > 0
    AND (aa.lost_reason NOT IN ('not-applied', 'indian-university', 'junk-lead', 'low-loan-amount') 
         OR sf.admission_status NOT IN ('not-applied', 'NOT APPLIED'))
'''

SQL_QUERY_EC_CATERABLE = '''
SELECT 
    sf.email "Email",
    sf.phone "Phone Number",
    'EC for Caterable Leads' AS "Conversion Name",
    TO_CHAR((sf.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') || ' Asia/Calcutta' AS "Conversion Time 1"
FROM unicredsnew.site_forms_lead sf
LEFT JOIN unicredsnew.crm_leadinfo AS aa ON sf.id = aa.lead_id
WHERE 
    LOWER(sf.utm_source) = 'google' 
    AND DATE(sf.created_at + INTERVAL '330' MINUTE) > DATE '2025-06-09'
    AND aa.is_repeated = 0
    AND sf.gclid > 0
    AND (aa.lost_reason NOT IN ('not-applied', 'indian-university', 'junk-lead', 'low-loan-amount') 
         OR sf.admission_status NOT IN ('not-applied', 'NOT APPLIED'))
'''

def get_redshift_connection():
    """Establishes and returns a Redshift connection."""
    try:
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST, database=REDSHIFT_DATABASE, port=REDSHIFT_PORT,
            user=REDSHIFT_USER, password=REDSHIFT_PASSWORD
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
        logger.error(f"Failed to authorize Google Sheet client. Ensure credentials file is accessible: {e}")
        raise

def execute_query_and_prepare_df(conn, query, conversion_name):
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"Query for '{conversion_name}' executed successfully. Fetched {len(df)} rows.")
    except Exception as e:
        logger.error(f"Redshift query execution failed for '{conversion_name}': {e}")
        raise
    finally:
        cur.close()
        
    if df.empty:
        logger.warning(f"No data returned for '{conversion_name}'.")
        return pd.DataFrame(columns=['Email', 'Phone Number', 'Conversion Name', 'Conversion Time'])
        
    df['Conversion Time'] = df['Conversion Time 1'].str.replace(
        r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})', r'\1T\2', regex=True
    ).str.replace(r' Asia/Calcutta', 'Z') # Google Ads uses ISO format, replacing T with Z is common practice for simple conversion
    
    #df = df.drop(columns=['Conversion Time 1'])
    df.columns = ['Email', 'Phone Number', 'Conversion Name', 'Conversion Time']
    
    return df

# --- Task 1: EC Qualified ETL (Merge Logic) ---

def ec_qualified_etl(conn, gc):
    logger.info("Starting EC Qualified ETL (Merge Logic)...")
    
    # 1. Fetch data from Redshift and prepare
    df_new = execute_query_and_prepare_df(conn, SQL_QUERY_EC_QUALIFIED, 'EC Qualified')
    if df_new.empty:
        return

    # 2. Read existing data from Google Sheet
    try:
        ws = gc.open_by_url(EC_QUALIFIED_SHEET_URL).worksheet(WORKSHEET_NAME)
        ema = ws.get_all_values()
        
        if len(ema) > 0:
             headers = ['Email', 'Phone Number', 'Conversion Name', 'Conversion Time']
             # The existing code assumes row 1 (index 0) has headers, but the upload starts at row 2 (index 1)
             # Let's assume the sheet structure matches the DF structure for merging
             ema_data = pd.DataFrame(ema[1:], columns=headers) 
        else:
            logger.warning("Existing Google Sheet is empty.")
            ema_data = pd.DataFrame(columns=df_new.columns)

        logger.info(f"Read {len(ema_data)} existing rows from Google Sheet for merging.")
    except Exception as e:
        logger.error(f"Failed to read existing Google Sheet for EC Qualified: {e}")
        raise

    # 3. Merge and Filter for New Rows
    merged_df = df_new.merge(ema_data, on=list(df_new.columns), how='outer', indicator=True)
    result_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
    
    logger.info(f"Found {len(result_df)} new, unique rows to append.")

    # 4. Append New Data to Google Sheet (row 2, skipping the header row)
    if not result_df.empty:
        final_df = pd.concat([ema_data, result_df], ignore_index=True)
        
        final_upload_df = final_df[df_new.columns] # Reorder columns if needed
        
        gd.set_with_dataframe(ws, final_upload_df, row=2, col=1, include_index=False, include_column_header=False, resize=False)
        logger.info(f"Successfully appended {len(result_df)} new rows (Total rows: {len(final_upload_df)}) to EC Qualified sheet.")
    else:
        logger.info("No new unique rows found for EC Qualified. Sheet not updated.")

# --- Task 2: EC Caterable ETL (Direct Upload) ---

def ec_caterable_etl(conn, gc):
    logger.info("Starting EC Caterable ETL (Direct Upload)...")
    
    # 1. Fetch data from Redshift and prepare
    df_11 = execute_query_and_prepare_df(conn, SQL_QUERY_EC_CATERABLE, 'EC for Caterable Leads')
    if df_11.empty:
        return
        
    # 2. Upload to Google Sheet (overwrite starting at row 2, excluding headers)
    try:
        ws1 = gc.open_by_url(EC_CATERABLE_SHEET_URL).worksheet(WORKSHEET_NAME)
        
        ws1.update('A1:D1', [df_11.columns.tolist()]) # Update header row
        
        gd.set_with_dataframe(
            ws1, df_11, row=2, col=1, 
            include_index=False, include_column_header=False, resize=False
        )
        logger.info(f"Successfully uploaded {len(df_11)} rows to EC for Caterable Leads sheet.")
    except Exception as e:
        logger.error(f"Failed to upload data to EC Caterable sheet: {e}")
        raise

# --- Main Callable Function for Airflow ---

def run_conversion_tracking_etl():
    conn = None
    
    try:
        # 1. Connect to Redshift
        conn = get_redshift_connection()
        
        # 2. Authorize Google Sheet Client
        gc = get_google_sheet_client()

        # 3. Run EC Qualified ETL
        ec_qualified_etl(conn, gc)
        
        # 4. Run EC Caterable ETL
        ec_caterable_etl(conn, gc)
        
        logger.info("🏁 Google Ads Conversion Tracking ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Conversion Tracking ETL.", exc_info=True)
        # Re-raise to fail the Airflow task
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
