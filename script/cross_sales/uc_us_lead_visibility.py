import logging
import os
import pandas as pd
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

GSPREAD_CREDENTIALS_PATH = os.environ.get("GSPREAD_CREDENTIALS_PATH", '/home/nishant/credentials.json') # Needs to be accessible
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1hyTLRzbEWENvVdvsKnqHnNLfEyfty1L1zD0POGODX_8/edit?usp=sharing"
WORKSHEET_NAME = "AIP LEAD FLOW DATA"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

SQL_QUERY_CROSS_SALES = '''
SELECT * FROM (
    SELECT 
        MAX(source_lead_id) AS us_lead_id,
        MAX(ul.name) AS student_name,
        MAX(cs.source_status) AS us_status,
        MAX(s.counsellor_agent) AS counsellor,
        MAX(DATE(s.lead_created_at)) AS us_created_date,
        MAX(s.fin_aip_dat) AS aip_date,
        MAX(cs.dest_lead_created_at) AS uc_created_date,
        MAX(s.hybrid_branch) AS branch,
        dest_lead_id AS uc_lead_id,
        MAX(dest_status) AS uc_status,
        MAX(cs.dest_agent) AS login_agent,
        MAX(uc3.logged_in_date) AS logged_in_at,
        MAX(uc3.sanctioned_date) AS sanctioned_at,
        MAX(uc3.pf_confirmed_date) AS pf_confired_date,
        MAX(cs.dest_utm_medium) AS utm_medium,
        MAX(cs.dest_lost_reason) AS lost_reason,
        MAX(uc_notes) AS uc_notes
    FROM cross_sales_uc cs
    INNER JOIN us_standard1 s ON s.lead_id = cs.source_lead_id
    LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = s.lead_id
    LEFT JOIN uc_lead_3 uc3 ON uc3.lead_id = cs.dest_lead_id
    LEFT JOIN (
        SELECT 
            lead_id,
            LISTAGG(TRIM(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(note, '</p>', '\n'), '</div>', '\n'), '&lt;', '<'), '&gt;', '>'), '&amp;', '&'), '<[^>]+>', '' ))) WITHIN GROUP (ORDER BY id DESC) AS uc_notes
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY lead_id ORDER BY id DESC) rankee 
            FROM unicredsnew.crm_note 
            WHERE created_by_system = 0
        )
        WHERE rankee IN (1, 2)
        GROUP BY 1
    ) AS aa ON aa.lead_id = cs.dest_lead_id
    WHERE cs.lead_id_from = 'unischolars'
    GROUP BY 9
    ORDER BY 7
)
WHERE uc_created_date >= '2024-04-01'
'''

def get_redshift_connection():
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

def execute_query_and_fetch_data(conn, query: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        data = pd.DataFrame(rows, columns=cols)
        logger.info(f"Query executed successfully. Fetched {len(data)} rows.")
        return data
    except Exception as e:
        logger.error(f"Redshift query execution failed: {e}")
        raise
    finally:
        cur.close()

def upload_to_google_sheet(gc, df: pd.DataFrame, sheet_url: str, worksheet_name: str):
    try:
        ws1 = gc.open_by_url(sheet_url).worksheet(worksheet_name)
        
        # Upload including column header and starting from row 1, col 1
        gd.set_with_dataframe(
            ws1, df, row=1, col=1, 
            include_index=False, include_column_header=True
        )
        logger.info(f"Successfully uploaded {len(df)} rows to Google Sheet: {worksheet_name}.")
    except Exception as e:
        logger.error(f"Failed to upload data to Google Sheet: {e}")
        raise

def run_us_uc_cross_sales_report():
    conn = None
    
    try:
        # 1. Connect and Authorize
        conn = get_redshift_connection()
        gc = get_google_sheet_client()

        # 2. Extract Data
        data_df = execute_query_and_fetch_data(conn, SQL_QUERY_CROSS_SALES)
        
        if data_df.empty:
            logger.warning("No cross-sales data found to upload. Exiting.")
            return

        # 3. Load Data to Google Sheet (Overwrites entire sheet)
        upload_to_google_sheet(gc, data_df, GSHEET_URL, WORKSHEET_NAME)
        
        logger.info("🏁 UniScholars to UniCreds Cross-Sales Report ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the ETL process.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
