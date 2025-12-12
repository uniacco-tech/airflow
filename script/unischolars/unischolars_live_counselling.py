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

REPORT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1GtM28FC1alv10tKMjvEQPM_zbxNRw-1zNsRa2FALXbY/edit?usp=sharing"
REPORT_WORKSHEET_NAME = "Live QL's"
GSPREAD_CREDENTIALS_PATH = '/opt/airflow/secrets/credential.json'

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
        logger.error(f"Failed to authorize Google Sheet client. Ensure credentials file is accessible: {e}")
        raise

def execute_redshift_query(conn):
    query = '''
    SELECT ul.id,
        DATE(convert_timezone('UTC', 'Asia/Kolkata', l.created_at)) AS created_date,
        'https://crm.unischolars.com/leads/' || CAST(l.lead_id AS VARCHAR) AS crm_link,
        ul.name AS student_name,
        l.status,
        CASE
            WHEN LOWER(ul.country) IN ('usa', 'united states', 'united-states', 'us') THEN 'USA'
            WHEN LOWER(ul.country) IN ('uk', 'united kingdom', 'united-kingdom') THEN 'UK'
            WHEN LOWER(ul.country) = 'ireland' THEN 'Ireland'
            WHEN LOWER(ul.country) = 'canada' THEN 'Canada'
            WHEN LOWER(ul.country) = 'australia' THEN 'Australia'
            WHEN LOWER(ul.country) IN ('new zealand', 'new-zealand') THEN 'NZ'
            WHEN LOWER(ul.country) = 'india' THEN 'India'
            ELSE 'ROW'
        END AS Dest_Country,
        ua.first_name || ' ' || ua.last_name AS lqt_agent,
        u.first_name || ' ' || u.last_name AS counsellor,
        ul.utm_source || '-' || ul.utm_medium AS utm,
        pf.name AS branch,
        CASE WHEN EXTRACT(MONTH FROM ul.intake) <= 6 THEN 'Spring ' || EXTRACT(YEAR FROM ul.intake) ELSE 'Fall ' || EXTRACT(YEAR FROM ul.intake)
        END AS Intake, l.lost_reason,
        CASE WHEN l.status='LOST' THEN 1 ELSE 0 END AS is_lost
    FROM unischolarz.crm_leadinfo l
    LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = l.lead_id
    LEFT JOIN unischolarz.accounts_user u ON u.id = l.counsellor_id
    LEFT JOIN unischolarz.unischolarz_site_unilead_tags lt ON lt.unilead_id=l.lead_id
    LEFT JOIN unischolarz.accounts_user ua ON ua.id = l.associate_id
    LEFT JOIN unischolarz.portfolio_branch pf ON pf.id = ul.branch_id
    WHERE l.is_repeated = 0
    AND lt.leadtag_id=246
    ORDER BY l.created_at DESC
    '''
    cur = conn.cursor()
    try:
        logger.info("Executing Redshift Live QL's query...")
        cur.execute(query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"Query executed successfully. Fetched {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Redshift query execution failed: {e}")
        raise
    finally:
        cur.close()


def update_google_sheet(gc, df):
    try:
        ws1 = gc.open_by_url(REPORT_SHEET_URL).worksheet(REPORT_WORKSHEET_NAME)
        gd.set_with_dataframe(worksheet=ws1, dataframe=df, row=1, col=1, include_index=False, include_column_header=True, resize=False)
        logger.info(f"Successfully updated Google Sheet '{REPORT_WORKSHEET_NAME}' with {len(df)} rows.")
    except Exception as e:
        logger.error(f"Failed to update Google Sheet: {e}")
        raise

def run_live_ql_report_etl():
    conn = None
    gc = None
    
    try:
        conn = get_redshift_connection()
        gc = get_google_sheet_client()
        df = execute_redshift_query(conn)
        
        if df.empty:
            logger.warning("Redshift query returned no data. Skipping sheet update.")
            return

        update_google_sheet(gc, df)
        
        logger.info("🏁 Live QL's Report ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Live QL's Report ETL.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
