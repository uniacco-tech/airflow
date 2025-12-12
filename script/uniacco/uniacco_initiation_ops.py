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

credentials = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/secrets/credential.json', scope)

REPORT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1MgWqSyOzwINVopojSi9lJX7IlpgAsNU3zLIZ-Y2jWSA/edit?usp=sharing"
REPORT_WORKSHEET_NAME = "Dump"

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
        logger.error(f"Failed to authorize Google Sheet client. Ensure credentials file is accessible: {e}")
        raise

def execute_redshift_query(conn):
    """Executes the Redshift query and returns the result as a DataFrame."""
    query = '''
    WITH booki AS
        (SELECT (bif.created_at + interval '330' minute) AS initiation_filled_at,
                date((bif.created_at + interval '330' minute - interval '360' minute)) AS initiation_filled_date,
                bif.id, bif.property_name AS property, (bif.first_name || ' ' || bif.last_name) AS student_name,
                bif.is_active, bif.booking_type, bif.cancel_reason, bif.kind, property_id, lead_id
        FROM uainventory.crm_bookinginitiationform bif),
        tagged_leads AS
        ( SELECT a.lead_id FROM uainventory.leads_lead_tags AS a
        WHERE leadtags_id = 132
        GROUP BY lead_id),
        
        crm AS
        (SELECT a.lead_id, a.status, a.agent_id, a.sales_ops_agent_id
        FROM uainventory.crm_leadinfo AS a),
        
        usee AS
        (SELECT (ae.first_name || ' ' || ae.last_name) namee, id
        FROM uainventory.auth_user ae),
        
        conversion_form AS
        (SELECT c.booking_reference_id AS initation_id, c.booking_date
        FROM uainventory.crm_conversionformmodel c
        WHERE booking_reference_id>0)
    
    SELECT bif.id,
        lf.lead_id, 'https://crm.uniacco.com/leads/' || cast(lf.lead_id as varchar) AS crm_link,
        student_name, pm.name AS property_manager, bif.property,
        (au.namee) AS sales_agent, ae.namee AS ops_agent, initiation_filled_at, lf.status, bif.booking_type, bif.kind,
        CASE WHEN tl.lead_id IS NOT NULL THEN 'yes' ELSE 'no' END AS booking_initiated,
        bif.is_active, bif.cancel_reason, initiation_filled_date, c.booking_date
    FROM booki bif
    JOIN crm lf ON bif.lead_id = lf.lead_id
    LEFT JOIN tagged_leads tl ON bif.lead_id = tl.lead_id
    LEFT JOIN uainventory.inventory_property p ON p.id = bif.property_id
    LEFT JOIN uainventory.inventory_propmanager pm ON pm.id = p.manager_id
    LEFT JOIN usee au ON au.id = lf.agent_id
    LEFT JOIN usee ae ON ae.id = lf.sales_ops_agent_id
    LEFT JOIN conversion_form c ON c.initation_id=bif.id
    ORDER BY bif.id
    '''
    cur = conn.cursor()
    try:
        logger.info("Executing Redshift inventory report query...")
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
    """Updates the specified Google Sheet worksheet with the DataFrame."""
    try:
        ws1 = gc.open_by_url(REPORT_SHEET_URL).worksheet(REPORT_WORKSHEET_NAME)
        # Sort values as in the original script
        existing_df = df.sort_values(by=['id'], ascending=False)
        
        # Clear existing data but keep headers (assuming row 1 is headers)
        ws1.clear()
        
        # Write the entire dataframe including headers
        gd.set_with_dataframe(
            worksheet=ws1,  dataframe=existing_df, row=1, col=1, include_index=False, include_column_header=True, resize=True)
        logger.info(f"Successfully updated Google Sheet '{REPORT_WORKSHEET_NAME}' with {len(existing_df)} rows.")
    except Exception as e:
        logger.error(f"Failed to update Google Sheet: {e}")
        raise

def run_inventory_report_etl():
    """
    Main function to run the entire ETL process for the inventory report.
    This function will be called by the Airflow PythonOperator.
    """
    conn = None
    gc = None
    
    try:
        conn = get_redshift_connection()
        gc = get_google_sheet_client()
        df = execute_redshift_query(conn)
        update_google_sheet(gc, df)
        
        logger.info("🏁 Inventory Report ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Inventory Report ETL.", exc_info=True)
        # Re-raise to fail the Airflow task
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
