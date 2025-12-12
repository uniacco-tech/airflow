import logging
import os
import pandas as pd
import redshift_connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import io
from datetime import datetime, timedelta

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", 5439))
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "nishant")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "dw620LFVkx1Y")

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "nishant.sharma@uniacco.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "vcjswljkfhiikmld") # ⬅️ SECURE THIS

RECIPIENT_EMAILS = ['anupam.gupta@uniacco.com', 'data@uniacco.com', 'bhavya.banda@unicreds.com', 'prajesh.agrawal@unicreds.com']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

SQL_QUERY_UC_LOST_NOT_APPLIED = '''
SELECT a.*, b.phone, b.email FROM
(SELECT lead_id, name AS lead_name, DATE(lead_date) lead_created_date, DATE(lost_day) lost_date, lost_reason, agent_name, utm_source, utm_medium
FROM uc_lead_2 
WHERE DATE(lost_day) BETWEEN DATE(CURRENT_DATE - INTERVAL '2' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)
AND is_repeated = 0
AND lost_reason IN ('NOT-APPLIED','NOT RESPONDING')) AS a 
LEFT JOIN unicredsnew.site_forms_lead AS b ON a.lead_id = b.id
'''

SQL_QUERY_UC_OPPORTUNITY = '''
SELECT a.*, b.phone, b.email FROM
(SELECT lead_id, name AS lead_name, DATE(lead_date) lead_created_date, status, agent_name, utm_source, utm_medium
FROM uc_lead_2 
WHERE DATE(max_opportunity_date) BETWEEN DATE(CURRENT_DATE - INTERVAL '2' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)
AND is_repeated = 0
AND status = 'OPPORTUNITY') AS a 
LEFT JOIN unicredsnew.site_forms_lead AS b ON a.lead_id = b.id
'''

SQL_QUERY_US_LOST = '''
SELECT s.lead_id, ul.email, ul.phone, ul.name, DATE(s.lead_created_at) AS created_date,
       cur_status, s.intake, s.lost_reason, DATE(s.qualified_date) AS qualified_date, DATE(s.lost_date) AS lost_date,
       s.utm_source, s.utm_medium, s.counsellor_agent, s.lqt_agent
FROM us_standard1 s
LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id=s.lead_id 
WHERE DATE(s.lost_date) BETWEEN DATE(CURRENT_DATE - INTERVAL '2' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)
AND s.qualified_date > 0
AND is_repeated = 0
AND s.lost_reason NOT IN ('wrong number', 'not an indian lead', 'junk lead', 'change of country', 'looking for jobs', 'secured job', 'dropped plans') 
AND s.intake IN ('09-2025','01-2026','2026-09') AND cur_status = 'LOST'
AND s.lead_id NOT IN
    (SELECT dest_lead_id FROM cross_sales_us
    WHERE DATE(dest_lead_created_at) >= DATE(CURRENT_DATE - INTERVAL '45' DAY))
'''

SQL_QUERY_UA_LOST = '''
SELECT a.lead_id, ll.email, ll.phone, ll.name, DATE(a.created_at) AS created_at, a.lead_status, a.nationality, a.agent_name, a.lost_reason, a.movein, a.intake_string, a.utm_source, a.utm_medium, a.lost_date
FROM uniacco_lead_mart a
LEFT JOIN uainventory.leads_lead ll ON ll.id=a.lead_id
WHERE DATE(a.lost_date) BETWEEN DATE(CURRENT_DATE - INTERVAL '2' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)
AND a.nationality = 'India'
AND a.intake_string IN ('2025-09','01-2026','2026-09')
AND a.lead_status = 'LOST'
AND a.lost_reason NOT IN ('Junk lead', 'Test Automation Junk Leads', 'Just Enquiring') 
AND a.is_repeated = 0
AND a.lead_id NOT IN
    (SELECT dest_lead_id FROM cross_sales_ua
    WHERE lead_id_from = 'unicreds' AND dest_lead_created_at >= CURRENT_DATE - INTERVAL '45' DAY)
'''

SQL_QUERY_UA_OPPORTUNITY = '''
SELECT a.lead_id, ll.email, ll.phone, ll.name, DATE(a.created_at) AS created_at, a.lead_status, a.nationality, a.agent_name, a.movein, a.intake_string, a.utm_source, a.utm_medium
FROM uniacco_lead_mart a
LEFT JOIN uainventory.leads_lead ll ON ll.id=a.lead_id
WHERE DATE(a.opportunity_date) BETWEEN DATE(CURRENT_DATE - INTERVAL '2' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)
AND a.nationality = 'India'
AND a.intake_string IN ('2025-09','01-2026','2026-09')
AND a.lead_status <> 'LOST'
AND a.lead_id NOT IN
    (SELECT dest_lead_id FROM cross_sales_ua
    WHERE lead_id_from = 'unicreds' AND dest_lead_created_at >= CURRENT_DATE - INTERVAL '45' DAY)
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

def execute_query(conn, query: str, name: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"Query '{name}' executed successfully. Fetched {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Redshift query execution failed for '{name}': {e}")
        raise
    finally:
        cur.close()

def process_uc_lost_leads(df: pd.DataFrame):
    app = df[(df['lost_reason'] == 'NOT-APPLIED')].copy()
    not_resp = df[(df['lost_reason'] == 'NOT RESPONDING')].copy()
    logger.info(f"UniCreds Lost: {len(app)} Not Applied, {len(not_resp)} Not Responding.")
    return app, not_resp

def create_excel_attachment(data_dict: dict) -> io.BytesIO:
    excel_bytes_io = io.BytesIO()

    try:
        with pd.ExcelWriter(excel_bytes_io, engine='xlsxwriter') as writer:
            data_dict['app'].to_excel(writer, sheet_name='Not_Applied_Leads', index=False)
            data_dict['not_resp'].to_excel(writer, sheet_name='Not_Responding_Leads', index=False)
            data_dict['opp'].to_excel(writer, sheet_name='Opportunity_Leads', index=False)
            data_dict['us'].to_excel(writer, sheet_name='UniScholars_Leads', index=False)
            data_dict['ua'].to_excel(writer, sheet_name='UniAcco_Lost_Leads', index=False)
            data_dict['ua1'].to_excel(writer, sheet_name='UniAcco_Opportunity_Leads', index=False)

        excel_bytes_io.seek(0)
        logger.info("Excel attachment created successfully in memory.")
        return excel_bytes_io
    except Exception as e:
        logger.error(f"Failed to create Excel file: {e}")
        raise

def send_lost_dump_email(subject, body, recipient_emails, excel_stream):    
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)

        message = MIMEMultipart()
        message['From'] = SENDER_EMAIL
        message['To'] = ', '.join(recipient_emails)
        message['Subject'] = subject

        message.attach(MIMEText(body, 'html'))
        
        yesterday = datetime.now() - timedelta(days=1)
        day_suffix = lambda d: "th" if 11<=d<=13 else {1:"st",2:"nd",3:"rd"}.get(d%10, "th")
        formatted_date = f"{yesterday.day}{day_suffix(yesterday.day)} {yesterday.strftime('%B %Y')}"
        
        excel_attachment = MIMEApplication(excel_stream.getvalue(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        excel_attachment.add_header('Content-Disposition', 'attachment', filename=f'UC_Lost_Leads_{formatted_date}.xlsx')
        message.attach(excel_attachment)

        # Send the email
        server.sendmail(SENDER_EMAIL, recipient_emails, message.as_string())
        server.quit()
        logger.info(f"Email sent successfully to {len(recipient_emails)} recipients.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise

def run_lost_lead_dump_etl():
    conn = None
    
    try:
        # 1. Setup Time and Connections
        conn = get_redshift_connection()
        
        yesterday = datetime.now() - timedelta(days=1)
        day_suffix = lambda d: "th" if 11<=d<=13 else {1:"st",2:"nd",3:"rd"}.get(d%10, "th")
        formatted_date = f"{yesterday.day}{day_suffix(yesterday.day)} {yesterday.strftime('%B %Y')}"
        
        # 2. Execute all Redshift Queries
        df_uc_lost = execute_query(conn, SQL_QUERY_UC_LOST_NOT_APPLIED, 'UC Lost/Not Responding')
        opp = execute_query(conn, SQL_QUERY_UC_OPPORTUNITY, 'UC Opportunity')
        us = execute_query(conn, SQL_QUERY_US_LOST, 'UniScholars Lost')
        ua = execute_query(conn, SQL_QUERY_UA_LOST, 'UniAcco Lost')
        ua1 = execute_query(conn, SQL_QUERY_UA_OPPORTUNITY, 'UniAcco Opportunity')
        
        # 3. Process UniCreds Lost Data
        app, not_resp = process_uc_lost_leads(df_uc_lost)

        # 4. Create Excel Attachment
        data_for_excel = {
            'app': app, 
            'not_resp': not_resp,
            'opp': opp,
            'us': us,
            'ua': ua,
            'ua1': ua1,
        }
        excel_bytes_io = create_excel_attachment(data_for_excel)
        
        # 5. Compile Email Body
        subject = f"UniCreds Lost Data - {formatted_date}"
        body = """
<html>
  <body>
    <p>Hi Team,<br><br>
    Please find the attached UniCreds Lost Dump below.</p>

    <p>Best regards,<br>Reporting Team</p>
  </body>
</html>
"""

        # 6. Send Email
        send_lost_dump_email(subject, body, RECIPIENT_EMAILS, excel_bytes_io)
        
        logger.info("🏁 Lost Lead Dump Report ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Lost Lead Dump ETL.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
