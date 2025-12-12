import logging
import os
import pandas as pd
import redshift_connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import io
from datetime import datetime, timedelta, date

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", 5439))
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "nishant")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "dw620LFVkx1Y")

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "aman.rohada@uniacco.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "dmrtiyrbpqoaeweg")

RECIPIENT_EMAILS = [
    'anupam.gupta@uniacco.com', 'data@uniacco.com', 'pratibha.singh@unischolars.com', 
    'rajesh.velamarthi@unischolars.com', 'roopali.birman@unischolars.com', 
    'neha.ansari@unischolars.com', 'mohammed.shareeq@unischolarz.com', 
    'misbah.rehman@unischolarz.com', 'minu.rajan@unischolarz.com', 'sanghamitra.das@unischolarz.com']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

SQL_QUERY_SALES_CADENCE = """
WITH Qualified_time AS (
    SELECT 
        l.lead_id, l.status,
        CASE
            WHEN ul.preferred_time_slot != '' AND TRY_CAST(ul.preferred_time_slot AS TIMESTAMP) IS NOT NULL THEN 
                CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', ul.preferred_time_slot::TIMESTAMP)
            ELSE 
                s.last_cycle_qualified_date
        END AS preferred_time,
        CASE WHEN ul.preferred_time_slot != '' AND TRY_CAST(ul.preferred_time_slot AS TIMESTAMP) IS NOT NULL THEN 1 ELSE 0 END AS has_preferred_time,
        s.last_cycle_qualified_date as qualified_date
    FROM unischolarz.crm_leadinfo l
    LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = l.lead_id
    LEFT JOIN us_standard1 s ON s.lead_id = l.lead_id
    WHERE l.status = 'QUALIFIED'
)
SELECT 
    t.id, t.summary, t.lead_id, t.created_by_id, pb.name AS branch_name,
    au.first_name || ' ' || au.last_name AS task_created_by, t.id AS task_id, t.is_complete,
    DATE(qt.preferred_time) AS preferred_time, DATE(qt.qualified_date) AS qualified_date, qt.has_preferred_time,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.created_at) AS task_created,
    -- Generate valid dates excluding Sundays (DOW 0)
    CASE 
        WHEN EXTRACT(DOW FROM qt.preferred_time) != 0 THEN qt.preferred_time::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '1 day') != 0 THEN (qt.preferred_time + INTERVAL '1 day')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '2 days') != 0 THEN (qt.preferred_time + INTERVAL '2 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '3 days') != 0 THEN (qt.preferred_time + INTERVAL '3 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '4 days') != 0 THEN (qt.preferred_time + INTERVAL '4 days')::DATE
        ELSE NULL
    END AS preferred_1,
    CASE 
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '1 day') != 0 THEN (qt.preferred_time + INTERVAL '1 day')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '2 days') != 0 THEN (qt.preferred_time + INTERVAL '2 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '3 days') != 0 THEN (qt.preferred_time + INTERVAL '3 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '4 days') != 0 THEN (qt.preferred_time + INTERVAL '4 days')::DATE
        ELSE NULL
    END AS preferred_2,
    CASE 
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '2 days') != 0 THEN (qt.preferred_time + INTERVAL '2 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '3 days') != 0 THEN (qt.preferred_time + INTERVAL '3 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '4 days') != 0 THEN (qt.preferred_time + INTERVAL '4 days')::DATE
        ELSE NULL
    END AS preferred_3,
    CASE 
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '3 days') != 0 THEN (qt.preferred_time + INTERVAL '3 days')::DATE
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '4 days') != 0 THEN (qt.preferred_time + INTERVAL '4 days')::DATE
        ELSE NULL
    END AS preferred_4,
    CASE 
        WHEN EXTRACT(DOW FROM qt.preferred_time + INTERVAL '4 days') != 0 THEN (qt.preferred_time + INTERVAL '4 days')::DATE
        ELSE NULL
    END AS preferred_5,
    CASE 
        WHEN preferred_1::DATE < CURRENT_DATE AND task_created::DATE != preferred_1::DATE THEN 'Flagged: Task not created on preferred date 1'
        WHEN preferred_2::DATE < CURRENT_DATE AND task_created::DATE != preferred_2::DATE THEN 'Flagged: Task not created on preferred date 2'
        WHEN preferred_3::DATE < CURRENT_DATE AND task_created::DATE != preferred_3::DATE THEN 'Flagged: Task not created on preferred date 3'
        WHEN preferred_4::DATE < CURRENT_DATE AND task_created::DATE != preferred_4::DATE THEN 'Flagged: Task not created on preferred date 4'
        WHEN preferred_5::DATE < CURRENT_DATE AND task_created::DATE != preferred_5::DATE THEN 'Flagged: Task not created on preferred date 5'
        ELSE 'Tasks created properly'
    END AS calls_made,
    lt.name
FROM unischolarz.crm_task t
LEFT JOIN unischolarz.crm_agentconfig ag ON ag.agent_id = t.created_by_id
LEFT JOIN unischolarz.accounts_user au ON au.id = ag.agent_id
LEFT JOIN unischolarz.portfolio_branch pb ON ag.branch_id = pb.id
LEFT JOIN unischolarz.crm_leadinfo l ON l.lead_id = t.lead_id
LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = l.lead_id
LEFT JOIN unischolarz.unischolarz_site_unilead_tags ut ON ut.unilead_id = ul.id
LEFT JOIN unischolarz.unischolarz_site_leadtag lt ON lt.id = ut.leadtag_id
LEFT JOIN Qualified_time qt ON qt.lead_id = t.lead_id
WHERE 
    l.status = 'QUALIFIED' and ag.is_counsellor = 1
    AND CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.created_at) >= qt.preferred_time AND t.lead_id NOT IN (
        SELECT DISTINCT ut.unilead_id
        FROM unischolarz.unischolarz_site_unilead_tags ut
        JOIN unischolarz.unischolarz_site_leadtag lt ON lt.id = ut.leadtag_id
        WHERE lt.name = 'Disqualified'
    )
ORDER BY t.lead_id, t.created_at
"""

SQL_QUERY_DELAYED_TASK = """
SELECT
    t.lead_id, t.summary, au.first_name || ' ' || au.last_name AS task_created_by,
    t.created_by_id, t.id AS task_id, t.is_complete,
    t.created_at AT TIME ZONE 'UTC+5:30' AS created_at,
    t.remind_at AT TIME ZONE 'UTC+5:30' AS remind_at_timestamp,
    DATE(t.remind_at AT TIME ZONE 'UTC+5:30') AS remind_at,
    DATE(t.updated_at AT TIME ZONE 'UTC+5:30') AS updated_date,
    t.updated_at AT TIME ZONE 'UTC+5:30' AS updated_at,
    LAG(t.created_at AT TIME ZONE 'UTC+5:30') OVER (PARTITION BY t.lead_id, au.first_name || ' ' || au.last_name ORDER BY t.created_at AT TIME ZONE 'UTC+5:30') AS flag,
    pb.name AS branch_name, d.dest_country, d.hybrid_intake, d.cur_status,
    last_cycle_qualified_date AS qualified_date, DATE(first_cycle_cnslr_contacted_date) AS cnslr_contacted_date,
    EXTRACT(EPOCH FROM (t.updated_at AT TIME ZONE 'UTC+5:30' - t.remind_at AT TIME ZONE 'UTC+5:30')) / 3600 AS remind_update_diff,
    CASE 
        WHEN EXTRACT(DOW FROM t.remind_at AT TIME ZONE 'UTC+5:30') = 6 THEN -- Remind_at is on Saturday
            CASE 
                WHEN EXTRACT(EPOCH FROM (t.updated_at AT TIME ZONE 'UTC+5:30' - t.remind_at AT TIME ZONE 'UTC+5:30')) / 3600 <= 48 THEN 'Completed on time'
                ELSE 'Not completed on time'
            END
        ELSE -- Remind_at is on other days
            CASE 
                WHEN EXTRACT(EPOCH FROM (t.updated_at AT TIME ZONE 'UTC+5:30' - t.remind_at AT TIME ZONE 'UTC+5:30')) / 3600 <= 24 THEN 'Completed on time'
                ELSE 'Not completed on time'
            END
    END AS task_completion_status
FROM unischolarz.crm_task t
LEFT JOIN unischolarz.crm_agentconfig ag ON ag.agent_id = t.created_by_id
LEFT JOIN unischolarz.accounts_user au ON au.id = ag.agent_id
LEFT JOIN unischolarz.portfolio_branch pb ON ag.branch_id = pb.id
LEFT JOIN us_standard1 d ON t.lead_id = d.lead_id
WHERE 
    ag.is_counsellor = 1
    AND t.created_at AT TIME ZONE 'UTC+5:30' >= '2025-03-01 18:30'
    AND t.is_deleted = 0
    AND t.created_at AT TIME ZONE 'UTC+5:30' IS NOT NULL
    AND t.created_by_system = 0
    AND DATE(t.remind_at AT TIME ZONE 'UTC+5:30') >= DATE(last_cycle_qualified_date)
    AND d.cur_status NOT IN ('LOST', 'ENROLLED', 'VISA RECEIVED')
"""

SQL_QUERY_NO_ACTIVE_TASKS = """
SELECT *,
        (tasks-completed) AS active
FROM
    (SELECT t.lead_id, s.cur_status, s.hybrid_intake, s.dest_country, s.hybrid_branch,
            counsellor_agent, count(t.id) AS tasks, sum(t.is_complete) AS completed
    FROM unischolarz.crm_task t
    LEFT JOIN us_standard1 s ON s.lead_id=t.lead_id
    AND t.created_by_id=s.counsellor_id
    WHERE t.is_deleted=0
        AND s.cur_status NOT IN ('LOST', 'ENROLLED', 'VISA RECEIVED', 'QUALIFIED', 'CREATED','CONTACTED')
        AND s.qualified_date >0 AND t.lead_id != 392420
    GROUP BY 1, 2, 3, 4, 5, 6)
"""

SQL_QUERY_AGEING_LEADS = """
SELECT s.lead_id, s.qualified_date, s.dest_country, s.hybrid_intake,
    au.first_name || ' ' || au.last_name AS counsellor_name,
    pb.name AS branch_name, s.cur_status, s.ageing_from_cur_status_days,
    CASE
        WHEN s.ageing_from_cur_status_days < 0 THEN 'Invalid Ageing'
        WHEN s.ageing_from_cur_status_days BETWEEN 0 AND 11 THEN '1. 0 to 11 days'
        WHEN s.ageing_from_cur_status_days BETWEEN 12 AND 30 THEN '2. 12 to 30 days'
        WHEN s.ageing_from_cur_status_days BETWEEN 31 AND 60 THEN '3. 31 to 60 days'
        WHEN s.ageing_from_cur_status_days BETWEEN 61 AND 100 THEN '4. 61 to 100 days'
        ELSE '5. > 100 days'
    END AS ageing_bucket,
    CASE
        WHEN s.ageing_from_cur_status_days < 0 THEN 'Invalid Ageing'
        WHEN s.ageing_from_cur_status_days BETWEEN 0 AND 1 THEN '1. 0 to 1 days'
        WHEN s.ageing_from_cur_status_days BETWEEN 2 AND 3 THEN '2. 2 to 3 days'
        ELSE '3. > 3 days'
    END AS ageing_bucket_QL
FROM us_standard1 s
LEFT JOIN unischolarz.crm_agentconfig ag ON ag.agent_id = s.counsellor_id
LEFT JOIN unischolarz.accounts_user au ON au.id = ag.agent_id
LEFT JOIN unischolarz.portfolio_branch pb ON ag.branch_id = pb.id
WHERE s.cur_status NOT IN ('LOST', 'ENROLLED', 'DEPOSIT', 'VISA APPLIED', 'VISA RECEIVED')
  AND qualified_date >0 AND is_repeated=0 AND au.is_active = 1
"""

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

def execute_query(conn, query: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
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

def process_sales_cadence(df: pd.DataFrame):
    filtered_df = df[
        df['calls_made'].notnull() & 
        (df['calls_made'] != 'Tasks created properly')
    ].copy()
    
    sales_cad = filtered_df.groupby('branch_name')['lead_id'].nunique().reset_index(name='lead_count')
    sales_cad1 = filtered_df.groupby(['branch_name', 'task_created_by'])['lead_id'].nunique().reset_index(name='lead_count')
    sales_cad2 = filtered_df[['lead_id', 'task_created_by', 'branch_name']].drop_duplicates(subset='lead_id')
    
    logger.info(f"Sales Cadence: {len(sales_cad)} branches flagged.")
    return sales_cad, sales_cad1, sales_cad2

def process_delayed_tasks(df_1: pd.DataFrame):
    df_1['remind_at'] = pd.to_datetime(df_1['remind_at'], errors='coerce').dt.date
    
    yesterday = date.today() - timedelta(days=1)
    
    filtered_df_1 = df_1[(df_1['remind_at'] == yesterday) & (df_1['task_completion_status'] == 'Not completed on time')].copy()

    del_task = filtered_df_1.groupby('branch_name')['lead_id'].nunique().reset_index(name='lead_count')
    del_task1 = filtered_df_1.groupby(['branch_name', 'task_created_by'])['lead_id'].nunique().reset_index(name='lead_count')
    del_task2 = filtered_df_1[['lead_id', 'task_created_by', 'branch_name']].drop_duplicates(subset='lead_id')

    logger.info(f"Delayed Tasks: {len(del_task)} branches flagged.")
    return del_task, del_task1, del_task2

def process_no_active_tasks(df_2: pd.DataFrame):
    filtered_df_2 = df_2[(df_2['active'] == 0)].copy()

    no_act = filtered_df_2.groupby('hybrid_branch')['lead_id'].nunique().reset_index(name='lead_count')
    no_act1 = filtered_df_2.groupby(['hybrid_branch', 'counsellor_agent'])['lead_id'].nunique().reset_index(name='lead_count')
    no_act2 = filtered_df_2[['lead_id', 'counsellor_agent', 'hybrid_branch']].drop_duplicates(subset='lead_id')
    
    logger.info(f"No Active Tasks: {len(no_act)} branches flagged.")
    return no_act, no_act1, no_act2

def process_aging_leads(df_3: pd.DataFrame):
    filtered_df_3 = df_3[df_3['cur_status'] == 'PROCESSING'].copy()
    grouped = (
        filtered_df_3
        .groupby(['branch_name', 'ageing_bucket'])['lead_id']
        .nunique().reset_index(name='lead_count'))
    pivot_df = (
        grouped
        .set_index(['branch_name', 'ageing_bucket'])['lead_count']
        .unstack(fill_value=0)
        .astype(int).reset_index())
    
    grouped1 = (
        filtered_df_3
        .groupby(['counsellor_name', 'ageing_bucket'])['lead_id']
        .nunique().reset_index(name='lead_count'))
    pivot_df1 = (
        grouped1
        .set_index(['counsellor_name', 'ageing_bucket'])['lead_count']
        .unstack(fill_value=0)
        .astype(int).reset_index())
  
    pivot_df2 = filtered_df_3[['lead_id', 'counsellor_name', 'branch_name', 'qualified_date']].drop_duplicates(subset='lead_id')
    
    logger.info("Aging Leads: Processing leads pivoted successfully.")

    filtered_df_4 = df_3[
        (df_3['cur_status'] == 'OPPORTUNITY') & 
        (df_3['hybrid_intake'].isin(['Fall,2025', 'Spring,2026']))
    ].copy()

    opp = filtered_df_4.groupby('branch_name')['lead_id'].nunique().reset_index(name='lead_count')
    opp1 = filtered_df_4.groupby('counsellor_name')['lead_id'].nunique().reset_index(name='lead_count')
    opp2 = filtered_df_4[['lead_id', 'counsellor_name', 'branch_name', 'qualified_date']].drop_duplicates(subset='lead_id')
    
    logger.info(f"Opportunity Leads: {len(opp)} branches flagged for S26/F25.")
    
    return pivot_df, pivot_df1, pivot_df2, opp, opp1, opp2

def df_to_inline_html(df, width='30%'):
    html = f'<table style="border-collapse: collapse; width: {width}; font-family: Arial, sans-serif; font-size: 10px;">'
    html += '<thead><tr>'
    for col in df.columns:
        html += f'<th style="border: 1px solid #ccc; padding: 5px 7px; background-color: #f2f2f2; text-align: left;">{col}</th>'
    html += '</tr></thead><tbody>'
    for _, row in df.iterrows():
        html += '<tr>'
        for cell in row:
            # Ensure date objects are correctly formatted as strings
            cell_str = str(cell.strftime('%Y-%m-%d') if isinstance(cell, date) else cell)
            html += f'<td style="border: 1px solid #ccc; padding: 6px 8px; text-align: left;">{cell_str}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html

def create_excel_attachment(data_dict: dict) -> io.BytesIO:
    excel_bytes_io = io.BytesIO()

    try:
        with pd.ExcelWriter(excel_bytes_io, engine='xlsxwriter') as writer:
            
            data_dict['del_task1'].to_excel(writer, sheet_name='leads_with_24+_hours', startcol=0, index=False)
            data_dict['del_task2'].to_excel(writer, sheet_name='leads_with_24+_hours', startcol=5, index=False)
            
            data_dict['sales_cad1'].to_excel(writer, sheet_name='sales_cadence', startcol=0, index=False)
            data_dict['sales_cad2'].to_excel(writer, sheet_name='sales_cadence', startcol=5, index=False)
            
            data_dict['no_act1'].to_excel(writer, sheet_name='Active Leads with no tasks', startcol=0, index=False)
            data_dict['no_act2'].to_excel(writer, sheet_name='Active Leads with no tasks', startcol=5, index=False)
            
            data_dict['pivot_df1'].to_excel(writer, sheet_name='Leads still in processing', startcol=0, index=False)
            data_dict['pivot_df2'].to_excel(writer, sheet_name='Leads still in processing', startcol=7, index=False)
            
            data_dict['opp1'].to_excel(writer, sheet_name='S 26 & F 25 leads in Opp', startcol=0, index=False)
            data_dict['opp2'].to_excel(writer, sheet_name='S 26 & F 25 leads in Opp', startcol=7, index=False)

        excel_bytes_io.seek(0)
        logger.info("Excel attachment created successfully in memory.")
        return excel_bytes_io
    except Exception as e:
        logger.error(f"Failed to create Excel file: {e}")
        raise

def send_red_flag_email(subject, body, recipient_emails, excel_stream):
    
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
        excel_attachment.add_header('Content-Disposition', 'attachment', filename=f'US_Counsellor_Red_Flag_{formatted_date}.xlsx')
        message.attach(excel_attachment)

        server.sendmail(SENDER_EMAIL, recipient_emails, message.as_string())
        server.quit()
        logger.info(f"Email sent successfully to {len(recipient_emails)} recipients.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise

def run_counsellor_red_flag_etl():
    conn = None
    
    try:
        conn = get_redshift_connection()
        
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        day_suffix = lambda d: "th" if 11<=d<=13 else {1:"st",2:"nd",3:"rd"}.get(d%10, "th")
        formatted_date = f"{yesterday.day}{day_suffix(yesterday.day)} {yesterday.strftime('%B %Y')}"
        
        df_sales_cadence = execute_query(conn, SQL_QUERY_SALES_CADENCE)
        df_delayed_task = execute_query(conn, SQL_QUERY_DELAYED_TASK)
        df_no_task = execute_query(conn, SQL_QUERY_NO_ACTIVE_TASKS)
        df_aging = execute_query(conn, SQL_QUERY_AGEING_LEADS)
        
        sales_cad, sales_cad1, sales_cad2 = process_sales_cadence(df_sales_cadence)
        del_task, del_task1, del_task2 = process_delayed_tasks(df_delayed_task)
        no_act, no_act1, no_act2 = process_no_active_tasks(df_no_task)
        pivot_df, pivot_df1, pivot_df2, opp, opp1, opp2 = process_aging_leads(df_aging)

        sales_table_html = df_to_inline_html(sales_cad)
        del_task_html = df_to_inline_html(del_task)
        no_act_html = df_to_inline_html(no_act)
        pivot_df_html = df_to_inline_html(pivot_df, width='50%')
        opp_html = df_to_inline_html(opp)
        
        data_for_excel = {
            'del_task1': del_task1, 'del_task2': del_task2,
            'sales_cad1': sales_cad1, 'sales_cad2': sales_cad2,
            'no_act1': no_act1, 'no_act2': no_act2,
            'pivot_df1': pivot_df1, 'pivot_df2': pivot_df2,
            'opp1': opp1, 'opp2': opp2
        }
        excel_bytes_io = create_excel_attachment(data_for_excel)
        
        subject = f"UniScholars Red-Flag for Counsellors - {formatted_date}"
        body = f"""
<html>
  <body>
    <p>Hi Team,<br><br>
    Please find Branch wise Red-Flag for Counsellors and refer to the attached Excel sheet for detailed Counsellor Red Flag.
    </p>
    
  <h3>1. Leads with 24+ hours delay in completing task</h3>
  <p>For leads in active funnel (excluding LOST, ENROLLED, VISA RECEIVED).</p>
    {del_task_html}
  
  <h3>2. Violation of Sales Cadence</h3>
  <p>Where five days consecutive calls were not picked after Qualification of Lead.</p>
    {sales_table_html}
  
  <h3>3. Active Leads with no tasks</h3>
  <p>Excluding LOST, ENROLLED, VISA RECEIVED. Leads with no active tasks might get lost after generation of this report.</p>
    {no_act_html}
  
  <h3>4. Number of days since leads are still in Processing</h3>
    {pivot_df_html}
  
  <h3>5. Spring 26 and Fall 25 leads still in Opportunity</h3>
  <p>These should ideally be either Lost or in Processing.</p>
    {opp_html}

  <p>You can find red flag details for individual counsellors in the attached Excel report.</p>

    <p>Best regards,<br>Reporting Team</p>
  </body>
</html>
"""

        send_red_flag_email(subject, body, RECIPIENT_EMAILS, excel_bytes_io)
        
        logger.info("🏁 Counsellor Red Flag Report ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Red Flag Report ETL.", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
