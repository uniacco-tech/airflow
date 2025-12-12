import logging
import redshift_connector
from typing import Optional

# --- Configuration ---
REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DATABASE = "dev"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y" # WARNING: Store credentials securely, e.g., in Airflow Connections

LOG_FILE = "log_olap.txt"

# --- Logging Setup ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"  # append mode
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Redshift Connection Management ---

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

def run_query(conn, cur, query: str, step_name: str):
    """Executes a single query with logging and transaction handling."""
    try:
        logger.info(f"▶️ {step_name} ...")
        cur.execute(query)
        conn.commit()
        logger.info(f"✅ {step_name} completed and committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ {step_name} failed: {e}")
        raise

# --- ETL Step Functions ---

def update_uc_lead_2(conn, cur):
    """Handles ETL for UC_Lead_2 table."""
    step_name_prefix = "UC_Lead_2"
    
    run_query(conn, cur, "truncate table public.UC_Lead_2", f"{step_name_prefix}: Truncate")

    query_uc_lead_2 = """
    insert into public.UC_Lead_2
    SELECT a.id AS Lead_ID,
            a.name,
            CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.created_at) lead_timestamp,
            date(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.created_at)) AS Lead_Date,
            date(d.LOST_Date) AS Lost_Day,
            CASE
                WHEN LOWER(a.country) IN ('usa', 'united states', 'united-states', 'us') THEN 'USA'
                WHEN LOWER(a.country) IN ('uk', 'united kingdom', 'united-kingdom') THEN 'UK'
                WHEN LOWER(a.country) = 'ireland' THEN 'Ireland'
                WHEN LOWER(a.country) = 'canada' THEN 'Canada'
                WHEN LOWER(a.country) = 'australia' THEN 'Australia'
                WHEN LOWER(a.country) IN ('new zealand', 'new-zealand') THEN 'NZ'
                WHEN LOWER(a.country) IN ('germany', 'switzerland', 'italy', 'poland', 'netherlands', 'denmark', 'austria', 'finland', 'france', 'spain', 'portugal', 'hungary', 'swizterland', 'france, poland, latvia') THEN 'EUR'
                WHEN LOWER(a.country) = 'india' THEN 'India'
                ELSE 'ROW'
            END AS Country,
            a.verified_email,
            a.verified_phone,
            upper(a.source) AS SOURCE,
            upper(a.university) AS university,
            upper(a.utm_campaign) AS utm_campaign,
            upper(a.utm_content) AS utm_content,
            upper(a.utm_medium) AS utm_medium,
            upper(a.utm_term) AS utm_term,
            CASE
                WHEN a.utm_medium IN ('cpc', 'ppc', 'nestpick', 'paid', 'ads') THEN 'Paid'
                ELSE 'Unpaid'
            END AS Channel,
            upper(a.utm_name) AS utm_name,
            CASE
                WHEN (a.utm_source = 'google' AND a.utm_medium = 'cpc') THEN a.utm_source || '-' || a.utm_medium
                ELSE a.utm_source
            END AS utm,
            upper(a.utm_source) AS utm_source,
            CASE
                WHEN upper(a.utm_source) = 'PARTNER' THEN 'Partner Lead'
                WHEN upper(a.utm_source) IN ('UNIACCO', 'UNISCHOLARS', 'UNISCHOLARS-HYDERABAD', 'UNISCHOLARZ') THEN 'Cross_Sales'
                ELSE 'Digital_Marketing'
            END AS Lead_Source,
            upper(a.city) AS city,
            upper(a.permanent_city) AS permanent_city,
            upper(a.admission_status) AS admission_status,
            upper(a.university_join_time) AS university_join_time,
            CASE
                WHEN a.loan_amount BETWEEN 0 AND 2000000 THEN '0-20 L'
                WHEN a.loan_amount BETWEEN 2000001 AND 4000000 THEN '20-40 L'
                WHEN a.loan_amount BETWEEN 4000001 AND 6000000 THEN '40-60 L'
                WHEN a.loan_amount BETWEEN 6000001 AND 8000000 THEN '60-80 L'
                WHEN a.loan_amount BETWEEN 8000001 AND 10000000 THEN '80-100 L'
                WHEN a.loan_amount > 10000000 THEN '> 1 cr'
            END AS Loan_Amount,
            upper(a.source_country) AS source_country,
            a.age,
            upper(a.loan_type) AS loan_type,
            upper(a.client_state) AS client_state,
            b.status,
            upper(b.lost_reason) AS lost_reason,
            date(b.updated_at) AS last_Update,
            b.agent_id,
            b.call_in,
            b.call_out,
            b.sms_in,
            b.sms_out,
            b.is_repeated,
            b.sanction_agent_id,
            date(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', b.sanction_agent_assigned_at)) AS Sanction_Agant_date,
            b.is_renewed,
            b.notes_count,
            b.tasks_count,
            b.am_agent_id,
            date(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', b.am_agent_assigned_at)) AS AM_Assigned_time,
            c.first_call_time,
            c.last_call_time,
            Total_call_Count,
            Answered_Call_Count,
            CASE
                WHEN b.status = 'LOST' AND date(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.created_at)) = date(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', d.LOST_Date)) THEN 1
                ELSE 0
            END AS LOST_same_day,
            CASE
                WHEN b.status = 'LOST' AND (EXTRACT(EPOCH FROM d.LOST_Date-CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.created_at))/3600)/24 <= 7 THEN 1
                ELSE 0
            END AS LOST_within_7_days,
            CASE
                WHEN b.status = 'LOST' THEN 1
                ELSE 0
            END AS LOST_till_date,
            CASE
                WHEN b.status IN ('CREATED', 'LOGGED_IN', 'SANCTIONED', 'PF_CONFIRMED') THEN 1
                ELSE 0
            END AS Leads_in_pipeline,
            CASE
                WHEN a.utm_source='partner' THEN 1
                ELSE 0
            END AS Partner_Leads,
            CASE
                WHEN b.lost_reason IN ('not-applied', 'indian-university', 'junk-lead', 'low-loan-amount') THEN 0
                WHEN a.admission_status IN ('not-applied', 'NOT APPLIED') THEN 0
                ELSE 1
            END AS Caterable_Leads,
            aa.name AS Agent_NAme,
            ab.name AS sanction_agent_Name,
            ac.name AS AM_Agent_Name,
            hli.CONTACTED_date,
            hli.PROCESSING_date,
            hli.IMPORTANT_date,
            ln.Loggin_Date,
            a.referrer,
            hli.min_opportunity_date,
            hli.max_opportunity_date,
            e.Status_before_lost
    FROM unicredsnew.site_forms_lead AS a
    LEFT JOIN unicredsnew.crm_leadinfo b ON a.id = b.lead_id
    LEFT JOIN
        (SELECT lead_id AS lead_id,
                max(CASE WHEN rnk1 = 1 THEN created_at END) AS first_call_time,
                max(CASE WHEN rnk2 = 1 THEN created_at END) AS last_call_time,
                count(*) AS Total_call_Count,
                sum(CASE WHEN Status = 'COMPLETED' THEN 1 ELSE 0 END) AS Answered_Call_Count
        FROM
            (SELECT *,
                    row_number() over(PARTITION BY lead_id ORDER BY created_at DESC) AS rnk1,
                    row_number() over(PARTITION BY lead_id ORDER BY created_at) AS rnk2
            FROM unicredsnew.crm_leadinteractionlog
            WHERE date(created_at) >= '2023-01-01' AND lead_id IS NOT NULL)
        GROUP BY 1) AS c ON a.id = c.lead_id
    LEFT JOIN
        (select lead_id, max(history_date + interval '330' minute) lost_date from
        (select status, history_id, history_date, lead_id, 
        lag(status) over(partition by lead_id order by history_id) next_status
        from unicredsnew.crm_historicalleadinfo
        WHERE history_date > '2022-01-30 18:30')
        where (status <> next_status or next_status is null)
        group by 1) d ON a.id = d.lead_id
    LEFT JOIN (
        SELECT b.lead_id, max(CASE WHEN li.status = 'LOST' AND b.Desc_rank = 2 THEN b.status ELSE NULL END) AS Status_before_lost
        FROM (
            SELECT *, rank() OVER (PARTITION BY a.lead_id ORDER BY a.history_id DESC) AS Desc_rank
            FROM (
                SELECT lead_id, history_id, history_date, status,
                    LAG(status, 1) OVER (PARTITION BY lead_id ORDER BY history_id) AS previous_status
                FROM unicredsnew.crm_historicalleadinfo ) a
            WHERE
                (status <> previous_status OR previous_status IS NULL)) b
        JOIN unicredsnew.crm_leadinfo li ON li.lead_id = b.lead_id
        GROUP BY b.lead_id) AS e ON e.lead_id = a.id
    LEFT JOIN unicredsnew.site_forms_lead AS aa ON b.agent_id = aa.id
    LEFT JOIN unicredsnew.site_forms_lead AS ab ON b.sanction_agent_id = ab.id
    LEFT JOIN unicredsnew.site_forms_lead AS ac ON b.am_agent_id = ac.id
    LEFT JOIN
        (SELECT Lead_id,
                MIN(CASE WHEN status IN ('CONTACTED','PROCESSING','IMPORTANT') THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) END) AS CONTACTED_date,
                MIN(CASE WHEN status IN ('PROCESSING','IMPORTANT') THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) END) AS PROCESSING_date,
                MIN(CASE WHEN status ='IMPORTANT' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) END) AS IMPORTANT_date,
                MIN(CASE WHEN status ='OPPORTUNITY' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) END) AS min_opportunity_date,
                MIN(CASE WHEN status ='OPPORTUNITY' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) END) AS max_opportunity_date    
        FROM unicredsnew.crm_historicalleadinfo
        WHERE is_repeated = 0 AND status IN ('CONTACTED', 'PROCESSING', 'IMPORTANT','OPPORTUNITY')
        GROUP BY Lead_id) hli ON hli.lead_id=c.lead_id
    LEFT JOIN
        (SELECT l.lead_id,
                min(coalesce(date(h.logged_in_date),date(lp.logged_in_at))) AS Loggin_Date
        FROM unicredsnew.crm_loanapplication l
        LEFT JOIN unicredsnew.crm_loanprocessingentity lp ON lp.lead_id=l.lead_id AND lp.entity_id=l.loan_partner_id
        LEFT JOIN
            (SELECT lead_id, loan_partner_id, min(date(history_date)) AS logged_in_date
            FROM unicredsnew.crm_historicalloanapplication
            WHERE status='LOGGED_IN' AND date(history_date) >='2024-12-10'
            GROUP BY lead_id, loan_partner_id) h ON h.lead_id=l.lead_id AND h.loan_partner_id=l.loan_partner_id
        WHERE coalesce(date(h.logged_in_date),date(lp.logged_in_at)) > 0
        GROUP BY l.lead_id) ln ON ln.lead_id=c.lead_id
    WHERE date(a.created_at) >= '2022-01-01'
    """
    run_query(conn, cur, query_uc_lead_2, f"{step_name_prefix}: Insert Data")

def update_uc_lead_3(conn, cur):
    """Handles ETL for UC_Lead_3 table."""
    step_name_prefix = "UC_Lead_3"
    run_query(conn, cur, "truncate table public.UC_Lead_3", f"{step_name_prefix}: Truncate")

    query_uc_lead_3 = """
    insert into public.UC_Lead_3
    SELECT sf.id AS Lead_id,
            date(cpi.logged_in) as LOGGED_IN_date,
            date(c.SANCTIONED_date) AS SANCTIONED_date,
            date(c.DISBURSED_date) as DISBURSED_date,
            date(c.PF_CONFIRMED_date) as PF_CONFIRMED_date,
            cpi.logged_in as Logged_in_timestamp,
            c.SANCTIONED_date AS SANCTIONED_timestamp,
            c.DISBURSED_date as disbursed_timestamp,
            c.PF_CONFIRMED_date as pf_confirmed_timestamp
    FROM unicredsnew.site_forms_lead sf
    LEFT JOIN
        (SELECT lead_id, date_of_booking, loan_partner_id
        FROM
            (SELECT lead_id, date_of_booking, loan_partner_id,
                    ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY date_of_booking DESC) AS rnk
            FROM unicredsnew.crm_conversionform)
        WHERE rnk = 1) cf ON cf.lead_id=sf.id
    LEFT JOIN unicredsnew.crm_leadinfo l ON l.lead_id=sf.id
    LEFT JOIN
        ((SELECT pe.lead_id,
        (pe.min_logged_in AT TIME ZONE 'utc' AT TIME ZONE 'asia/kolkata') AS logged_in
    FROM
        (SELECT l.lead_id,
                date(min(coalesce(lp.created_at,l.created_at))) AS created_at,
                (min(coalesce(h.logged_in_date,lp.logged_in_at))) AS min_logged_in,
                min(h.loan_partner_id) as loan_partner_id
        FROM unicredsnew.crm_loanapplication l
        LEFT JOIN unicredsnew.crm_loanprocessingentity lp ON lp.lead_id=l.lead_id AND lp.entity_id=l.loan_partner_id
        LEFT JOIN
            (SELECT lead_id, loan_partner_id, min((history_date)) AS logged_in_date
            FROM unicredsnew.crm_historicalloanapplication
            WHERE status='LOGGED_IN' AND date(history_date) >='2024-12-10'
            GROUP BY lead_id, loan_partner_id) h ON h.lead_id=l.lead_id AND h.loan_partner_id=l.loan_partner_id
        WHERE coalesce(date(h.logged_in_date),date(lp.logged_in_at)) > 0
        GROUP BY l.lead_id) pe
    LEFT JOIN unicredsnew.site_forms_lead s ON s.id=pe.lead_id
    LEFT JOIN unicredsnew.crm_leadinfo li ON pe.lead_id = li.lead_id
    JOIN unicredsnew.site_forms_lead sf ON li.agent_id = sf.id
    LEFT JOIN unicredsnew.portfolio_loanpartner lp ON pe.loan_partner_id = lp.id
    LEFT JOIN unicredsnew.site_forms_lead sfl ON sfl.id = li.sanction_agent_id)) cpi ON cpi.lead_id = l.lead_id
    LEFT JOIN unicredsnew.portfolio_loanpartner clp ON clp.id = cf.loan_partner_id
    LEFT JOIN
        (SELECT hl.lead_id,
                min(CASE WHEN hl.status='LOGGED_IN' THEN (CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hl.history_date)) ELSE NULL END) AS LOGGED_IN_date,
                min(CASE WHEN hl.status='SANCTIONED' THEN (CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hl.history_date)) ELSE NULL END) AS SANCTIONED_date,
                min(CASE WHEN hl.status='DISBURSED' THEN (CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hl.history_date)) ELSE NULL END) AS DISBURSED_date,
                min(CASE WHEN hl.status='PF_CONFIRMED' THEN (CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hl.history_date)) ELSE NULL END) AS PF_CONFIRMED_date
        FROM unicredsnew.crm_historicalleadinfo hl
        WHERE hl.Status IN ('LOGGED_IN', 'SANCTIONED', 'PF_CONFIRMED', 'DISBURSED') AND hl.is_repeated=0
        GROUP BY hl.lead_id) c ON c.lead_id=sf.id
    WHERE l.is_repeated=0
        AND l.agent_id IS NOT NULL
        AND (cf.date_of_booking >= '2022-01-01'
            OR c.PF_CONFIRMED_date >= '2022-01-01'
            OR c.DISBURSED_date >= '2022-01-01'
            OR c.LOGGED_IN_date >= '2022-01-01'
            OR cpi.logged_in >= '2023-01-01')
    """
    run_query(conn, cur, query_uc_lead_3, f"{step_name_prefix}: Insert Data")

def update_uc_loan_partner(conn, cur):
    """Handles ETL for uc_loan_partner table."""
    step_name_prefix = "uc_loan_partner"
    run_query(conn, cur, "truncate table public.uc_loan_partner", f"{step_name_prefix}: Truncate")

    query_uc_loan_partner = """
    insert into public.uc_loan_partner
    select a.ID, a.status as Partner_Status, a.entity_id, a.lead_id, a.created_at, 
            a.process_end_at, a.rejection_reason, a.updated_at, b.name as Lender_Name,b.kind,
            c.name as Name, c.country as country, c.source as source, utm_campaign, utm_content, 
            utm_medium, channel, utm_name, utm, utm_source, admission_status, loan_amount, loan_type,
            c.status, c.lost_reason, is_repeated, is_renewed,partner_leads, caterable_leads, agent_name, 
            sanction_agent_name, am_agent_name, 
            case when a.status in ('LOGGED_IN', 'POST_LOGGED_IN_REJECTED', 'POST_SANCTION_REJECTED', 'SANCTIONED') then 1 else 0 end as Logged_in_Flag,
            case when a.status in ('POST_LOGGED_IN_REJECTED') then 1 else 0 end as POST_LOGGED_IN_REJECTED_Flag,
            case when a.status in ('PRE_LOGGED_IN_REJECTED') then 1 else 0 end as PRE_LOGGED_IN_REJECTED_Flag,
            case when a.status in ('SANCTIONED', 'POST_SANCTION_REJECTED') then 1 else 0 end as SANCTIONED_Flag,
            case when a.status in ('SANCTIONED') then 1 else 0 end as True_SANCTIONED,
            case when a.status in ('POST_SANCTION_REJECTED') then 1 else 0 end as POST_SANCTION_REJECTED_Flag,
            case when a.process_end_at is null then 1 else 0 end as Open_Account
            from unicredsnew.crm_loanprocessingentity as a
            left join unicredsnew.portfolio_loanpartner as b on a.entity_id = b.id 
            left join public.UC_Lead_2 as c on a.lead_id = c.lead_id 
            where a.created_at >= '2023-01-01'
    """
    run_query(conn, cur, query_uc_loan_partner, f"{step_name_prefix}: Insert Data")

def update_uc_lender(conn, cur):
    """Handles ETL for uc_lender_standard_table."""
    step_name_prefix = "uc_lender_standard_table"
    run_query(conn, cur, "truncate table public.uc_lender_standard_table", f"{step_name_prefix}: Truncate")

    query_uc_lender = """
    insert into uc_lender_standard_table
    SELECT a.id,
            a.lead_id,
            a.application_id,
            CASE
                WHEN a.status = 'YET_TO_CONNECT' THEN 'Yet_to_Connect'
                WHEN a.status IN ('UNDER_DISCUSSION', 'NO_PRODUCT_OR_NOT_ELIGIBLE', 'ALREADY_PROCESSED', 'FUTURE_DATE') THEN 'Contacted'
                WHEN a.status = 'NOT_RESPONDING' THEN 'Not_Responding'
                WHEN a.status IN ('AWAITING_DOCS', 'READY_TO_LOGIN', 'LOGGED_IN', 'SANCTIONED') THEN 'Doable'
                WHEN a.status IN ('PF_CONFIRMED', 'YES', 'PARTIALLY_PAID', 'NOT_REQUIRED') THEN 'PF_Confirmed'
                WHEN a.status = 'DISBURSEMENT' THEN 'Disbursement'
                WHEN a.status = 'REJECTED' THEN 'Rejected'
                ELSE 'Unknown'
            END AS lender_status,
            a.status,
            date(a.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'asia/kolkata') AS application_created_at,
            a.loan_amount,
            b.lead_date,
            b.country,
            b.agent_name,
            b.is_repeated,
            b.channel,
            b.utm_campaign,
            b.utm_medium,
            b.utm_source,
            b.lead_source,
            b.utm_content,
            b.status AS lead_status,
            b.admission_status,
            b.sanction_agent_name,
            b.am_agent_name,
            b.lost_reason,
            c.name,
            e.lender_id,
            e.role,
            d.name as lender_user_name
    FROM unicredsnew.crm_loanapplication a
    LEFT JOIN uc_lead_2 b ON a.lead_id = b.lead_id
    LEFT JOIN unicredsnew.portfolio_loanpartner c ON c.id = a.loan_partner_id
    LEFT JOIN unicredsnew.crm_lenderconfig e ON e.lender_id = a.lender_id
    LEFT JOIN unicredsnew.site_forms_lead d ON d.id = a.lender_id
    WHERE application_created_at >= '2025-09-01'
    """
    run_query(conn, cur, query_uc_lender, f"{step_name_prefix}: Insert Data")

# --- Main Callable Function for Airflow ---

def run_all_etl_steps():
    """
    The main function called by the Airflow PythonOperator.
    It manages the connection lifecycle for the entire DAG run.
    """
    conn = None
    cur = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        logger.info("Starting all ETL refresh steps.")

        update_uc_lead_2(conn, cur) 

        update_uc_lead_3(conn, cur)

        update_uc_loan_partner(conn, cur)
        
        update_uc_lender(conn, cur)
        
        logger.info("🏁 All refresh steps completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during ETL: {e}", exc_info=True)
        raise
    finally:
        if cur:
            cur.close()
            logger.info("Redshift cursor closed.")
        if conn:
            conn.close()
            logger.info("Redshift connection closed.")
