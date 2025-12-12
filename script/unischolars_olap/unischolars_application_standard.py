import logging
import redshift_connector
from typing import Optional

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DATABASE = "dev"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"

LOG_FILE = "log_app_olap.txt"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

def run_query(conn, cur, query: str, step_name: str):
    try:
        logger.info(f"▶️ {step_name} ...")
        cur.execute(query)
        conn.commit()
        logger.info(f"✅ {step_name} completed and committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ {step_name} failed: {e}")
        raise

def update_ucs_standard(conn, cur):
    step_name_prefix = "US_App_Standard"
    
    run_query(conn, cur, "truncate table public.us_application_standard_1", f"{step_name_prefix}: Truncate")

    query_us_app_standard = """
    insert into public.us_application_standard_1
    WITH MaxLeadID AS (
      SELECT 
          ua.id AS Application_ID,
          COALESCE(
              MAX(CASE WHEN important_date IS NOT NULL THEN ul.lead_id ELSE NULL END),
              MAX(ul.lead_id)
          ) AS Max_Lead_ID
      FROM unischolarz.application_universityapplication ua
      LEFT JOIN us_standard1 ul ON ul.student_id = ua.student_id
      WHERE ul.is_repeated = 0
      GROUP BY ua.id
    )
    
    SELECT 
          ua.id AS App_ID,
          mli.Max_Lead_ID AS Lead_ID,
          c.id AS conv_form_id,
          u.utm_source,
          u.cur_status AS Current_status,
          ua.status AS Application_status,
          CASE 
              WHEN (pd.first_name || ' ' || pd.last_name) IS NOT NULL THEN (pd.first_name || ' ' || pd.last_name)
              ELSE ul.name 
          END AS student_name,
          CAST(pd.passport_no AS VARCHAR) AS student_passport_no,
          pd.date_of_birth AS student_DOB,
          u.hybrid_branch AS Branch,
          ul.level_of_study,
          CASE
               WHEN EXTRACT(MONTH FROM ua.intake) BETWEEN 1 AND 6 THEN ('Spring''' || TO_CHAR(ua.intake, 'YY'))
               WHEN EXTRACT(MONTH FROM ua.intake) BETWEEN 7 AND 12 THEN ('Fall''' || TO_CHAR(ua.intake, 'YY'))
               ELSE 'Others'
          END AS intake_category,
          co.name AS Country,
          uv.name AS Campus,
          pr.name AS Programme,
          ucou.first_name || ' ' || ucou.last_name AS counsellor,
          uvis.first_name || ' ' || uvis.last_name AS Visa_Counselor,
          uadd.first_name || ' ' || uadd.last_name AS Admission_APT_Agent,
          ulqt.first_name || ' ' || ulqt.last_name AS LQT_Agent,
          upft.first_name || ' ' || upft.last_name AS PFT_Agent,  -- ✅ New column added
          li.is_cross_sale_lead,
          ua.intake,
          ua.defer_intake,
          ua.student_id AS Student_ID,
          TRUNC(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', ua.created_at)) AS Application_date,
          ua.shortlist_status,
          c.course_start_date,
          c.fy_tuition_fee,
          c.scholarship_amount,
          (c.fy_tuition_fee - c.scholarship_amount) AS net_payable_fees,
          ua.application_submission AS application_submission_date_apt,
          ua.rejection_reason,
          DATE(u.important_date) AS Imp_date,
          DATE(u.aip_date) AS aip_date,
          DATE(u.qualified_date) AS QL_date,
          DATE(u.offer_date) AS Offer_date,
          DATE(CONVERT_TIMEZONE('Asia/Kolkata', c.created_at)) AS deposit_date,
          DATE(enrolled_date) AS enrolled_date,
          DATE(last_cycle_lost_date) AS final_lost_date, 
          status_dates.*,
          ua.pending_documents,
          CASE 
              WHEN status_dates.Rejected_By_Agent_Date IS NOT NULL THEN 
                  DATEDIFF(day, CONVERT_TIMEZONE('Asia/Kolkata', ua.created_at), status_dates.Rejected_By_Agent_Date)
              WHEN status_dates.Submit_To_University_Date IS NOT NULL THEN 
                  DATEDIFF(day, CONVERT_TIMEZONE('Asia/Kolkata', ua.created_at), status_dates.Submit_To_University_Date)
              ELSE NULL 
          END AS TAT_Created_To_Rejected_By_Agent_OR_Submit_To_University_Date,
          CASE 
              WHEN status_dates.Rejected_By_University_Date IS NOT NULL THEN 
                  DATEDIFF(day, status_dates.Submit_To_University_Date, status_dates.Rejected_By_University_Date)
              WHEN status_dates.Offer_Received_Date IS NOT NULL THEN 
                  DATEDIFF(day, status_dates.Submit_To_University_Date, status_dates.Offer_Received_Date)
              ELSE NULL 
          END AS TAT_Submit_To_University_To_Rejected_OR_Offer,
          c.deposit_amount,
          c.deposit_date AS deposit_form_filled_date,
          c.payment_mode,
          ua.fee_received_date AS ua_fee_recieved,
          CASE WHEN ua.is_conditional = 1 THEN 1 ELSE 0 END AS is_conditional,
          ua.conditional_requirements,
          ap.partner_type,
          ap.name AS partner,
          ua.agent_type,
          ua.document_type AS document_type_required,
          ua.offer_letter,
          c.commissionable,
          c.is_change_of_agent,
          c.is_tagged,
          c.cancel_reason AS conv_cancel_reason,
          ua.cas_letter,
          ua.cas_status AS cas_status,
          ua.cas_applied_date AS cas_applied_date,
          ua.cas_received_date AS cas_received_date,
          ua.cas_rejection_reason AS cas_rejection_reason,
          DATE(u.vaip_date) AS visa_in_process_status_date,
          DATE(u.va_date) AS visa_applied_status_date,
          DATE(vr_date) AS visa_recieved_status_date,
          v.created_at AS visa_processing_started_at,
          v.updated_at AS visa_applied_date,
          v.visa_status AS visa_status,
          v.visa_refusal AS visas_refusal,
          e.flight_ticket,
          e.flight_ticket_booked,
          e.arrived_on_campus,
          e.accomodation_booked,
          e.brp,
          e.id_card,
          c_notes.last_note,
          c_notes.last_note_created_at,
          c_notes.before_last_note,
          c_notes.before_last_note_created_at,
          ua.submission_type,
          had.pending_app_received_date,
          had.approved_app_received_date
    
    FROM unischolarz.application_universityapplication ua
    JOIN MaxLeadID mli 
        ON ua.id = mli.Application_ID
    LEFT JOIN (
      SELECT 
          id,
          MIN(CASE WHEN LOWER(TRIM(pft_status)) = 'pending' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END) AS pending_app_received_date,
          MIN(CASE WHEN LOWER(TRIM(pft_status)) = 'approved' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END) AS approved_app_received_date
      FROM unischolarz.application_historicaluniversityapplication
      GROUP BY id
    ) had 
        ON had.id = ua.id
    LEFT JOIN unischolarz.crm_leadinfo li 
        ON li.lead_id = mli.Max_Lead_ID
    LEFT JOIN unischolarz.accounts_user upft 
        ON upft.id = li.pft_agent_id
    LEFT JOIN unischolarz.crm_conversionform c 
        ON c.application_id = ua.id
    LEFT JOIN unischolarz.unischolarz_site_unilead ul 
        ON ul.student_id = ua.student_id AND ul.id = mli.Max_Lead_ID
    LEFT JOIN us_standard1 u 
        ON (u.student_id = ul.student_id AND u.lead_id = li.lead_id)
    LEFT JOIN unischolarz.accounts_user ulqt 
        ON ulqt.id = li.associate_id
    LEFT JOIN unischolarz.accounts_user ucou 
        ON ucou.id = li.counsellor_id
    LEFT JOIN unischolarz.accounts_user uadd 
        ON uadd.id = li.admission_agent_id
    LEFT JOIN unischolarz.accounts_user uvis 
        ON uvis.id = li.visa_agent_id
    LEFT JOIN unischolarz.application_personaldetail pd 
        ON pd.student_id = c.student_id
    LEFT JOIN unischolarz.application_visadetails v 
        ON v.student_id = c.student_id
    LEFT JOIN unischolarz.application_enrollmentdetail e 
        ON e.student_id = c.student_id
    LEFT JOIN unischolarz.portfolio_country co 
        ON co.id = ua.country_of_study_id
    LEFT JOIN unischolarz.portfolio_campus uv 
        ON uv.id = ua.campus_id
    LEFT JOIN unischolarz.portfolio_programme pr 
        ON pr.id = ua.program_id
    LEFT JOIN unischolarz.portfolio_applicationpartner ap 
        ON ap.id = ua.application_partner_id
    LEFT JOIN (
        SELECT 
            lead_id,
            MAX(CASE WHEN note_rank = 1 THEN note END) AS last_note,
            MAX(CASE WHEN note_rank = 2 THEN note END) AS before_last_note,
            MAX(CASE WHEN note_rank = 1 THEN created_at END) AS last_note_created_at,
            MAX(CASE WHEN note_rank = 2 THEN created_at END) AS before_last_note_created_at
        FROM (
            SELECT 
                n.lead_id,
                n.note,
                n.created_by_id,
                n.created_at,
                ROW_NUMBER() OVER (PARTITION BY n.lead_id ORDER BY n.id DESC) AS note_rank
            FROM unischolarz.crm_note n
            LEFT JOIN unischolarz.crm_agentconfig u ON u.id = n.created_by_id
            WHERE n.is_deleted = 0
        ) AS counsellor_ranked_notes
        WHERE note_rank <= 2
        GROUP BY lead_id
    ) c_notes 
        ON li.lead_id = c_notes.lead_id
    LEFT JOIN (
      SELECT 
          id AS application_id,
          MIN(DATE(CASE WHEN status = 'APPLIED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Applied_Date,
          MIN(DATE(CASE WHEN status = 'NOT_APPLIED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Not_Applied_Date,
          MIN(DATE(CASE WHEN status = 'DOCUMENT_PENDING' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Document_Pending_Date,
          MIN(DATE(CASE WHEN status = 'INTAKE_NOT_OPEN' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Intake_Not_Open_Marked_at_Date,
          MIN(DATE(CASE WHEN status = 'OFFER_SENT_TO_STUDENT' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Offer_sent_to_student_Marked_at_Date,
          MIN(DATE(CASE WHEN status = 'STUDENT_DECLINED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Student_Declined_marked_at_Date,
          MIN(DATE(CASE WHEN status = 'STUDENT_ACCEPTED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Student_Accepted_Date,  
          MIN(DATE(CASE WHEN status = 'REJECTED_BY_AGENT' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Rejected_By_Agent_Date,
          MIN(DATE(CASE WHEN status = 'REJECTED_BY_UNIVERSITY' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Rejected_By_University_Date,
          MIN(DATE(CASE WHEN status = 'SUBMIT_TO_UNIVERSITY' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Submit_To_University_Date,
          MIN(DATE(CASE WHEN status = 'OFFER_RECEIVED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Offer_Received_Date,
          MIN(DATE(CASE WHEN pft_status = 'PENDING' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS Application_Received_Date,
          MIN(DATE(CASE WHEN pft_status = 'APPROVED' THEN CONVERT_TIMEZONE('Asia/Kolkata', history_date) END)) AS PFT_Received_Date
      FROM unischolarz.application_historicaluniversityapplication
      GROUP BY id
    ) status_dates 
        ON status_dates.application_id = ua.id
    ORDER BY 1 DESC
    """
    run_query(conn, cur, query_us_app_standard, f"{step_name_prefix}: Insert Data")

def run_all_etl_steps():
    conn = None
    cur = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        logger.info("Starting all ETL refresh steps.")
        update_uc_lead_2(conn, cur) 
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
