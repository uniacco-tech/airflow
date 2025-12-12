import logging
import redshift_connector
from typing import Optional

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DATABASE = "dev"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"

LOG_FILE = "log_olap.txt"

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
    step_name_prefix = "US_Standard"
    
    run_query(conn, cur, "truncate table public.us_standard1", f"{step_name_prefix}: Truncate")

    query_us_standard = """
    insert into public.us_standard1
    SELECT a.lead_id,
           a.student_id,
           CASE
               WHEN b.associate_id = 8183
                    OR lower(a.country) = 'india'
                    OR lower(b.lost_reason) = 'not an indian lead' THEN 0
               ELSE 1
           END AS to_be_considered,
           a.Lead_created_at,
           a.client_city,
           a.client_country,
           a.client_region,
           a.utm_source,
           a.utm_campaign,
           a.utm_medium,
           a.utm_content,
           a.utm_name,
           lower(a.level_of_study) AS level_of_study,
           CASE
               WHEN LOWER(a.level_of_study) LIKE '%master%'
                    OR LOWER(a.level_of_study) IN ('mba',
                                                   'msc') THEN 'Masters'
               WHEN LOWER(a.level_of_study) LIKE '%bachelor%'
                    OR LOWER(a.level_of_study) IN ('ug',
                                                   'graduate') THEN 'Bachelors'
               WHEN LOWER(a.level_of_study) IN ('pg-diploma',
                                                'diploma') THEN 'Diploma'
               WHEN LOWER(a.level_of_study) IN ('phd') THEN 'Phd'
               WHEN LOWER(a.level_of_study) IS NULL
                    OR LOWER(a.level_of_study) ='' THEN 'Null'
               ELSE 'Others'
           END AS clean_level_of_study,
           lower(a.field_of_study) AS field_of_study,
           to_char(a.intake,'MM-YYYY') AS intake,
           a.intake AS intake_date,
           a.programme,
           a.university,
           a.source,
           a.element,
           a.verified_phone,
           a.verified_email,
           a.country,
           CASE
               WHEN LOWER(a.country) IN ('usa',
                                         'united states',
                                         'united-states',
                                         'us') THEN 'USA'
               WHEN LOWER(a.country) IN ('uk',
                                         'united kingdom',
                                         'united-kingdom') THEN 'UK'
               WHEN LOWER(a.country) = 'ireland' THEN 'Ireland'
               WHEN LOWER(a.country) = 'canada' THEN 'Canada'
               WHEN LOWER(a.country) = 'australia' THEN 'Australia'
               WHEN LOWER(a.country) IN ('new zealand',
                                         'new-zealand') THEN 'NZ'
               WHEN LOWER(a.country) = 'india' THEN 'India'
               WHEN LOWER(a.country) = 'germany' THEN 'Germany'
               ELSE 'ROW'
           END AS Dest_Country,
           CASE
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) IN ('usa','united states','united-states','us') THEN 'USA'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) IN ('uk','united kingdom','united-kingdom') THEN 'UK'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) = 'ireland' THEN 'Ireland'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) = 'canada' THEN 'Canada'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) = 'australia' THEN 'Australia'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) IN ('new zealand','new-zealand') THEN 'NZ'
            WHEN LOWER(COALESCE(cf_latest.target_country_name, a.country)) = 'india' THEN 'India'
            ELSE 'ROW'
            END AS hybrid_country_new,
            CASE
               WHEN a.has_passport IS NULL OR a.has_passport is null THEN '-'
               WHEN a.has_passport = 0 AND (a.passport_status ILIKE 'Applied' OR a.passport_status IS NULL OR a.passport_status = '')
                   THEN 'Yes_Applied'
               WHEN a.has_passport = 0 AND a.passport_status ILIKE 'Not Applied'
                   THEN 'No_Not Applied'
               WHEN a.has_passport = 1
                   THEN 'Has_Passport'
               ELSE a.passport_status
           END AS Passport_New,
            a.avg_living_cost,
            a.education_funding,
           a.exam_attempted,
           a.has_passport,
           a.Branch AS Branch,
           a.gender,
           a.nationality,
           CASE
               WHEN a.nationality='indian' THEN 'indian'
               WHEN a.nationality IS NULL
                    OR a.nationality='' THEN 'Null'
               ELSE 'Non-Indian'
           END AS clean_nationality,
           a.passport_status,
           a.dashboard_qualified_tag,
           a.sd_dq_tag,
           a.dis_ql_tag, -- Added missing column
           convert_timezone('UTC', 'Asia/Kolkata', p.First_Walkin) as walkin_at,
           b.Cur_status,
           b.is_repeated,
           lower(b.lost_reason) AS lost_reason,
           b.call_in,
           b.call_out,
           b.sms_in,
           b.sms_out,
           b.admission_agent_id,
           b.associate_id,
           b.counsellor_id,
           b.visa_agent_id,
           b.admission_agent_assigned_at,
           b.counsellor_assigned_at,
           b.visa_agent_assigned_at,
           b.is_renewed,
           b.notes_count,
           b.tasks_count,
           b.outcome,
           b.is_cross_sale_lead,
           b.loan_needed,
           b.income_source,
           b.is_walkin,
           a.Hybrid_intake,
           a.Walk_in,
           a.Lead_Category,
           b.Lead_assigned,
           a.medium,
           a.qualification_score_buckets,
           a.OTP_Verified,
           o.Cur_Status_date,
           DATEDIFF('day', o.Cur_Status_date, CURRENT_DATE) AS Ageing_From_Cur_Status_days,
           o.Status_before_lost,
           o.Status_bef_lost_dat,
           b.Counsellor_Branch,
           CASE
               WHEN b.Counsellor_Branch IS NULL THEN a.Branch
               ELSE b.Counsellor_Branch
           END AS Hybrid_Branch,
           CASE
               WHEN m.lead_id>0 THEN 1
               ELSE 0
           END AS is_aip,
           m.fin_aip_dat,
           CASE
               WHEN n.lead_id>0 THEN 1
               ELSE 0
           END AS is_offer,
           n.fin_offer_dat,
           c.conversion_id,
           c.Conversions_filled,
           c.is_cancel,
           c.conversion_counsellor,
           c.Conv_Deposit_date,
           c.Conv_branch,
           c.Conv_Country,
           c.Conv_Campus,
           c.Conv_Programme,
           c.Conv_partner,
           c.Conv_kind,
           c.Conv_fy_tuition_fee,
           c.Conv_currency_code,
           c.Conv_actual_com,
           c.Conv_expected_com,
           d.LQT_Agent,
           e.Counsellor_Agent,
           f.APT_Agent,
           g.Visa_Agent,
           i.CONTACTED_date,
           i.OPPORTUNITY_date,
           case when (a.dis_ql_tag <> 1 OR a.dis_ql_tag IS NULL) then i.Qualified_date end AS Qualified_date, -- Use a.dis_ql_tag
           i.Cnslr_contacted_date,
           i.PROCESSING_date,
           i.IMPORTANT_date,
           i.AIP_date,
           i.OFFER_date,
           i.Deposit_date,
           i.VA_date,
           i.VAIP_date,
           i.VR_date,
           i.Enrolled_date,
           i.LOST_date,
           (CASE
                WHEN DATE(a.Lead_created_at) = DATE(i.LOST_date) THEN 1
            END) AS lost_lead_same_day,
           (CASE
                WHEN dateadd('day',7,DATE(a.Lead_created_at)) >= i.LOST_date THEN 1
            END) AS lost_lead_within_7,
           j.first_Cycle_CREATED_date,
           j.Last_Cycle_CREATED_date,
           j.first_Cycle_CONTACTED_date,
           j.Last_Cycle_CONTACTED_date,
           j.first_Cycle_OPPORTUNITY_date,
           j.Last_Cycle_OPPORTUNITY_date,
           j.first_Cycle_Qualified_date,
           j.Last_Cycle_Qualified_date,
           j.first_Cycle_Cnslr_Contacted_date,
           j.Last_Cycle_Cnslr_Contacted_Qualified_date,
           j.first_Cycle_PROCESSING_date,
           j.Last_Cycle_PROCESSING_date,
           j.first_Cycle_IMPORTANT_date,
           j.Last_Cycle_IMPORTANT_date,
           j.first_Cycle_AIP_date,
           j.Last_Cycle_AIP_date,
           j.first_Cycle_OFFER_date,
           j.Last_Cycle_OFFER_date,
           j.first_Cycle_Deposit_date,
           j.Last_Cycle_Deposit_date,
           j.first_Cycle_VA_date,
           j.Last_Cycle_VA_date,
           j.first_Cycle_VAIP_date,
           j.Last_Cycle_VAIP_date,
           j.first_Cycle_VR_date,
           j.Last_Cycle_VR_date,
           j.first_Enrolled_date,
           j.Last_Enrolled_date,
           j.first_Cycle_LOST_date,
           j.Last_Cycle_LOST_date,
           k.LQT_Calls,
           k.LQT_Outbound_dialled,
           k.LQT_Outbound_Connected,
           k.LQT_Inbound_calls,
           k.LQT_Inbound_Connected,
           k.LQT_Total_Duration_min,
           k.LQT_Avg_Duration_min,
           l.Counsellor_Calls,
           l.Counsellor_Outbound_dialled,
           l.Counsellor_Outbound_Connected,
           l.Counsellor_Inbound_calls,
           l.Counsellor_Inbound_Connected,
           l.Counsellor_Total_Duration_min,
           l.Counsellor_Avg_Duration_min
    FROM
      (SELECT a.id AS Lead_id,
              a.student_id,
              convert_timezone('UTC', 'Asia/Kolkata', a.created_at) AS Lead_created_at,
              a.client_city,
              a.client_country,
              a.client_region,
              a.utm_source,
              a.utm_campaign,
              a.utm_medium,
              a.utm_content,
              a.utm_name,
              a.level_of_study,
              a.field_of_study,
              a.intake,
              a.programme,
              a.university,
              a.source,
              a.element,
              a.verified_phone,
              a.verified_email,
              a.country,
              a.avg_living_cost,
              a.education_funding,
              a.exam_attempted,
              a.has_passport,
              pf.name AS Branch,
              a.gender,
              a.nationality,
              a.passport_status,
              convert_timezone('UTC', 'Asia/Kolkata', a.walkin_at) AS walkin_at,
              CASE
                  WHEN EXTRACT(MONTH
                               FROM a.intake) BETWEEN 1 AND 6 THEN 'Spring,' || EXTRACT(YEAR
                                                                                        FROM a.intake)
                  WHEN EXTRACT(MONTH
                               FROM a.intake) BETWEEN 7 AND 12 THEN 'Fall,' || EXTRACT(YEAR
                                                                                       FROM a.intake)
                  ELSE 'Others'
              END AS Hybrid_intake,
              CASE
                  WHEN a.walkin_at>0 THEN 'Walkin'
                  ELSE 'Remaining'
              END AS Walk_in,
              CASE
                  WHEN lower(a.utm_source) ='referral'
                       AND a.utm_medium='crm' THEN 'Agent_Referral'
                  WHEN lower(a.utm_source) ='referral'
                       AND a.utm_medium<>'crm' THEN 'Student_Referral'
                  WHEN (ut.is_BD=1
                        OR a.utm_source IN ('Institution - Help Desk',
                                            'Institution - Seminar',
                                            'institution-help-desk',
                                            'institution-seminar',
                                            'institution-sponsorship',
                                            'business-development')) THEN 'BD_Lead'
                  ELSE 'Remaining'
              END AS Lead_Category,
              CASE
                  WHEN a.utm_medium IN ('cpc',
                                        'ppc',
                                        'nestpick',
                                        'paid',
                                        'ads') THEN 'Paid'
                  ELSE 'Unpaid'
              END AS medium,
              CASE
                  WHEN a.qualification_score = 0 THEN '0'
                  WHEN a.qualification_score >0
                       AND a.qualification_score <= 10 THEN '1-10'
                  WHEN a.qualification_score > 10
                       AND a.qualification_score <= 20 THEN '11-20'
                  WHEN a.qualification_score > 20
                       AND a.qualification_score <= 30 THEN '21-30'
                  WHEN a.qualification_score > 30
                       AND a.qualification_score <= 40 THEN '31-40'
                  WHEN a.qualification_score > 40
                       AND a.qualification_score <= 50 THEN '41-50'
                  WHEN a.qualification_score > 50
                       AND a.qualification_score <= 60 THEN '51-60'
                  WHEN a.qualification_score > 60
                       AND a.qualification_score <= 70 THEN '61-70'
                  WHEN a.qualification_score > 70
                       AND a.qualification_score <= 80 THEN '71-80'
                  WHEN a.qualification_score > 80
                       AND a.qualification_score <= 90 THEN '81-90'
                  WHEN a.qualification_score > 90 THEN '91-100' else 'undi'
              END AS qualification_score_buckets,
              ut.is_otp_verified AS OTP_Verified,ut.dashboard_qualified_tag, 
              ut.sd_dq_tag, 
              ut.dis_ql_tag -- Added missing column
       FROM unischolarz.unischolarz_site_unilead a
       LEFT JOIN unischolarz.portfolio_branch pf ON pf.id= a.branch_id
       LEFT JOIN
         (SELECT ut.unilead_id,
                 max(CASE
                         WHEN ut.leadtag_id =66 THEN 1
                         ELSE 0
                     END) AS is_otp_verified,
                 max(CASE
                         WHEN ut.leadtag_id IN (85, 107, 106) THEN 1
                         ELSE 0
                     END) AS is_BD, max(CASE
                         WHEN ut.leadtag_id =144 THEN 1
                         ELSE 0
                     END) AS dashboard_qualified_tag,
                     max(CASE
                         WHEN ut.leadtag_id in (219, 187) THEN 1
                         ELSE 0
                     END) AS sd_dq_tag,
                     max(CASE
                         WHEN ut.leadtag_id in (311) THEN 1
                         ELSE 0
                     END) AS dis_ql_tag
          FROM unischolarz.unischolarz_site_unilead_tags ut
          WHERE ut.leadtag_id IN (85,
                                  107,
                                  106,
                                  66,144,219, 187,311)
          GROUP BY 1)ut ON ut.unilead_id = a.id) a
    LEFT JOIN
      (SELECT a.lead_id,
              a.status AS Cur_status,
              a.is_repeated,
              a.lost_reason,
              a.call_in,
              a.call_out,
              a.sms_in,
              a.sms_out,
              a.admission_agent_id,
              a.associate_id,
              a.counsellor_id,
              a.visa_agent_id,
              convert_timezone('UTC', 'Asia/Kolkata', a.admission_agent_assigned_at) AS admission_agent_assigned_at,
              convert_timezone('UTC', 'Asia/Kolkata', a.counsellor_assigned_at) AS counsellor_assigned_at,
              convert_timezone('UTC', 'Asia/Kolkata', a.visa_agent_assigned_at) AS visa_agent_assigned_at,
              a.is_renewed,
              a.notes_count,
              a.tasks_count,
              a.outcome,
              a.is_cross_sale_lead,
              a.loan_needed,
              a.income_source,
              a.is_walkin,
              CASE
                  WHEN a.associate_id=8537 THEN 'Squadstack'
                  WHEN a.associate_id IS NOT NULL THEN 'LQT'
                  ELSE 'Counsellor'
              END AS Lead_assigned,
              pf.name AS Counsellor_Branch
       FROM unischolarz.crm_leadinfo a
       LEFT JOIN unischolarz.crm_agentconfig ag ON ag.agent_id=a.counsellor_id
       LEFT JOIN unischolarz.portfolio_branch pf ON pf.id=ag.branch_id) b ON b.lead_id = a.lead_id
    -- JOINS for hybrid_country_new logic and AIP/Offer checks
    -- JOIN that picks latest application per student (Snowflake / supports QUALIFY)
    LEFT JOIN (
        SELECT
            ua.student_id,
            pc.name AS latest_application_country
        FROM unischolarz.application_universityapplication ua
        LEFT JOIN unischolarz.portfolio_country pc
          ON pc.id = ua.country_of_study_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ua.student_id ORDER BY ua.created_at DESC, ua.id DESC) = 1
    ) latest_app
      ON latest_app.student_id = a.student_id
      LEFT JOIN (
      SELECT
        cf.lead_id,
        pc.name AS target_country_name
      FROM unischolarz.crm_conversionform cf
      LEFT JOIN unischolarz.portfolio_country pc 
        ON pc.id = cf.target_country_id
      WHERE cf.target_country_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY cf.lead_id ORDER BY cf.created_at DESC) = 1
    ) cf_latest
      ON cf_latest.lead_id = a.lead_id
    
    
    -- END JOINS
    LEFT JOIN
      (SELECT c.lead_id,
              c.id AS conversion_id,
              rnk.Conversions_filled,
              CASE
                  WHEN uap.is_cancel = 1
                       OR c.is_cancel = 1 THEN 1
                  ELSE 0
              END AS is_cancel,
              ui.first_name+ ' '+ ui.last_name AS conversion_counsellor,
              convert_timezone('UTC', 'Asia/Kolkata', ttt.created_at) AS Conv_Deposit_date,
              CASE
                  WHEN b.name IS NULL THEN 'Mumbai - HO'
                  ELSE b.name
              END AS Conv_branch,
              co.name AS Conv_Country,
              uv.name AS Conv_Campus,
              pm.name AS Conv_Programme,
              ap.name AS Conv_partner,
              CASE
                  WHEN de.kind= 'Master' THEN 'Masters'
                  WHEN de.kind= 'masters' THEN 'Masters'
                  WHEN de.kind='Bachelor' THEN 'bachelors'
                  ELSE de.kind
              END AS Conv_kind,
              c.fy_tuition_fee AS Conv_fy_tuition_fee,
              c.currency_code AS Conv_currency_code,
              c.actual_com AS Conv_actual_com,
              c.expected_com AS Conv_expected_com
       FROM unischolarz.crm_conversionform c
       JOIN
         (SELECT a.id,
                 a.Conversions_filled
          FROM
            (SELECT c.id,
                    rank() OVER (PARTITION BY lead_id
                                 ORDER BY id DESC) AS rnk1,
                                rank() OVER (PARTITION BY lead_id
                                             ORDER BY id) AS Conversions_filled
             FROM unischolarz.crm_conversionform c)a
          WHERE rnk1=1) rnk ON rnk.id=c.id
       left join (SELECT lead_id, min(created_at) created_at FROM unischolarz.crm_conversionform group by 1) ttt on c.lead_id = ttt.lead_id
       LEFT JOIN unischolarz.application_universityapplication uap ON c.application_id = uap.id
       LEFT JOIN unischolarz.portfolio_programme pm ON pm.id=uap.program_id
       LEFT JOIN unischolarz.portfolio_degree de ON pm.degree_id=de.id
       LEFT JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = c.lead_id
       LEFT JOIN unischolarz.accounts_user ui ON ui.id = c.counsellor_id
       LEFT JOIN unischolarz.crm_agentconfig ac ON ac.agent_id = c.counsellor_id
       LEFT JOIN unischolarz.portfolio_branch b ON b.id = ac.branch_id
       LEFT JOIN unischolarz.portfolio_country co ON co.id = uap.country_of_study_id
       LEFT JOIN unischolarz.portfolio_campus uv ON uv.id = uap.campus_id
       LEFT JOIN unischolarz.portfolio_applicationpartner ap ON ap.id = uap.application_partner_id
       LEFT JOIN unischolarz.portfolio_country pc ON pc.id = uap.country_of_study_id
       ) c ON c.lead_id = a.Lead_id
    LEFT JOIN
      (SELECT first_name + ' ' + last_name AS LQT_Agent,
              id AS LQT_id
       FROM unischolarz.accounts_user) d ON d.LQT_id = b.associate_id
    LEFT JOIN
      (SELECT first_name + ' ' + last_name AS Counsellor_Agent,
              id AS Counsellor_id
       FROM unischolarz.accounts_user) e ON e.Counsellor_id = b.counsellor_id
    LEFT JOIN
      (SELECT first_name + ' ' + last_name AS APT_Agent,
              id AS APT_id
       FROM unischolarz.accounts_user) f ON f.APT_id = b.admission_agent_assigned_at
    LEFT JOIN
      (SELECT first_name + ' ' + last_name AS Visa_Agent,
              id AS VISA_Agent_id
       FROM unischolarz.accounts_user) g ON g.VISA_Agent_id = b.visa_agent_id
    LEFT JOIN
      (SELECT Lead_id,
              MIN(CASE
                      WHEN status = 'CONTACTED' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS CONTACTED_date,
              MIN(CASE
                      WHEN status = 'OPPORTUNITY' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS OPPORTUNITY_date,
              MIN(CASE
                      WHEN status IN ('QUALIFIED','COUNSELLOR CONTACTED','PROCESSING') THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Qualified_date,
              MIN(CASE
                      WHEN status = 'COUNSELLOR CONTACTED' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Cnslr_contacted_date,
              MIN(CASE
                      WHEN status = 'PROCESSING' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS PROCESSING_date,
              MIN(CASE
                      WHEN status = 'IMPORTANT' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS IMPORTANT_date,
              MIN(CASE
                      WHEN status = 'ADMISSION IN PROCESS' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS AIP_date,
              MIN(CASE
                      WHEN status = 'OFFER' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS OFFER_date,
              MIN(CASE
                      WHEN status = 'DEPOSIT' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Deposit_date,
              MIN(CASE
                      WHEN status = 'VISA APPLIED' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS VA_date,
              MIN(CASE
                      WHEN status = 'VISA APPLICATION IN PROCESS' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS VAIP_date,
              MIN(CASE
                      WHEN status = 'VISA RECEIVED' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS VR_date,
              MIN(CASE
                      WHEN status = 'ENROLLED' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Enrolled_date,
              MIN(CASE
                      WHEN status = 'LOST' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS LOST_date
       FROM unischolarz.crm_historicalleadinfo
       WHERE is_repeated = 0
       GROUP BY Lead_id) i ON i.Lead_id = a.lead_id
    LEFT JOIN
      (SELECT a.Lead_id AS Lead_id,
              MIN(CASE
                      WHEN status = 'CREATED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_CREATED_date,
              MIN(CASE
                      WHEN status = 'CREATED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_CREATED_date,
              MIN(CASE
                      WHEN status = 'CONTACTED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_CONTACTED_date,
              MIN(CASE
                      WHEN status = 'CONTACTED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_CONTACTED_date,
              MIN(CASE
                      WHEN status = 'OPPORTUNITY'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_OPPORTUNITY_date,
              MIN(CASE
                      WHEN status = 'OPPORTUNITY'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_OPPORTUNITY_date,
              MIN(CASE
                      WHEN status IN ('QUALIFIED','COUNSELLOR CONTACTED','PROCESSING')
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_Qualified_date,
              MIN(CASE
                      WHEN status IN ('QUALIFIED','COUNSELLOR CONTACTED','PROCESSING')
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_Qualified_date,
              MIN(CASE
                      WHEN status = 'COUNSELLOR CONTACTED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_Cnslr_Contacted_date,
              MIN(CASE
                      WHEN status = 'COUNSELLOR CONTACTED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_Cnslr_Contacted_Qualified_date,
              MIN(CASE
                      WHEN status = 'PROCESSING'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_PROCESSING_date,
              MIN(CASE
                      WHEN status = 'PROCESSING'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_PROCESSING_date,
              MIN(CASE
                      WHEN status = 'IMPORTANT'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_IMPORTANT_date,
              MIN(CASE
                      WHEN status = 'IMPORTANT'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_IMPORTANT_date,
              MIN(CASE
                      WHEN status = 'ADMISSION IN PROCESS'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_AIP_date,
              MIN(CASE
                      WHEN status = 'ADMISSION IN PROCESS'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_AIP_date,
              MIN(CASE
                      WHEN status = 'OFFER'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_OFFER_date,
              MIN(CASE
                      WHEN status = 'OFFER'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_OFFER_date,
              MIN(CASE
                      WHEN status = 'DEPOSIT'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_Deposit_date,
              MIN(CASE
                      WHEN status = 'DEPOSIT'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_Deposit_date,
              MIN(CASE
                      WHEN status = 'VISA APPLIED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_VA_date,
              MIN(CASE
                      WHEN status = 'VISA APPLIED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_VA_date,
              MIN(CASE
                      WHEN status = 'VISA APPLICATION IN PROCESS'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_VAIP_date,
              MIN(CASE
                      WHEN status = 'VISA APPLICATION IN PROCESS'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_VAIP_date,
              MIN(CASE
                      WHEN status = 'VISA RECEIVED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_VR_date,
              MIN(CASE
                      WHEN status = 'VISA RECEIVED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_VR_date,
              MIN(CASE
                      WHEN status = 'ENROLLED'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Enrolled_date,
              MIN(CASE
                      WHEN status = 'ENROLLED'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Enrolled_date,
              MIN(CASE
                      WHEN status = 'LOST'
                           AND history_date >= first_cycle_start_date
                           AND history_date < first_cycle_end_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS first_Cycle_LOST_date,
              MIN(CASE
                      WHEN status = 'LOST'
                           AND history_date >=Last_cycle_start_date THEN convert_timezone('UTC', 'Asia/Kolkata', history_date)
                  END) AS Last_Cycle_LOST_date
       FROM unischolarz.crm_historicalleadinfo a
       LEFT JOIN
         (SELECT lead_id,
                 min(CASE
                         WHEN rank = 1 THEN history_date
                     END) AS first_cycle_start_date,
                 min(CASE
                         WHEN rank = 2 THEN history_date
                     END) AS first_cycle_end_date,
                 min(CASE
                         WHEN Desc_Rank = 1 THEN history_date
                     END) AS Last_cycle_start_date
          FROM
            (SELECT rank () OVER (PARTITION BY lead_id
                                  ORDER BY history_id) AS Rank,
                                 rank () OVER (PARTITION BY lead_id,
                                                            status
                                               ORDER BY history_date DESC) AS Desc_Rank,
                                              *
             FROM
               (SELECT *
                FROM
                  (SELECT lead_id,
                          history_id,
                          history_date,
                          status,
                          LAG(status, 1) OVER (PARTITION BY lead_id
                                               ORDER BY history_id) AS previous_status
                   FROM unischolarz.crm_historicalleadinfo) a
                WHERE (status <> previous_status
                       OR previous_status IS NULL) )
             WHERE status = 'CREATED') a
          GROUP BY 1) b ON b.lead_id = a.lead_id
       GROUP BY 1) j ON j.Lead_id = a.lead_id
    LEFT JOIN
      (WITH Calls_Summary AS
         (SELECT li.id AS Calls,
                 li.agent_id AS Agent_id,
                 li.lead_id,
                 li.service,
                 CASE
                     WHEN li.service IN ('OZONETEL',
                                         'CALL_LOGGER')
                          AND li.status IN ('COMPLETED',
                                            NULL)
                          AND REGEXP_INSTR(li.call_duration, '^[0-9]{2}:[0-9]{2}:[0-9]{2}$') = 1 THEN (CAST(SPLIT_PART(li.call_duration, ':', 1) AS INT) * 3600) + (CAST(SPLIT_PART(li.call_duration, ':', 2) AS INT) * 60) + CAST(SPLIT_PART(li.call_duration, ':', 3) AS INT)
                     WHEN li.service = 'KNOWLARITY'
                          AND li.status = 'Connected'
                          AND REGEXP_INSTR(li.duration, '^[0-9]{2}:[0-9]{2}:[0-9]{2}$') = 1 THEN (CAST(SPLIT_PART(li.duration, ':', 1) AS INT) * 3600) + (CAST(SPLIT_PART(li.duration, ':', 2) AS INT) * 60) + CAST(SPLIT_PART(li.duration, ':', 3) AS INT)
                     WHEN li.service = 'SALES_TRAIL'
                          AND li.status = 'ANSWERED' THEN li.duration::INT
                 END AS call_duration,
                 li.status,
                 li.direction
          FROM unischolarz.crm_leadinteractionlog li
          JOIN unischolarz.crm_leadinfo l ON l.lead_id = li.lead_id
          AND l.associate_id = li.agent_id) SELECT lead_id,
                                                   COUNT(Calls) AS LQT_Calls,
                                                   SUM(CASE
                                                           WHEN direction = 'OUTBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS LQT_Outbound_dialled,
                                                   SUM(CASE
                                                           WHEN call_duration > 10
                                                                AND direction = 'OUTBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS LQT_Outbound_Connected,
                                                   SUM(CASE
                                                           WHEN direction = 'INBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS LQT_Inbound_calls,
                                                   SUM(CASE
                                                           WHEN (status IN ('connected', 'answered', 'completed')
                                                                 OR (service = 'CALL_LOGGER'
                                                                     AND call_duration >= 10))
                                                                 AND direction = 'INBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS LQT_Inbound_Connected,
                                                   SUM(call_duration) / 60 AS LQT_Total_Duration_min,
                                                   AVG(CASE
                                                           WHEN call_duration > 10 THEN call_duration
                                                           ELSE NULL
                                                       END) / 60 AS LQT_Avg_Duration_min
            FROM Calls_Summary
            GROUP BY 1) k ON k.lead_id = a.Lead_id
    LEFT JOIN
      (WITH Calls_Summary AS
         (SELECT li.id AS Calls,
                 li.agent_id AS Agent_id,
                 li.lead_id,
                 li.service,
                 CASE
                     WHEN li.service IN ('OZONETEL',
                                         'CALL_LOGGER')
                          AND li.status IN ('COMPLETED',
                                            NULL)
                          AND REGEXP_INSTR(li.call_duration, '^[0-9]{2}:[0-9]{2}:[0-9]{2}$') = 1 THEN (CAST(SPLIT_PART(li.call_duration, ':', 1) AS INT) * 3600) + (CAST(SPLIT_PART(li.call_duration, ':', 2) AS INT) * 60) + CAST(SPLIT_PART(li.call_duration, ':', 3) AS INT)
                     WHEN li.service = 'KNOWLARITY'
                          AND li.status = 'Connected'
                          AND REGEXP_INSTR(li.duration, '^[0-9]{2}:[0-9]{2}:[0-9]{2}$') = 1 THEN (CAST(SPLIT_PART(li.duration, ':', 1) AS INT) * 3600) + (CAST(SPLIT_PART(li.duration, ':', 2) AS INT) * 60) + CAST(SPLIT_PART(li.duration, ':', 3) AS INT)
                     WHEN li.service = 'SALES_TRAIL'
                          AND li.status = 'ANSWERED' THEN li.duration::INT
                 END AS call_duration,
                 li.status,
                 li.direction
          FROM unischolarz.crm_leadinteractionlog li
          JOIN unischolarz.crm_leadinfo l ON l.lead_id = li.lead_id
          AND l.counsellor_id = li.agent_id) SELECT lead_id,
                                                   COUNT(Calls) AS Counsellor_Calls,
                                                   SUM(CASE
                                                           WHEN direction = 'OUTBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS Counsellor_Outbound_dialled,
                                                   SUM(CASE
                                                           WHEN call_duration > 10
                                                                AND direction = 'OUTBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS Counsellor_Outbound_Connected,
                                                   SUM(CASE
                                                           WHEN direction = 'INBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS Counsellor_Inbound_calls,
                                                   SUM(CASE
                                                           WHEN (status IN ('connected', 'answered', 'completed')
                                                                 OR (service = 'CALL_LOGGER'
                                                                     AND call_duration >= 10))
                                                                 AND direction = 'INBOUND' THEN 1
                                                           ELSE 0
                                                       END) AS Counsellor_Inbound_Connected,
                                                   SUM(call_duration) / 60 AS Counsellor_Total_Duration_min,
                                                   AVG(CASE
                                                           WHEN call_duration > 10 THEN call_duration
                                                           ELSE NULL
                                                       END) / 60 AS Counsellor_Avg_Duration_min
            FROM Calls_Summary
            GROUP BY 1) l ON l.lead_id = a.Lead_id
    LEFT JOIN (
        SELECT
            lead_id,
            convert_timezone('UTC', 'Asia/Kolkata', MAX(history_date)) AS Cur_Status_date,
            MAX(CASE WHEN status <> 'LOST' AND Final_Status = 'LOST' THEN status ELSE NULL END) AS Status_before_lost,
            MAX(CASE WHEN status <> 'LOST' AND Final_Status = 'LOST' THEN convert_timezone('UTC', 'Asia/Kolkata', history_date) ELSE NULL END) AS Status_bef_lost_dat,
            MAX(Final_Status) as Cur_Status
        FROM (
            SELECT
                *,
                LAST_VALUE(status) OVER (PARTITION BY lead_id ORDER BY history_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Final_Status
            FROM unischolarz.crm_historicalleadinfo
        ) AS history_with_final_status
        GROUP BY lead_id, Final_Status
    ) o ON o.lead_id = a.Lead_id
    -- SUBQUERY M: AIP status based on application table
    LEFT JOIN
      (SELECT a.lead_id,
              min(a.AIP_Date) AS 
              fin_aip_dat
       FROM
         (SELECT h.lead_id,
                 ul.student_id,
                 TRUNC(MIN(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', h.history_date))) AS AIP_Date
          FROM unischolarz.crm_historicalleadinfo h
          JOIN unischolarz.unischolarz_site_unilead ul ON ul.id = h.lead_id
          JOIN unischolarz.crm_leadinfo l ON l.lead_id = h.lead_id
          AND l.admission_agent_id IS NOT NULL
          WHERE h.status IN ('ADMISSION IN PROCESS',
                             'OFFER',
                             'DEPOSIT')
            AND l.status IN ('ADMISSION IN PROCESS',
                             'OFFER',
                             'DEPOSIT',
                             'CAS',
                             'VISA APPLICATION IN PROCESS',
                             'VISA APPLIED',
                             'VISA RECEIVED',
                             'LOST',
                             'ENROLLED','OPPORTUNITY','PROCESSING','IMPORTANT','COUNSELLOR CONTACTED','QUALIFIED','CONTACTED')
          GROUP BY 1,
                   2) a
       JOIN unischolarz.application_universityapplication ua ON ua.student_id = a.student_id
       WHERE ua.status <> 'NOT_APPLIED'
       GROUP BY 1) m ON m.lead_id=a.lead_id
    -- SUBQUERY N: Offer status based on application table
    LEFT JOIN (
    SELECT a.lead_id,
              min(a.Offer_Date) AS fin_offer_dat
       FROM
         (SELECT h.lead_id,
                 ul.student_id,
                 TRUNC(MIN(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', h.history_date))) AS Offer_Date
          FROM unischolarz.crm_historicalleadinfo h
          JOIN unischolarz.unischolarz_site_unilead ul ON ul.id=h.lead_id
          JOIN unischolarz.crm_leadinfo l ON l.lead_id=h.lead_id
          AND l.admission_agent_id IS NOT NULL
          WHERE h.status IN ('OFFER',
                             'DEPOSIT')
            AND l.status IN ('OFFER',
                             'DEPOSIT',
                             'CAS',
                             'VISA APPLICATION IN PROCESS',
                             'VISA APPLIED',
                             'VISA RECEIVED',
                             'LOST',
                             'ENROLLED')
          GROUP BY 1,
                   2)a
       JOIN unischolarz.application_universityapplication ua ON ua.student_id=a.student_id
       WHERE offer_letter <>''
         AND ua.status<>'NOT_APPLIED'
       GROUP BY 1
    ) n ON n.lead_id = a.Lead_id
    -- SUBQUERY P: Corrected 'lead_id' to 'id'
    LEFT JOIN (
        SELECT
            id AS lead_id,
            convert_timezone('UTC', 'Asia/Kolkata', MIN(walkin_at)) AS First_Walkin
        FROM unischolarz.unischolarz_site_unilead
        WHERE walkin_at IS NOT NULL
        GROUP BY id
    ) p ON p.lead_id = a.Lead_id
    """
    run_query(conn, cur, query_us_standard, f"{step_name_prefix}: Insert Data")

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
