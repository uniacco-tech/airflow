import logging
import time, sys, os
import datetime
import pandas as pd
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import warnings
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import redshift_connector
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"
REDSHIFT_PORT = 5439

def run_query():

    logging.info("Starting UniScholars ETL email job")
    today_date = datetime.today().date() 

    # ------------- Redshift Connection -------------
    try:
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            database=REDSHIFT_DB,
            port=REDSHIFT_PORT,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {e}")
        send_basic_error_email("Redshift Connection Failed", f"The UniScholars ETL job failed to connect to Redshift. Error: {e}")
        return

    query = """
    SELECT ul.id,
          CASE 
            WHEN LOWER(ul.level_of_study) IN ('masters', 'master', 'master''s', 'mastres', 'mastes') THEN 'Masters'
            WHEN LOWER(ul.level_of_study) IN ('bachelors', 'bachelor', 'bachelor''s', 'bachlor', 'bachlors') THEN 'Bachelors'
            WHEN LOWER(ul.level_of_study) IN ('phd') THEN 'PhD'
            WHEN LOWER(ul.level_of_study) IN ('diploma') THEN 'Diploma'
            ELSE 'Blank'
          END AS level_of_study,
          DATE(s.lead_created_at) as lead_created_at,
          s.dest_country,
          CASE WHEN tg.name = 'OTP Verified' THEN 'Yes' ELSE 'No' END AS otp_verified,
          pb.name AS Branch,
          s.hybrid_intake,
          CASE WHEN ul.has_passport = 1 THEN 'Yes'
               WHEN ul.has_passport = 0 THEN 'No'
               ELSE ' ' END as has_passport,
          s.qualified_date,
          s.hybrid_branch,
          s.utm_source,
          s.utm_campaign
    FROM unischolarz.unischolarz_site_unilead ul
    JOIN us_standard1 s ON s.lead_id = ul.id
    LEFT JOIN unischolarz.unischolarz_site_unilead_tags t ON t.unilead_id = ul.id AND t.leadtag_id=66
    LEFT JOIN unischolarz.unischolarz_site_leadtag tg ON tg.id = t.leadtag_id
    LEFT JOIN unischolarz.portfolio_branch pb ON pb.id=ul.branch_id
    WHERE ((DATE(s.lead_created_at) BETWEEN DATE(CURRENT_DATE - INTERVAL '91' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY))
    OR
          (DATE(s.qualified_date) BETWEEN DATE(CURRENT_DATE - INTERVAL '91' DAY) AND DATE(CURRENT_DATE - INTERVAL '1' DAY)))
    AND s.is_repeated=0 
    AND s.lost_reason <> 'not an indian lead' 
    AND LOWER(s.utm_source) <> 'partner' 
    AND s.to_be_considered = 1
    """

    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [x[0] for x in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    cur.close()
    conn.close()
    logging.info("Data fetched successfully from Redshift")

    for col in ['lead_created_at', 'qualified_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        '/home/ubuntu/airflow/secrets/credential.json', scope
    )
    gc = gspread.authorize(credentials)

    try:
        ws1 = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1b4_DGN1DlSEclc-P9guyZOT-2ZQIUC_qO3VR-fP4klc/edit?usp=sharing"
        ).worksheet("Dump")

        gd.set_with_dataframe(ws1, df, row=1, col=1)
        logging.info("Data dumped to Google Sheet successfully")
    except Exception as e:
        logging.error(f"Failed to dump data to Google Sheet: {e}")

    def categorize_channel(row):
        utm = row.get('utm_source', None)
        campaign = row.get('utm_campaign', '')
        if utm is None or (isinstance(utm, str) and utm.strip() == ''):
            return 'others'
        utm_l = str(utm).lower().strip()
        if utm_l == 'google':
            if 'src-' in str(campaign).lower():
                return 'google-search'
            else:
                return 'google-others'
        return utm_l

    allowed_channels = [
        'google-search', 'google-others',
        'unicreds', 'instagram', 'bing', 'moengage'
    ]

    df['channel'] = df.apply(categorize_channel, axis=1)
    df['channel'] = df['channel'].fillna('others').replace('', 'others')
    df['channel'] = df['channel'].apply(lambda x: x if str(x).lower() in allowed_channels else 'others')
    order_list = ['google-search','google-others','instagram','unicreds','bing','moengage','others']
    df['channel'] = pd.Categorical(df['channel'], categories=order_list, ordered=True)

    def intake_sort_key(val):
        if pd.isna(val) or str(val).strip() == "" or str(val).lower() in ["blank", "others"]:
            return (9999, 9)
        try:
            val = str(val).replace("’", "'").replace("`","'").strip().title()
            term, year = val.split(",")   # <-- comma-separated intake
            year = int(year)
            term_order = {"Spring": 1, "Fall": 2}
            return (year, term_order.get(term, 9))
        except:
            return (9999, 9)

    all_intakes = df['hybrid_intake'].fillna('Others').astype(str).unique()
    sorted_intakes = sorted(all_intakes, key=intake_sort_key)
    if "Others" in sorted_intakes:
        sorted_intakes.remove("Others")
        sorted_intakes.append("Others")
    unique_intakes = sorted_intakes

    df_uk = df[df['dest_country']=='UK'].copy()
    df_us = df[df['dest_country']=='USA'].copy()

    def df_to_inline_html(df):
        if df is None or df.empty:
            return "<p>No data available</p>"
        
        styles = """
        <style>
            table {border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 13px;}
            th, td {border: 1px solid black; padding: 6px 10px; text-align: center; vertical-align: middle;}
            th {background-color: #f2f2f2; font-weight: bold;}
            td.index {text-align: center; vertical-align: middle;}
        </style>
        """
        html_table = df.to_html(index=True, border=1, justify="center").replace('class="dataframe"', '')
        return styles + html_table

    def generate_summary(df_in, groupby_column):
        if df_in.empty or groupby_column not in df_in.columns:
            return pd.DataFrame()
        temp = df_in.copy()
        temp[groupby_column] = temp[groupby_column].fillna('others').replace('', 'others').astype(str)

        yesterday = today_date - timedelta(days=1)

        windows = {
            'Yesterday': (yesterday, yesterday, 1),
            'Last 7 Days': (yesterday - timedelta(days=6), yesterday, 7),
            'Last 30 Days': (yesterday - timedelta(days=29), yesterday, 30),
            'Last 3 Months': (yesterday - timedelta(days=89), yesterday, 90)
        }

        if groupby_column == 'channel':
            categories_list = order_list[:]
            for p in temp[groupby_column].unique():
                if p not in categories_list:
                    categories_list.append(p)
        elif groupby_column == "hybrid_intake":
            categories_list = unique_intakes
        else:
            categories_list = list(pd.Series(temp[groupby_column].astype(str).unique()))

        result = pd.DataFrame(index=categories_list)

        for label, (start, end, days) in windows.items():
            mask = (temp['lead_created_at'] >= start) & (temp['lead_created_at'] <= end)
            counts = temp.loc[mask].groupby(temp[groupby_column])['id'].nunique()
            counts = counts.reindex(result.index).fillna(0).astype(int)
            result[label] = counts
            result[f"{label} Avg"] = (counts / days).round(0).astype(int)

        return result

    def finalize_summary(numeric_df):
        if numeric_df is None or numeric_df.empty:
            ordered_cols = [
                'Yesterday', '% Share Yesterday',
                'Last 7 Days', 'Last 7 Days Avg', '% Share Last 7 Days',
                'Last 30 Days', 'Last 30 Days Avg', '% Share Last 30 Days',
                'Last 3 Months', 'Last 3 Months Avg', '% Share Last 3 Months'
            ]
            empty_df = pd.DataFrame(columns=ordered_cols)
            empty_df.loc['Total'] = [0 if not c.startswith('% Share') else '100%' for c in ordered_cols]
            return empty_df.fillna(0)


        df_work = numeric_df.copy()

        windows = [
            ('Yesterday', 1),
            ('Last 7 Days', 7),
            ('Last 30 Days', 30),
            ('Last 3 Months', 90)
        ]

        for label, days in windows:
            total = int(df_work[label].sum())
            pct_col = f"% Share {label}"
            if total == 0:
                df_work[pct_col] = "0%"
            else:
                df_work[pct_col] = (df_work[label] / total * 100).round(0).astype(int).astype(str) + '%'
            df_work[label] = df_work[label].astype(int)
            df_work[f"{label} Avg"] = df_work[f"{label} Avg"].astype(int)

        ordered_cols = [
            'Yesterday', '% Share Yesterday',
            'Last 7 Days', 'Last 7 Days Avg', '% Share Last 7 Days',
            'Last 30 Days', 'Last 30 Days Avg', '% Share Last 30 Days',
            'Last 3 Months', 'Last 3 Months Avg', '% Share Last 3 Months'
        ]
        
        existing_cols = [col for col in ordered_cols if col in df_work.columns]
        df_work = df_work.reindex(columns=existing_cols)

        totals = {}
        for col in existing_cols:
            if col.startswith('% Share'):
                totals[col] = "100%"
            else:
                totals[col] = int(df_work[col].sum()) if col in df_work.columns else 0

        df_final = df_work.copy()
        df_final.loc['Total'] = pd.Series(totals)

        for c in df_final.columns:
            if c.startswith('% Share'):
                df_final[c] = df_final[c].astype(str)
            else:
                df_final[c] = df_final[c].astype(int)

        return df_final

    # === GENERATE RAW NUMERIC SUMMARIES (no totals yet) ===
    overall_stream_num = generate_summary(df, 'level_of_study')
    overall_stream_uk_num = generate_summary(df_uk, 'level_of_study')
    overall_stream_us_num = generate_summary(df_us, 'level_of_study')

    overall_intake_num = generate_summary(df, 'hybrid_intake')
    overall_intake_uk_num = generate_summary(df_uk, 'hybrid_intake')
    overall_intake_us_num = generate_summary(df_us, 'hybrid_intake')

    overall_otp_num = generate_summary(df, 'otp_verified')
    overall_passport_num = generate_summary(df, 'has_passport')
    overall_hybrid_num = generate_summary(df, 'hybrid_branch')
    overall_channel_num = generate_summary(df, 'channel')

    overall_hybrid_uk_num = generate_summary(df_uk, 'hybrid_branch')
    overall_hybrid_us_num = generate_summary(df_us, 'hybrid_branch')
    overall_channel_uk_num = generate_summary(df_uk, 'channel')
    overall_channel_us_num = generate_summary(df_us, 'channel')

    # === FILTERING RULE: For intake views ONLY, drop intake rows where Yesterday == 0 in ALL three views (overall, UK, US) ===
    def filter_intake_rows(num_overall, num_uk, num_us):
        # Gather all unique intake indexes
        all_idx = set()
        if not num_overall.empty: all_idx.update(num_overall.index)
        if not num_uk.empty: all_idx.update(num_uk.index)
        if not num_us.empty: all_idx.update(num_us.index)

        keep = []
        for idx in all_idx:
            y_ov = int(num_overall.loc[idx, 'Yesterday']) if (not num_overall.empty and idx in num_overall.index) else 0
            y_uk = int(num_uk.loc[idx, 'Yesterday']) if (not num_uk.empty and idx in num_uk.index) else 0
            y_us = int(num_us.loc[idx, 'Yesterday']) if (not num_us.empty and idx in num_us.index) else 0
            # Keep this intake row if ANY of the three Yesterday values > 0
            if (y_ov > 0) or (y_uk > 0) or (y_us > 0):
                keep.append(idx)
        
        def reidx_and_fill(num_df):
            if num_df.empty:
                return num_df # Return empty df as is
            
            new = num_df.reindex(keep)
            new = new.fillna(0).astype(int) 
            return new[new.index.isin(keep)] # Only keep the rows that are in the 'keep' list

        return reidx_and_fill(num_overall), reidx_and_fill(num_uk), reidx_and_fill(num_us)

    overall_intake_num_f, overall_intake_uk_num_f, overall_intake_us_num_f = filter_intake_rows(
        overall_intake_num, overall_intake_uk_num, overall_intake_us_num
    )

    # === FINALIZE ALL summaries (convert to display format with % and Totals) ===
    overall_stream = finalize_summary(overall_stream_num)
    overall_stream_uk = finalize_summary(overall_stream_uk_num)
    overall_stream_us = finalize_summary(overall_stream_us_num)

    overall_intake = finalize_summary(overall_intake_num_f)
    overall_intake_uk = finalize_summary(overall_intake_uk_num_f)
    overall_intake_us = finalize_summary(overall_intake_us_num_f)

    overall_otp = finalize_summary(overall_otp_num)
    overall_passport = finalize_summary(overall_passport_num)
    overall_hybrid = finalize_summary(overall_hybrid_num)

    overall_hybrid_uk = finalize_summary(overall_hybrid_uk_num)
    overall_hybrid_us = finalize_summary(overall_hybrid_us_num)

    overall_channel = finalize_summary(overall_channel_num)
    overall_channel_uk = finalize_summary(overall_channel_uk_num)
    overall_channel_us = finalize_summary(overall_channel_us_num)

    # HTML output (center-aligned tables)
    overall_stream_html = df_to_inline_html(overall_stream)
    overall_stream_uk_html = df_to_inline_html(overall_stream_uk)
    overall_stream_us_html = df_to_inline_html(overall_stream_us)

    overall_intake_html = df_to_inline_html(overall_intake)
    overall_intake_uk_html = df_to_inline_html(overall_intake_uk)
    overall_intake_us_html = df_to_inline_html(overall_intake_us)

    overall_otp_html = df_to_inline_html(overall_otp)
    overall_passport_html = df_to_inline_html(overall_passport)
    overall_hybrid_html = df_to_inline_html(overall_hybrid)
    overall_hybrid_uk_html = df_to_inline_html(overall_hybrid_uk)
    overall_hybrid_us_html = df_to_inline_html(overall_hybrid_us)

    overall_channel_html = df_to_inline_html(overall_channel)
    overall_channel_uk_html = df_to_inline_html(overall_channel_uk)
    overall_channel_us_html = df_to_inline_html(overall_channel_us)

    # Email date formatting
    yesterday_dt = today_date - timedelta(days=1)
    day_suffix = lambda d: "th" if 11<=d<=13 else {1:"st",2:"nd",3:"rd"}.get(d%10,"th")
    formatted_date = f"{yesterday_dt.day}{day_suffix(yesterday_dt.day)} {yesterday_dt.strftime('%B %Y')}"

    # Email body
    body = f"""
    <html><body>
    <p>Hi Team,<br><br>Please find below the UniScholars lead parameter metrics.</p>
    <p><b>1. Level of Study - Overall</b><br>{overall_stream_html}</p>
    <p><b>2. Level of Study - UK</b><br>{overall_stream_uk_html}</p>
    <p><b>3. Level of Study - US</b><br>{overall_stream_us_html}</p>
    <p><b>4. OTP Verified</b><br>{overall_otp_html}</p>
    <p><b>5. Intake - Overall</b><br>{overall_intake_html}</p>
    <p><b>6. Intake - UK</b><br>{overall_intake_uk_html}</p>
    <p><b>7. Intake - US</b><br>{overall_intake_us_html}</p>
    <p><b>8. Passport</b><br>{overall_passport_html}</p>
    <p><b>9. Hybrid Branch - Overall</b><br>{overall_hybrid_html}</p>
    <p><b>10. Hybrid Branch - UK</b><br>{overall_hybrid_uk_html}</p>
    <p><b>11. Hybrid Branch - US</b><br>{overall_hybrid_us_html}</p>
    <p><b>12. Channel - Overall</b><br>{overall_channel_html}</p>
    <p><b>13. Channel - UK</b><br>{overall_channel_uk_html}</p>
    <p><b>14. Channel - US</b><br>{overall_channel_us_html}</p>
    <p>Best regards,<br></p></body></html>
    """
    
    # ---------------- SEND EMAIL FUNCTION ----------------
    def send_email(subject, body, recipients):
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        # NOTE: The password 'vcjswljkfhiikmld' is a hardcoded App Password from the original script.
        # In a real-world scenario, this should be stored securely (e.g., in a secret manager or Airflow connection).
        server.login('nishant.sharma@uniacco.com', 'vcjswljkfhiikmld')
        message = MIMEMultipart()
        message['From'] = 'nishant.sharma@uniacco.com'
        message['To'] = ', '.join(recipients)
        message['Subject'] = subject
        message.attach(MIMEText(body, 'html'))
        server.sendmail(message['From'], recipients, message.as_string())
        server.quit()

    # Fallback error email function
    def send_basic_error_email(subject_prefix, error_message):
        error_subject = f"ERROR: {subject_prefix} - UniScholars ETL"
        error_body = f"The UniScholars ETL job failed. Details:\n{error_message}"
        error_recipients = ["data@uniacco.com"] # Only send to data team for errors
        try:
            send_email(error_subject, error_body, error_recipients)
        except Exception as e:
            logging.error(f"FATAL: Failed to send error email: {e}")

    # ---------------- SEND FINAL METRICS EMAIL ----------------
    subject = f"UniScholars Lead Basic Metrics - {formatted_date}"
    recipient_emails = ['aman.rohada@uniacco.com','nishant.sharma@uniacco.com']
#     recipient_emails = ['aman.rohada@uniacco.com','prem@uniacco.com','abhishek@uniacco.com','data@uniacco.com',
#         'harpritsingh.bhamara@unischolars.com','performance@uniacco.com',
#         'anupam.gupta@uniacco.com','sara.solkar@unischolars.com',
#         'shreeya.dalvi@unischolars.com','shubham.pardeshi@unischolars.com',
#         'anaa.shaikh@unischolarz.com','saurab.rao@unischolars.com']

    try:
        send_email(subject, body, recipient_emails)
        logging.info("Metrics email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send final metrics email: {e}")
        send_basic_error_email("Final Metrics Email Failed", f"The attempt to send the metrics email failed. Error: {e}")

    logging.info("UniScholars ETL email job Completed")
