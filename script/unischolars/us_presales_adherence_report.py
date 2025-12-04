#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"
REDSHIFT_PORT = 5439

def run_query():

    logging.info("Starting UniScholars ETL email job")

    # ------------- Redshift Connection -------------
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DB,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    # ---------------- SQL QUERY --------------------
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

    warnings.filterwarnings("ignore")
    df['lead_created_at'] = pd.to_datetime(df['lead_created_at']).dt.date

    # ---------------- GOOGLE SHEETS AUTH ----------------
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        '/opt/airflow/dags/current/credentials/credential.json', scope
    )
    gc = gspread.authorize(credentials)

    ws1 = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1b4_DGN1DlSEclc-P9guyZOT-2ZQIUC_qO3VR-fP4klc/edit?usp=sharing"
    ).worksheet("Dump")

    gd.set_with_dataframe(ws1, df, row=1, col=1)
    logging.info("Data dumped to Google Sheet successfully")

    # ---------------- SEND EMAIL ----------------
    def send_email(subject, body, recipients):
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login('nishant.sharma@uniacco.com', 'vcjswljkfhiikmld')
        message = MIMEMultipart()
        message['From'] = 'nishant.sharma@uniacco.com'
        message['To'] = ', '.join(recipients)
        message['Subject'] = subject
        message.attach(MIMEText(body, 'html'))
        server.sendmail(message['From'], recipients, message.as_string())
        server.quit()

    subject = "UniScholars Lead Basic Metrics"
    body = "<p>Hi Team,<br><br>Data pipeline completed & Google Sheet updated.</p>"

    recipient_emails = ["data@uniacco.com"]
    send_email(subject, body, recipient_emails)

    logging.info("Email sent successfully, ETL Completed")


