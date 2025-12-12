import logging
import os
import pandas as pd
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from typing import List

SHEET_URL = "https://docs.google.com/spreadsheets/d/1msg2xxl6TRs0rYtbfzse0eUBTEY1d3IJZ3s0vRAdobM/edit?usp=sharing"
TRACKER_WORKSHEET_NAME = "new_m_checker"       
SOURCE_WORKSHEET_NAME = "Form responses 1"    
GSPREAD_CREDENTIALS_PATH = '/opt/airflow/secrets/credential.json' 

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "ayush.mangwani@uniacco.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "hfckbyqeiuapisla") 

STATIC_RECIPIENTS = ['karan.jadhav@uniacco.com', 'sagar@uniacco.com', 'managers@uniacco.com', 'revenue@uniacco.com',
    'bhavik.bhadra@uniacco.com', 'aftersales@uniacco.com', 'nikhil.menon@uniacco.com', 'ayush.mangwani@uniacco.com']

COLUMNS_TO_MATCH = ['Lead Link', 'Payment Date', 'Agent Name', 'Refund Reason', 
    'Explain The Scenario In Detail', 'Email address', 'Platform Fee Transaction ID',
    'Deposit Transaction ID', 'Platform Fee Amount', 'Deposit Amount']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

def get_google_sheet_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_CREDENTIALS_PATH, scope)
        gc = gspread.authorize(credentials)
        logger.info("Google Sheet client authorized successfully.")
        return gc
    except Exception as e:
        logger.error(f"Failed to authorize Google Sheet client: {e}")
        raise

def read_sheet_to_df(gc, sheet_name: str) -> pd.DataFrame:
    try:
        ws = gc.open_by_url(SHEET_URL).worksheet(sheet_name)
        data_values = ws.get_all_values()
        
        if not data_values:
            logger.warning(f"Worksheet '{sheet_name}' is empty.")
            return pd.DataFrame()

        headers = data_values.pop(0)
        df = pd.DataFrame(data_values, columns=headers)
        
        str_cols = [col for col in headers if col not in ['Payment Date']]
        df = df.astype({col: 'string' for col in str_cols}, errors='ignore')
        
        if 'Payment Date' in df.columns:
            df['Payment Date'] = pd.to_datetime(df['Payment Date'], errors='coerce')
        
        logger.info(f"Read {len(df)} rows from '{sheet_name}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to read data from worksheet '{sheet_name}': {e}")
        raise

def find_unsent_requests(form_df: pd.DataFrame, tracker_df: pd.DataFrame) -> pd.DataFrame:
    match_cols = [col for col in COLUMNS_TO_MATCH if col in form_df.columns and col in tracker_df.columns]
    
    if not match_cols:
        logger.error("Required columns for matching are missing from one or both sheets.")
        raise ValueError("Missing essential columns for merge operation.")
        
    logger.info(f"Matching on columns: {match_cols}")

    merged_df = form_df.merge(tracker_df, on=match_cols, how='outer', indicator=True, suffixes=('_form', '_tracker'))
    
    unsent_df = merged_df[merged_df['_merge'] == 'left_only'].copy()
    unsent_df = unsent_df.drop('_merge', axis=1)
    unsent_df = unsent_df[[col for col in form_df.columns]]
    
    logger.info(f"Found {len(unsent_df)} new (unsent) refund requests.")
    return unsent_df

def send_approval_email(row, smtp_server, smtp_port, sender_email, sender_password):
    try:
        lead_link = row['Lead Link']
        payment_date = row['Payment Date'].strftime('%Y-%m-%d') if pd.notna(row['Payment Date']) else 'N/A'
        agent_name = row['Agent Name']
        refund_reason = row['Refund Reason']
        detail_reason = row['Explain The Scenario In Detail']
        agent_id = row['Email address'] # Assuming 'Email address' is the submitting agent's email
        pf_id = row['Platform Fee Transaction ID']
        deposit_id = row['Deposit Transaction ID']
        pf_amount = row['Platform Fee Amount']
        deposit_amount = row['Deposit Amount']
        
        recipient_emails = [agent_id] + STATIC_RECIPIENTS
        
        subject = f"Request for Approval: Platform Fee Refund for Lead by {agent_name}"
        body = f"""
Hi,

I hope this message finds you well. I'm reaching out to request your approval for a platform fee refund concerning the following lead:
    
Lead Link: {lead_link}
Payment Date: {payment_date}
Agent Name: {agent_name}
Refund Reason: {refund_reason}
Detail Reason: {detail_reason}
PF Transaction ID: {pf_id}
PF Amount: {pf_amount}
Deposit Transaction ID: {deposit_id}
Deposit Amount: {deposit_amount}
    
Your prompt attention to this matter would be greatly appreciated.

Best regards,
{agent_name}
"""
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)

        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(recipient_emails)
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))

        server.sendmail(sender_email, recipient_emails, message.as_string())
        server.quit()
        
        logger.info(f"Email sent successfully for Lead Link: {lead_link}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email for Lead Link {lead_link}: {e}", exc_info=True)
        return False


def run_refund_email_etl():
    gc = None
    
    try:
        gc = get_google_sheet_client()

        form_df = read_sheet_to_df(gc, SOURCE_WORKSHEET_NAME)
        tracker_df = read_sheet_to_df(gc, TRACKER_WORKSHEET_NAME)
        
        if form_df.empty:
            logger.info("Form submission sheet is empty. No new emails to send.")
            return

        unsent_df = find_unsent_requests(form_df, tracker_df)

        if unsent_df.empty:
            logger.info("No new refund requests found to send emails for.")
            return

        sent_emails_list = []
        for index, row in unsent_df.iterrows():
            if send_approval_email(row, SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, SENDER_PASSWORD):
                row_data = row.to_dict()
                row_data['Payment Date'] = row_data['Payment Date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row_data['Payment Date']) else ''
                sent_emails_list.append(row_data)

        sent_emails_df = pd.DataFrame(sent_emails_list, columns=tracker_df.columns)
        
        if not sent_emails_df.empty:
            final_tracker_df = pd.concat([tracker_df, sent_emails_df], ignore_index=True)
            
            ws1 = gc.open_by_url(SHEET_URL).worksheet(TRACKER_WORKSHEET_NAME)
            
            gd.set_with_dataframe(
                worksheet=ws1, dataframe=final_tracker_df, row=1, col=1, include_index=False, include_column_header=True, resize=False)
            logger.info(f"Successfully updated '{TRACKER_WORKSHEET_NAME}' with {len(sent_emails_df)} new entries.")
        else:
            logger.info("No emails were successfully sent, skipping tracker sheet update.")

        logger.info("🏁 Refund Email Notification ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Email Notification ETL.", exc_info=True)
        raise
