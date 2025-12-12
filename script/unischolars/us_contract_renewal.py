import logging
import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread_dataframe as gd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date

SHEET_URL = "https://docs.google.com/spreadsheets/d/1M8OwlA010-ac3jCqbhLRBnlEhKKUH0yAfNEchDQ2UlQ/edit?usp=sharing"
SOURCE_WORKSHEET_NAME = "Sheet1"       
TRACKER_WORKSHEET_NAME = "mail_checker"
GSPREAD_CREDENTIALS_PATH = '/opt/airflow/secrets/credential.json'

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "nishant.sharma@uniacco.com")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "vcjswljkfhiikmld") # ⬅️ REPLACE THIS SECURELY

STATIC_RECIPIENTS = [
    'data@uniacco.com', 'matheen@unischolars.com', 'parth.soni@uniacco.com', 'viral.sharma@unischolars.com', 'kajol.nagori@uniacco.com']

COLUMNS_TO_MATCH = ['Name of First Party', 'Renewal Date', 'First Reminder', 'Second Reminder']

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

def read_and_clean_sheet(gc, sheet_name: str, is_tracker: bool = False) -> pd.DataFrame:
    try:
        ws = gc.open_by_url(SHEET_URL).worksheet(sheet_name)
        data_values = ws.get_all_values()
        
        if not data_values:
            logger.warning(f"Worksheet '{sheet_name}' is empty.")
            return pd.DataFrame()

        headers = data_values.pop(0)
        df = pd.DataFrame(data_values, columns=headers)
        
        date_cols = ['Renewal Date', 'First Reminder', 'Second Reminder']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        
        logger.info(f"Read {len(df)} rows from '{sheet_name}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to read/clean data from worksheet '{sheet_name}': {e}")
        raise

def send_renewal_email(row, smtp_server, smtp_port, sender_email, sender_password):
    
    try:
        party_name = row['Name of First Party']
        renewal_date = row['Renewal Date'].strftime('%Y-%m-%d')
        
        recipient_emails = STATIC_RECIPIENTS
        
        subject = f"Reminder: Upcoming Contract Renewal: {party_name}"
        body = f"""
Hi,

I hope you're doing well. I wanted to send a quick reminder that the contract renewal for the following is pending:
    
Name: {party_name}
Renewal Date: {renewal_date}
    
Your prompt attention to this matter would be greatly appreciated.

Best regards,
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
        
        logger.info(f"Email sent successfully for contract: {party_name} (Renewal Date: {renewal_date})")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email for contract {party_name}: {e}", exc_info=True)
        return False

def run_contract_renewal_etl():
    gc = None
    
    try:
        today = date.today()
        logger.info(f"Starting Contract Renewal ETL for date: {today}")

        gc = get_google_sheet_client()
        source_df = read_and_clean_sheet(gc, SOURCE_WORKSHEET_NAME)
        tracker_df = read_and_clean_sheet(gc, TRACKER_WORKSHEET_NAME, is_tracker=True)
        
        if source_df.empty:
            logger.info("Source contract list is empty. Exiting.")
            return

        filtered_df = source_df[
            (source_df['First Reminder'] == today) | (source_df['Second Reminder'] == today)].copy()
        
        if filtered_df.empty:
            logger.info("No contracts require a reminder today. Exiting.")
            return

        match_cols = [col for col in COLUMNS_TO_MATCH if col in filtered_df.columns and col in tracker_df.columns]
        
        merged_df = filtered_df.merge(
            tracker_df, on=match_cols, how='outer', indicator=True
        )
        
        result_df = merged_df[merged_df['_merge'] == 'left_only'].copy()
        unsent_df = result_df[filtered_df.columns].drop_duplicates().reset_index(drop=True)
        
        logger.info(f"Found {len(unsent_df)} unique reminders to send today.")

        if unsent_df.empty:
            logger.info("All contracts requiring reminders today have already been sent. Exiting.")
            return

        sent_emails_list = []
        for index, row in unsent_df.iterrows():
            if send_renewal_email(row, SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, SENDER_PASSWORD):
                row_data = row.to_dict()
                row_data['Renewal Date'] = row_data['Renewal Date'] # Already date object
                row_data['First Reminder'] = row_data['First Reminder'] # Already date object
                row_data['Second Reminder'] = row_data['Second Reminder'] # Already date object
                sent_emails_list.append(row_data)

        sent_emails_df = pd.DataFrame(sent_emails_list, columns=tracker_df.columns)
        
        if not sent_emails_df.empty:
            final_tracker_df = pd.concat([tracker_df, sent_emails_df], ignore_index=True)
            
            for col in ['Renewal Date', 'First Reminder', 'Second Reminder']:
                if col in final_tracker_df.columns:
                    final_tracker_df[col] = final_tracker_df[col].astype(str)
            
            ws1 = gc.open_by_url(SHEET_URL).worksheet(TRACKER_WORKSHEET_NAME)
            
            gd.set_with_dataframe(
                worksheet=ws1, dataframe=final_tracker_df, row=1, col=1, include_index=False, include_column_header=True, resize=False)
            logger.info(f"Successfully updated '{TRACKER_WORKSHEET_NAME}' with {len(sent_emails_df)} new entries.")
        
        logger.info("🏁 Contract Renewal Notifier ETL completed successfully.")
        
    except Exception as e:
        logger.error(f"A critical error occurred during the Renewal Notifier ETL.", exc_info=True)
        raise
