#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 13:59:15 2025

@author: rikcrijns
"""

# -*- 
import re
import os
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from flask import Flask
import sys
import traceback
import chardet
import io
import json
from waiting_order_leads_upload_telforce_api import process_and_upload_leads
import logging
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from io import BytesIO
import mysql.connector
from mysql.connector import Error
from google.cloud import logging as cloud_logging
from google.cloud import secretmanager
from collections import defaultdict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from paramiko import Transport, SFTPClient
import time
import fnmatch

# Global session initialized here
session = None

app = Flask(__name__)

def setup_google_cloud_logging():
    # Instantiates a client
    client = cloud_logging.Client()

    # Retrieves a Cloud Logging handler based on the environment
    # and integrates the handler with the Python logging module.
    # This captures all logs at INFO level and higher.
    client.setup_logging()

    # You can still set up additional handlers for local output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Get the root logger and attach both handlers
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)

    return root_logger

# Initialize the logger
logger = setup_google_cloud_logging()

# Correcting the exception handling to log with severity
def log_uncaught_exceptions(ex_cls, ex, tb):
    logging.error(''.join(traceback.format_tb(tb)))
    logging.error(f'{ex_cls.__name__}: {str(ex)}')

sys.excepthook = log_uncaught_exceptions

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to config.json
config_path = os.path.join(script_dir, 'config.json')

# Load configuration from JSON file
with open(config_path) as config_file:
    config = json.load(config_file)


def access_secret_version(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/637358609369/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload


# Fetch credentials from Secret Manager
credentials_json = access_secret_version("telforce_api_credentials")
credentials_merkle_sftp_json = access_secret_version("Merkle_SFTP_credentials")


# Parse the credentials (assuming they are in JSON format)
credentials = json.loads(credentials_json)
credentials_merkle_sftp = json.loads(credentials_merkle_sftp_json)


# API Base URL
api_url = config['api_url']

# Common API parameters that are always included
api_params_common = config['common_api_params']

# Merge credentials into the config dictionary
api_params_common.update(credentials)

retentie_variants = config["retentie_variants"]

def setup_global_session(retries=3, backoff_factor=1.0, status_forcelist=(500, 502, 503, 504)):
    """
    Set up a global session with retry and connection pooling.
    """
    global session  # Use the global variable
    session = requests.Session()  # Create a session object
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['GET', 'POST'])  # Ensure retrying on GET and POST requests
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

# Call the function to set up the session at the start
setup_global_session()


def build_drive_service():
    # Define the scopes required for the Google Drive service
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    # Obtain default credentials for the Cloud Run environment
    credentials, _ = default(scopes=scopes)

    # Explicitly request the credentials to refresh if they are not already valid
    if not credentials.valid:
        if credentials.requires_scopes:
            credentials = credentials.with_scopes(scopes)
    
    # Build the service client using the obtained credentials
    service = build('drive', 'v3', credentials=credentials)
    return service

def upload_file_to_drive(service, file_name, folder_id):
    logging.info(f"Attempting to upload file to folder ID: {folder_id}")
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_name, mimetype='text/plain')
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True).execute()
        logging.info(f"File ID: {file['id']} uploaded to Google Drive.")
        return file['id']
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        return None



def get_latest_file_sftp():
    try:
        # Define constants for SFTP connection
        SFTP_HOST = credentials_merkle_sftp["sftp_host"]
        SFTP_PORT = int(credentials_merkle_sftp["sftp_port"])
        SFTP_USERNAME = credentials_merkle_sftp["sftp_username"]
        SFTP_PASSWORD = credentials_merkle_sftp["sftp_password"]
        SFTP_DIRECTORY = credentials_merkle_sftp["sftp_directory"]
        
        transport = Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = SFTPClient.from_transport(transport)
        
        # Construct file name pattern to match
        file_pattern = "Belbestand_waitingorders*.csv"
        logging.info(f"Searching for files matching pattern: {file_pattern}")
        
        # Debugging output
        print(f"Searching for files matching pattern: {file_pattern}")
        
        # Get list of files in SFTP directory
        files_in_directory = sftp.listdir_attr(SFTP_DIRECTORY)
        
        # Filter files matching the pattern and find the latest file by modification time
        latest_file = None
        latest_mtime = 0
        for file_info in files_in_directory:
            if fnmatch.fnmatch(file_info.filename, file_pattern):
                if file_info.st_mtime > latest_mtime:
                    latest_file = file_info
                    latest_mtime = file_info.st_mtime

        # If no matching file is found, return an empty DataFrame and None
        if not latest_file:
            print("No matching files found.")
            return pd.DataFrame(), None

        # Open the latest file and process it
        latest_file_path = f"{SFTP_DIRECTORY.rstrip('/')}/{latest_file.filename}"
        print(f"Latest file found: {latest_file.filename}, modified at {latest_file.st_mtime}")
        
        with sftp.open(latest_file_path) as file:
            df = pd.read_csv(file, delimiter=';', dtype=str)
        # Close SFTP connection
        sftp.close()
        transport.close()
        
        # Return the DataFrame and filename
        return df, latest_file.filename
    
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame(), None  # Return an empty DataFrame and None on error
    
def extract_datetime_from_filename(filename):
    # Extract the base filename from the full filepath
    
    # Regex to find datetime pattern YYYYMMDD_HHMMSS in the filename
    match = re.search(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})', filename)
    if match:
        datetime_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)} {match.group(4)}:{match.group(5)}:{match.group(6)}"
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    return None

def wo_leads_telforce_check(list_id):
    # Initialize connection and cursor outside of try to close them in finally block if they are open
    connection = None
    cursor = None
    
    json_credentials = access_secret_version("telforce_database_credentials")
    credentials = json.loads(json_credentials)

    host = credentials["host"]
    user = credentials["user"]
    database = credentials["database"]
    password = credentials["password"]
    
    try:
        # Set up the connection to the MySQL database
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password  
        )
        cursor = connection.cursor()
        
        # Check if connected to MySQL and print server version
        if connection.is_connected():
            db_info = connection.get_server_info()
            logging.info(f"Connected to MySQL Server version {db_info}")

            # Prepare the SQL query using the phone_number provided
            query = f"""
            SELECT
            	v.phone_number AS 'phonenumber',
            	v.lead_id AS 'lead id',
            	v.last_local_call_time AS 'last local call time',
            	v.status AS 'status',
            	c.RESPONSEDATUM AS 'responsdatum'
            	
            FROM
            	vicidial_list v
                
            LEFT JOIN custom_{list_id} c ON v.lead_id = c.lead_id
            JOIN vicidial_users u ON v.user = u.user
            	
            WHERE
                v.entry_list_id = {list_id}
                AND v.entry_date > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)      
            """
            # Execute the query
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()

            # Define DataFrame to store the fetched results
            results_df = pd.DataFrame(results, columns=['phone_number', 'lead_id', 'last_local_call_time', 'status', 'responsedatum'])

            # Convert 'last_local_call_time' to local timezone (Europe/Amsterdam)
            results_df['responsedatum'] = results_df['responsedatum'].str.strip()
            results_df['responsedatum'] = pd.to_datetime(results_df['responsedatum'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            results_df['responsedatum'] = results_df['responsedatum'].dt.tz_localize('UTC').dt.tz_convert('Europe/Amsterdam')
            results_df['last_local_call_time'] = pd.to_datetime(results_df['last_local_call_time']).dt.tz_localize('UTC').dt.tz_convert('Europe/Amsterdam')
            results_df['phone_number'] = results_df['phone_number'].astype(str)
            results_df['lead_id'] = results_df['lead_id'].astype(str)
            
            return results_df
    except mysql.connector.Error as e:
        logging.error(f"Database connection failed: {e}")
        return pd.DataFrame()
    finally:
        # Close cursor and connection if they were successfully opened
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def update_lead_status_batch(lead_ids, new_status):
    lead_id_list = ','.join(map(str, lead_ids))
    leads_count = len(lead_ids)
    """Function to update the status of a lead based on the phone number."""
    api_params = {**api_params_common, 'function': 'batch_update_lead', 'lead_ids':lead_id_list, 'status': new_status}
    response = requests.get(api_url, params=api_params)
    logging.info(f"Updating status for {leads_count} leads to {new_status}... Status code: {response.status_code}")
    logging.info(f"Lead ids: {lead_id_list}")
    if response.ok and 'SUCCESS:' in response.text:
        logging.info(f"{leads_count} leads updated successfully to {new_status}.")
        return {'success': True, 'lead_id': lead_id_list, 'message': 'Lead updated successfully'}
    # logging.info(f"Failed to update leads {lead_id_list}: {response.text}")
    return {'success': False, 'lead_id': lead_id_list, 'error': response.text}




def load_leads_from_csv(df):
    leads = {}

    cleaned_df = pd.DataFrame()  # Initialize an empty DataFrame for cleaned data

    # Initialize a set to track unique phone numbers
    unique_phone_numbers = set()

    rows_to_keep = []  # List to keep track of valid rows for the DataFrame
    

    for index, row in df.iterrows():
        # Extract and clean phone number

            
        phone_number = str(row['CONTACT_TELEFOONNR']).strip()
        phone_number = ''.join(filter(str.isdigit, phone_number))  # Keep only digits
        
        # Ensure phone number is not empty and not a duplicate
        if phone_number and phone_number not in unique_phone_numbers:
            try:
                # Attempt to parse the response date
                response_date = pd.to_datetime(row['RESPONSDATUM'].strip(), dayfirst=True, errors='coerce')

                # Only add to leads and DataFrame if the response_date is valid
                if pd.notnull(response_date):
                        leads[phone_number] = {
                            'phone_number': phone_number,
                            'status': 'NEW',
                            'response_date': response_date
                        }
                        # Add this row index to the list of rows to keep
                        rows_to_keep.append(index)
                        # Add phone number to the set of unique phone numbers
                        unique_phone_numbers.add(phone_number)
                else:
                    logging.info(f"Skipping row {index} due to invalid response date for phone number {phone_number}.")
            except Exception as e:
                logging.info(f"Error processing row {index} for phone number {phone_number}: {e}")
        else:
            logging.info(f"Duplicate or empty phone number found: {phone_number} at row {index}. Skipping this entry.")
    
    # Create a cleaned DataFrame by selecting only the rows that are in the rows_to_keep list
    cleaned_df = df.loc[rows_to_keep].reset_index(drop=True)
    cleaned_df['RESPONSDATUM'] = pd.to_datetime(cleaned_df['RESPONSDATUM'], format='%d-%b-%y', errors='coerce').dt.tz_localize('UTC').dt.tz_convert('Europe/Amsterdam')
    


    
    return leads, cleaned_df

def download_split_wo_csv(df):
    try:
        df_220 = df[df['CAMPAGNE_CODE'].isin(retentie_variants)]
        df_216 = df[~df['CAMPAGNE_CODE'].isin(retentie_variants)]
        
        df_220['list_id'] = 220
        df_216['list_id'] = 216
        
    except pd.errors.EmptyDataError as e:
        logging.error(f"Error loading CSV file: {e}")
    except UnicodeDecodeError as e:
        logging.error(f"Error decoding CSV file: {e}")
    
    return [df_216, df_220]

def process_daily_list(df, new_leads, filename):
    list_id = int(df['list_id'].iloc[0])
    local_zone = pytz.timezone('Europe/Amsterdam')
    now = datetime.now(pytz.timezone('Europe/Amsterdam'))

    logging.info('Loading Telforce leads')
    leads_db = wo_leads_telforce_check(list_id)
    db_leads_count = len(leads_db)
    logging.info(f'Telforce contains {db_leads_count} leads')

    logging.info('Loading Waiting order csv file leads')
    
    
    
    new_leads_count = len(new_leads)
    
    time_condition_callable = ((now - df['RESPONSDATUM'] > timedelta(hours=36)) & (now - df['RESPONSDATUM'] < timedelta(days=7)))

    callable_leads_wo_file = df[time_condition_callable]['CONTACT_TELEFOONNR'].tolist()
    callable_leads_wo_file_count = len(callable_leads_wo_file)
    

    logging.info(f'Waiting Order CSV file contains {new_leads_count} unique leads')
    
    # Ensure all keys (phone numbers) are strings and clean
    leads_db['phone_number'] = leads_db['phone_number'].astype(str).str.strip()
    leads_db['lead_id'] = leads_db['lead_id'].astype(str).str.strip()
    
    df['CONTACT_TELEFOONNR'] = df['CONTACT_TELEFOONNR'].astype(str).str.strip()

    file_datetime = extract_datetime_from_filename(filename)
    
    upload_leads = pd.DataFrame()
    update_leads_EBO, update_leads_BOD, update_leads_SGA, update_leads_EBA = [],[],[],[]
    
    
    logging.info("Comparing Waiting order csv file leads against Telforce")
    
    phone_numbers_list = df['CONTACT_TELEFOONNR'].tolist()
    logging.info(f"Unique phone numbers in WO leads list: {phone_numbers_list}")
        
    mask_new_leads = ~df['CONTACT_TELEFOONNR'].isin(leads_db['phone_number'])
    # Filter and process new leads
    new_leads_upload = df[mask_new_leads]
    time_condition = (now - new_leads_upload['RESPONSDATUM'] > timedelta(hours=36)) & (now - new_leads_upload['RESPONSDATUM'] < timedelta(days=7))
    new_leads_upload = new_leads_upload[time_condition]
    upload_leads_phone_number_list = new_leads_upload['CONTACT_TELEFOONNR'].tolist()
    upload_leads_phone_number_list_count = len(upload_leads_phone_number_list)
    
    logging.info(f"{df['RESPONSDATUM']}")
    logging.info(f"{leads_db['responsedatum']}")

    
    if not new_leads_upload.empty:
        logging.info(f"Uploading {upload_leads_phone_number_list_count} leads to Telforce")
        upload_leads = pd.concat([upload_leads, new_leads_upload])
        
        
     
    leads_both_mask = leads_db['phone_number'].isin(df['CONTACT_TELEFOONNR'])
    leads_both = leads_db[leads_both_mask]

    wrong_status_leads = leads_both.loc[(leads_both['status'] == 'SGA') & (now - leads_both['responsedatum'] < timedelta(days=7))]

    if not wrong_status_leads.empty:
        wrong_status_leads_list = wrong_status_leads['lead_id'].tolist()
        wrong_status_leads_list_count = len(wrong_status_leads_list)
        logging.info(f"{wrong_status_leads_list_count} Leads are incorrectly set to SGA")
        update_leads_EBO.append(wrong_status_leads_list)
        logging.info("Setting these leads back to EBO")
        
    # Using .loc with directly embedded conditions for leads_both_bga
    leads_both_bga = leads_both.loc[
        (leads_both['status'] == 'BGA') &
        (now - leads_both['responsedatum'] < timedelta(days=7)) &
        (now - leads_both['last_local_call_time'] > timedelta(hours=36))
    ]
    
    # Using .loc with directly embedded conditions for leads_both_bod
    leads_both_bod = leads_both.loc[
        (leads_both['status'] == 'BOD') &
        (now - leads_both['responsedatum'] < timedelta(days=7)) &
        (now - leads_both['last_local_call_time'] > timedelta(hours=36))
    ]
    
    if not leads_both_bga.empty:
        leads_both_bga_list = leads_both_bga['lead_id'].tolist()
        logging.info("Setting {leads_both_bga_list} to EBA")
        update_leads_EBA.append(leads_both_bga_list)

    if not leads_both_bod.empty:
        leads_both_bod_list = leads_both_bod['lead_id'].tolist()
        logging.info("Setting {leads_both_bod_list} to EBO")
        update_leads_EBO.append(leads_both_bod_list)
        
    leads_old_mask = ~leads_db['phone_number'].isin(df['CONTACT_TELEFOONNR'])
    old_leads_status_condition = ~leads_db['status'].isin(['SGA'])
    
    older_than_7_days_mask =  (now - leads_db['responsedatum']) > timedelta(days=7)
    
    # Combine masks using the logical AND operator and apply to DataFrame
    old_leads = leads_db[(leads_old_mask & old_leads_status_condition) | (leads_both_mask & older_than_7_days_mask)]
    
    if not old_leads.empty:
        old_leads_list = old_leads['lead_id'].tolist()
        old_leads_list_count = len(old_leads_list)
        update_leads_SGA.append(old_leads_list)
   
    if not upload_leads.empty:
        process_and_upload_leads(upload_leads, session) 
        
    upload_leads_count = len(upload_leads)
    logging.info(f'Uploading {upload_leads_count} new leads to Telforce')
   
    if update_leads_EBO:
        update_lead_status_batch(update_leads_EBO, "EBO")
    if update_leads_BOD:
        update_lead_status_batch(update_leads_BOD, "BOD")
    if update_leads_SGA:
        update_lead_status_batch(update_leads_SGA, "SGA")
    if update_leads_EBA:
        update_lead_status_batch(update_leads_EBA, "EBA")
        
    results_sga_count, results_count = len(update_leads_SGA), len(update_leads_EBO) + len(update_leads_EBA)
    logging.info(f'{results_sga_count} leads are updated to SGA in Telforce')
    logging.info(f'{results_count} leads are updated to EBA or EBO in Telforce')
    
    leads_db = wo_leads_telforce_check(list_id)

    callabale_leads_mask = ~leads_db['status'].isin(['SGA'])
    callable_leads_mask_no_zs = ~leads_db['status'].isin(['SGA','ZS'])

    callable_leads = leads_db[callabale_leads_mask]['lead_id'].tolist()
    callable_leads_count = len(callable_leads)
    
    callable_leads_no_zs = leads_db[callable_leads_mask_no_zs]['lead_id'].tolist() 
    callable_leads_count_no_zs = len(callable_leads_no_zs)
    
    
    logging.info(f'There are, with ZS, {callable_leads_count} leads callable in Telforce')
    logging.info(f'There are, without ZS, {callable_leads_count_no_zs} leads callable in Telforce')
    logging.info(f'There are, {callable_leads_wo_file_count} unique callable leads in the latest WO file')
    
    if callable_leads_count != callable_leads_wo_file_count:
        logging.warning("The number of callable leads in Telforce is not equal to the WO leads list:")
        logging.warning(f"WO:{new_leads_count}, Telforce:{callable_leads_count_no_zs}")

    logging.info("Finished processing daily list.")


@app.route('/')
def main():
    folder_id = "17eBCh1OEt8aQxo0sUm_I94mFRgVVClgm"
    try:
        # Setup Google Drive service
        service = build_drive_service()
        
        latest_file_content, latest_file_name = get_latest_file_sftp()
        if not latest_file_content.empty:
            logging.info(f"Selected file: {latest_file_name}")
            dfs = download_split_wo_csv(latest_file_content)
            for df in dfs:
                leads, cleaned_df = load_leads_from_csv(df)
                process_daily_list(cleaned_df, leads, latest_file_name)

        else:
            logging.debug("No file selected.")
    except Exception as e:
        logging.exception("An error occurred during execution.")
    return "Script executed successfully."

if __name__ == "__main__":
    logging.info("Starting the application.")
    port = int(os.environ.get('PORT', 8080))
    app.debug = True
    app.run(host='0.0.0.0', port=port)
