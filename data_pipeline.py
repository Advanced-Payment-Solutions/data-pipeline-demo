import base64
import csv
import io
import os
import json
import tempfile
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from email.message import EmailMessage
from tqdm import tqdm

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

from supabase import create_client, Client
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import html

SUPABASE_URL = os.getenv('SUPABASE_URL')
print('SUPABASE_URL: ' + SUPABASE_URL)
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
print('SUPABASE_KEY: ' + SUPABASE_KEY)
BUCKET_NAME = os.getenv('BUCKET_NAME')
print('BUCKET_NAME: ' + BUCKET_NAME)
FILE_PATH = os.getenv('FILE_PATH')
print('FILE_PATH: ' + FILE_PATH)
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
print('REFRESH_TOKEN: ' + REFRESH_TOKEN)
CLIENT_ID = os.getenv('CLIENT_ID')
print('CLIENT_ID: ' + CLIENT_ID)
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
print('CLIENT_SECRET: ' + CLIENT_SECRET)
TOKEN_URI = os.getenv('TOKEN_URI')
print('TOKEN_URI: ' + TOKEN_URI)



# SUPABASE_URL="https://thxvfnachnpgmeottlem.supabase.co"
# SUPABASE_KEY= "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRoeHZmbmFjaG5wZ21lb3R0bGVtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Njg2ODQ1OSwiZXhwIjoyMDYyNDQ0NDU5fQ.TBgZdtH3INLZtpnraa4dfPbZ0hZHLdCoY1VKhqEv8FA"
# BUCKET_NAME= "apsbucket"
# FILE_PATH= "Configuration/config.xml"
# REFRESH_TOKEN= "1//0gqC0VG2Y-k7bCgYIARAAGBASNwF-L9IrRl9l5XlRt7ftwBQfy4XX86wx5m4yLCM_18tMkNy25uJk6P_wtF3KOa1liVdlak_Amt0"
# CLIENT_ID= "1096839893158-m2aosmj5oroum4soa9q1aj67l9tq9m74.apps.googleusercontent.com"
# CLIENT_SECRET= "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"
# TOKEN_URI= "https://oauth2.googleapis.com/token" 



#SUPABASE_URL= "https://thxvfnachnpgmeottlem.supabase.co"
#SUPABASE_KEY= "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRoeHZmbmFjaG5wZ21lb3R0bGVtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Njg2ODQ1OSwiZXhwIjoyMDYyNDQ0NDU5fQ.TBgZdtH3INLZtpnraa4dfPbZ0hZHLdCoY1VKhqEv8FA"
#BUCKET_NAME = "apsbucket"
#FILE_PATH ="Configuration/config.xml"

# Hardcoded credentials (client_id and client_secret)
#client_config = {
#    "installed": {
#        "client_id": "1096839893158-m2aosmj5oroum4soa9q1aj67l9tq9m74.apps.googleusercontent.com",
#        "project_id": "database-459719",
#        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#        "token_uri": "https://oauth2.googleapis.com/token",
#        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#        "client_secret": "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",
#        "redirect_uris": ["http://localhost"]
#    }} 

tables = {}  
uploaded_files = []
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
EMAIL_STATUS = False

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
]

def authenticate_gmail(GMAIL_TOKEN_PATH):
    refresh_token = REFRESH_TOKEN
    if not refresh_token:
        raise Exception("Missing REFRESH_TOKEN in environment variables")

    creds = Credentials(
        token=None,  # no access token yet
        refresh_token=refresh_token,
        token_uri=TOKEN_URI,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES
    )

    # Use the refresh token to get a fresh access token
    creds.refresh(Request())

    # Build and return the Gmail API client
    service = build('gmail', 'v1', credentials=creds)
    return service

######################################################################

def download_and_upload_attachments(bucket_name,table_name,sender,recipient,subjectdata,message_text):

    global EMAIL_STATUS
    token_data = load_json_from_supabase(BUCKET_NAME, 'Credentials/token.json', SUPABASE_URL, SUPABASE_KEY)
    if not token_data:
        print("Failed to load Gmail token from Supabase.")
        return
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        json.dump(token_data, tmp)
        tmp.flush()
        service = authenticate_gmail(tmp.name) 
        today = datetime.today().strftime('%Y/%m/%d')    
        #yesterday = (datetime.today() - timedelta(days=2)).strftime('%Y/%m/%d')    
        fileprocessdate=today
        query = f'after:{fileprocessdate} filename:csv has:attachment subject:"Dealer Transactions Report"'
        results = service.users().messages().list(userId='me', q=query).execute()
        messages = results.get('messages', [])
        removeexistingfiles(BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)        
        for message in messages:
                                msg = service.users().messages().get(userId='me', id=message['id']).execute()
                                headers = msg['payload'].get('headers', [])
                                subject = next((header['value'] for header in headers if header['name'] == 'Subject'), '(No Subject)')
                                for part in msg['payload'].get('parts', []):
                                 filename = part.get("filename") 
                                 body = part.get("body", {})       
                                 mime_type = part.get("mimeType", "")
                                 data = body.get("data")
                                 if mime_type == "text/html":
                                    from base64 import urlsafe_b64decode 
                                    decoded_body = urlsafe_b64decode(data).decode("utf-8")
                                    plain_text = extract_inner_text(decoded_body)
                                    print("Extracted text:\n", plain_text)   
                                 exists = check_row_exists(supabase_url=SUPABASE_URL,supabase_key=SUPABASE_KEY,table_name="TransactionLog", date_column="filedate",date_value=fileprocessdate,filename_column="filename",filename_value=filename)
                                 if exists:
                                    print("Row already exists duplicate excution for the same date")
                                 else: 
                                    print("Row does not exist for the date of " + fileprocessdate)
                                    if filename and 'attachmentId' in body:                                        
                                        att_id = body['attachmentId']
                                        attachment = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=att_id).execute()
                                        data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8')) 
                                        upload_file_to_supabase(data,'Data/'+filename,BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)                                     
                                        df = load_csv_from_supabase("apsbucket", 'Data/'+filename, SUPABASE_URL, SUPABASE_KEY)
                                        num_rows = len(df)
                                        print(num_rows)
                                        if df is not None: 
                                            mailsubject = subjectdata +' started for the '+ filename + ' with rows of ' + str(num_rows)                                           
                                            send_test_email(mailsubject,recipient,message_text)
                                            if EMAIL_STATUS:                                                
                                             push_data_supabase_database(df,SUPABASE_URL,SUPABASE_KEY,table_name)
                                             insert_file_record(SUPABASE_URL,SUPABASE_KEY,filename,fileprocessdate,'TransactionLog',num_rows,plain_text)                                             
                                             removeexistingfiles(BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)
                                             mailsubject = subjectdata +' completed for the '+ filename + ' with rows of ' + str(num_rows)
                                             send_test_email(mailsubject,recipient,message_text)
                                             EMAIL_STATUS = False
                                        else:
                                            print("Failed to load CSV from Supabase.")
def extract_inner_text(html_content):
    # Decode HTML entities
    decoded = html.unescape(html_content)    
    # Use BeautifulSoup to strip HTML tags
    soup = BeautifulSoup(decoded, "html.parser")
    return soup.get_text(separator="\n").strip()
    
def load_xml_config_from_supabase(FILE_PATH):
    try:
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        storage = supabase.storage.from_(BUCKET_NAME)        
        print(f"Attempting to download XML file: {FILE_PATH}")        
        # Download file bytes
        file_bytes = storage.download(FILE_PATH)        
        # Decode bytes to string
        xml_str = file_bytes.decode('utf-8')        
        # Parse XML string
        root = ET.fromstring(xml_str)        
        # Extract values from XML
        bucket_name = root.find('./supabase/bucketName').text
        table_name = root.find('./supabase/tableName').text
        sender = root.find('./mail/sender').text
        to = root.find('./mail/to').text
        subject = root.find('./mail/subject').text
        message_text = root.find('./mail/message_text').text
        ENV_TABLE_NAME = table_name    
        recipient=to
        subjectdata=subject
        message_text=message_text
        # Print extracted data
        print("Bucket Name:", bucket_name)
        print("Table Name:", ENV_TABLE_NAME)
        print("Sender:", sender)
        print("To:", recipient)
        print("Subject:", subjectdata)
        print("Message Text:", message_text)        
        return {
            "bucketName": bucket_name,
            "tableName": table_name,
            "sender": sender,
            "to": to,
            "subject": subject,
            "message_text": message_text
        }
        
    except Exception as e:
        print("Error downloading or parsing the XML file:", e)
        return None 

def check_row_exists(supabase_url: str, supabase_key: str, table_name: str,  date_column: str, date_value: str,filename_column: str,filename_value: str):
    supabase: Client = create_client(supabase_url, supabase_key)

    try:
        #response = supabase.table(table_name).select("*").eq(column_name, value).limit(1).execute()
        response = (
            supabase.table(table_name)
            .select("*")
            .eq(date_column, date_value)
            .eq(filename_column, filename_value)
            .limit(1)
            .execute()
        )
        print(response) 

        # ✅ This checks if at least one row was returned
        if response.data and isinstance(response.data, list) and len(response.data) > 0:
            print("✅ Row exists:", response.data[0])
            return True
        else:
            print("❌ No matching row found.")
            return False

    except Exception as e:
        print("❌ Error checking row existence:", e)
        return False

                                        
def insert_file_record(supabase_url: str, supabase_key: str, filename: str, date_str: str,table_name:str,num_rows:str,subject:str):
  
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
        data = {
            "filename": filename,
            "filedate": date_str,
            "num_rows": num_rows,
            "subject": subject
        }
        response = supabase.table(table_name).insert(data).execute()
        # ✅ Check if data was returned (insertion success)
        if response.data and isinstance(response.data, list):
            print("✅ File record inserted:", response.data[0])
            return response.data
        else:
            print("❌ Insert failed. Response:", response)
            return None
    except Exception as e:
        print("❌ Error during insertion:", e)
        return None
    
def load_json_from_supabase(bucket_name, file_path, supabase_url, supabase_key):
    supabase = create_client(supabase_url, supabase_key)
    storage = supabase.storage.from_(bucket_name)
    print(file_path)
    
    download_response = storage.download(file_path)

    if hasattr(download_response, "error") and download_response.error is not None:
        print(f"Error downloading file '{file_path}': {download_response.error}")
        return None
    try:
        file_bytes = download_response
        json_str = file_bytes.decode('utf-8')
        json_data = json.loads(json_str)
        return  json_data
        
    except Exception as e:
        print(f"Failed to parse JSON file '{file_path}': {e}")
        return None

def load_csv_from_supabase(bucket_name: str, file_path: str, supabase_url: str, supabase_key: str):
 
    supabase = create_client(supabase_url, supabase_key)
    storage = supabase.storage.from_(bucket_name)
    print(file_path)
    
    download_response = storage.download(file_path)
    
    if hasattr(download_response, "error") and download_response.error is not None:
        print(f"Error downloading file '{file_path}': {download_response.error}")
        return None
    
    try:
        file_bytes = download_response
        csv_str = file_bytes.decode('utf-8')
        csv_buffer = io.StringIO(csv_str)
        df = pd.read_csv(csv_buffer)
        return df
    except Exception as e:
        print(f"Failed to parse CSV file '{file_path}': {e}")
        return None

def removeexistingfiles(bucket_name, supabase_url, supabase_key):  
    bucket_name = "apsbucket"
    folder_path = "Data"

    supabase = create_client(supabase_url, supabase_key)
    storage = supabase.storage.from_(bucket_name)

    files_response = storage.list(path=folder_path)
    if hasattr(files_response, 'error') and files_response.error is not None:
        print(f"Error listing files: {files_response.error}")
        return False

    files = files_response.data if hasattr(files_response, 'data') else files_response
    file_paths = [f"{folder_path}/{file['name']}" for file in files if not file['name'].startswith(".")]

    if file_paths:
        BATCH_SIZE = 20
        for i in range(0, len(file_paths), BATCH_SIZE):
            batch = file_paths[i:i + BATCH_SIZE]
            delete_response = storage.remove(batch)
            if hasattr(delete_response, 'error') and delete_response.error is not None:
                print(f"Error deleting files {batch}: {delete_response.error}")
                return False
            print(f"Deleted files: {batch}")
    else:
        print("No deletable files found.")

    # Upload .keep file to maintain folder presence
    #storage.upload(f"{folder_path}/.keep", b"", {"content-type": "text/plain"})
    #print("Uploaded .keep file to preserve folder.")

    return True                                                  
######################################################################
def upload_file_to_supabase(file_data,filename, bucket_name, supabase_url, supabase_key):  
    supabase = create_client(supabase_url, supabase_key)
    storage = supabase.storage.from_(bucket_name)    

    # Step 3: Upload the new file
    file_path = filename  # or prefix with folder like f"csv_files/{filename}"
    upload_response = storage.upload(file_path, file_data)    
    if hasattr(upload_response, 'error') and upload_response.error is not None:
        print(f"Failed to upload file '{file_path}': {upload_response.error}")
        return False
    
    print(f"File '{file_path}' uploaded successfully to bucket '{bucket_name}'.")
    return True
###############################################################################
def push_data_supabase_database(data_list,SUPABASE_URL,SUPABASE_KEY,ENV_TABLE_NAME):
  
    # %%
    import pandas as pd

    # %%
    #df = pd.read_csv(data_list)
    df=data_list
    num_rows = len(df)
    print('push_data_supabase_database row count is : '+str(num_rows))
    # %%
    df_filtered = df.copy()

    # %%
    # List all column names in the April DataFrame
    print("Column names in April DataFrame:")
    print(df_filtered.columns.tolist())
    df_filtered =process_transaction_datetime(df_filtered)

    # %%
    columns_to_clean = ['Amount', 'TotalAmount', 'Surcharge', 'MSF', 'Tip', 'Cashout', 
                    'Extras', 'Levy', 'ServiceFee', 'TxnFee', 'Rebate']

    df_filtered[columns_to_clean] = (df_filtered[columns_to_clean]
                                    .replace(r'[\$,]', '', regex=True)
                                    .astype(float))
    
    #df_filtered = process_transaction_datetime(df_filtered)

    # %%
    # Find rows where Amount, Surcharge, and TotalAmount are null/NaN
    blank_rows = df_filtered[
        df_filtered['Amount'].isna() & 
        df_filtered['Surcharge'].isna() & 
        df_filtered['Tip'].isna() & 
        df_filtered['Cashout'].isna() & 
        df_filtered['Extras'].isna() & 
        df_filtered['Levy'].isna() & 
        df_filtered['ServiceFee'].isna() & 
        df_filtered['TotalAmount'].isna() & 
        df_filtered['MSF'].isna() & 
        df_filtered['TxnFee'].isna()&
        df_filtered['Rebate'].isna()
    ]

    print(f"Found {len(blank_rows)} rows with null Amount, Surcharge, and TotalAmount:")
    print(blank_rows)

    # %%
    sum_df = df_filtered['TotalAmount'].sum()
    print(f"Sum of Amount in df_filtered: {sum_df}")

    # %%
    # Only convert negative values to positive, leave positive values as-is
    df_filtered['MSF'] = df_filtered['MSF'].apply(lambda x: abs(x) if x < 0 else x)

    # %%
    print(df_filtered.MSF.head())

    # %%
    # Step 1: First check the current format of TransactionDatetime
    print("Current format of TransactionDatetime column:")
    print(df_filtered['TransactionDatetime'].head(5))
    print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")

    # Step 2: Convert to datetime format
    print("\nConverting TransactionDatetime to datetime format...")
    try:
        # Try standard conversion
        df_filtered['TransactionDatetime'] = pd.to_datetime(df_filtered['TransactionDatetime'])
    except Exception as e:
        print(f"Error in standard conversion: {str(e)}")
        
        # Try with error handling
        print("Attempting conversion with errors='coerce'...")
        df_filtered['TransactionDatetime'] = pd.to_datetime(df_filtered['TransactionDatetime'], errors='coerce')
        
        # Check for NaT values created during conversion
        nat_count = df_filtered['TransactionDatetime'].isna().sum()
        if nat_count > 0:
            print(f"Warning: {nat_count} values couldn't be converted to datetime and were set to NaT")
            
            # Show some examples of values that couldn't be converted
            original_values = df_filtered['TransactionDatetime'].copy()
            problem_indices = df_filtered[df_filtered['TransactionDatetime'].isna()].index
            if len(problem_indices) > 0:
                print("\nSample of problematic values:")
                for idx in problem_indices[:5]:  # Show up to 5 examples
                    print(f"Index {idx}: '{original_values[idx]}'")

    # Step 3: Check the conversion results
    print("\nAfter datetime conversion:")
    print(df_filtered['TransactionDatetime'].head(5))
    print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
    print(f"NaT values: {df_filtered['TransactionDatetime'].isna().sum()} out of {len(df_filtered)}")

    # Step 4: Now you can use .dt accessor to convert to date
    if pd.api.types.is_datetime64_dtype(df_filtered['TransactionDatetime']):
        print("\nConverting datetime to date...")
        df_filtered['TransactionDatetime'] = df_filtered['TransactionDatetime'].dt.date
        
        # Step 5: Check the final date format
        print("\nFinal date format:")
        print(df_filtered['TransactionDatetime'].head(5))
        print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
    else:
        print("\nWARNING: TransactionDatetime is still not a datetime type after conversion attempts.")
        print("Current data type:", df_filtered['TransactionDatetime'].dtype)
        print("Sample values:", df_filtered['TransactionDatetime'].head(5).tolist())

    # %%
    import supabase
    from supabase import create_client, Client

    # %%
    url = SUPABASE_URL
    key =  SUPABASE_KEY
    
    #url = 'https://thxvfnachnpgmeottlem.supabase.co'
    #key =  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRoeHZmbmFjaG5wZ21lb3R0bGVtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Njg2ODQ1OSwiZXhwIjoyMDYyNDQ0NDU5fQ.TBgZdtH3INLZtpnraa4dfPbZ0hZHLdCoY1VKhqEv8FA'
    
    # %%
    supabase = create_client(url, key,)

    # %%
    import time
    import numpy as np
    from tqdm import tqdm
    import json
    from datetime import date, datetime


    # %%
    # Replace NaN values with None
    df_filtered = df_filtered.replace({np.nan: None})

    # %%

    # Custom JSON encoder to handle NaN values
    class NanHandlingEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, float) and np.isnan(obj):
                return None
            if isinstance(obj, (datetime, date, pd.Timestamp)):
                return obj.isoformat()
            return super().default(obj)

 

    def upload_dataframe_in_chunks(df, table_name, chunk_size=5000):
        total_rows = len(df)
        chunks = range(0, total_rows, chunk_size)
        successful_rows = 0
        failed_chunks = []

        with tqdm(total=total_rows, desc=f"Uploading to {table_name}") as pbar:
            for i in chunks:
                end_idx = min(i + chunk_size, total_rows)
                chunk = df.iloc[i:end_idx]
                chunk_records = json.loads(json.dumps(chunk.to_dict('records'), cls=NanHandlingEncoder))

                try:
                    # Use upsert instead of insert to handle duplicates gracefully
                    response = supabase.table(table_name).upsert(chunk_records, on_conflict="id").execute()

                    if hasattr(response, 'data') and response.data:
                        successful_rows += len(response.data)
                        print(f"✅ Successfully uploaded chunk {i} to {end_idx - 1}")
                    elif hasattr(response, 'error') and response.error:
                        raise Exception(response.error)
                    else:
                        print(f"⚠️ No error but no data returned — assuming success")
                        successful_rows += len(chunk_records)
                except Exception as e:
                    print(f"❌ Error uploading chunk {i} to {end_idx - 1}: {e}")
                    failed_chunks.append((i, end_idx))

                pbar.update(len(chunk))

        return {
            "total_rows": total_rows,
            "successful_rows": successful_rows,
            "failed_chunks": failed_chunks
        }


    # --- MAIN EXECUTION STARTS HERE ---

    # Example: table_name = "Dev_Transaction"
    table_name = ENV_TABLE_NAME
    result = upload_dataframe_in_chunks(df_filtered, table_name, chunk_size=5000)

    # Retry failed chunks with smaller chunk size
    permanently_failed_data = []

    if result['failed_chunks']:
        print("🔁 Retrying failed chunks with smaller chunk size (1000)...")

        for start_idx, end_idx in result['failed_chunks']:
            failed_chunk = df_filtered.iloc[start_idx:end_idx]

            # Retry with smaller chunk size
            retry_result = upload_dataframe_in_chunks(failed_chunk, table_name, chunk_size=1000)

            print(f"🔁 Retry result: {retry_result['successful_rows']} of {retry_result['total_rows']} rows uploaded")

            # If retry still has failed chunks, collect those rows
            if retry_result['failed_chunks']:
                for retry_start, retry_end in retry_result['failed_chunks']:
                    permanently_failed_chunk = failed_chunk.iloc[retry_start:retry_end]
                    permanently_failed_data.append(permanently_failed_chunk)

    # Save permanently failed data to CSV
    if permanently_failed_data:
        permanently_failed_df = pd.concat(permanently_failed_data, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_filename = f"failed_upload_data_{table_name}_{timestamp}.csv"
        permanently_failed_df.to_csv(failed_filename, index=False)
        print(f"⚠️ {len(permanently_failed_df)} rows permanently failed and saved to: {failed_filename}")
    else:
        print("✅ All rows uploaded successfully after retries.")


#####################        EMAIL SEND    ###########################
 
def gmail_authenticate():
    refresh_token = REFRESH_TOKEN
    if not refresh_token:
        raise Exception("Missing REFRESH_TOKEN in environment variables")

    creds = Credentials(
        token=None,  # no access token yet
        refresh_token=refresh_token,
        token_uri=TOKEN_URI,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES
    )

    # Use the refresh token to get a fresh access token
    creds.refresh(Request())

    # Build and return the Gmail API client
    service = build('gmail', 'v1', credentials=creds)
    return service

    #token_data = load_json_from_supabase(BUCKET_NAME, 'Credentials/SendMailToken/data_SendMailToken_token.json', SUPABASE_URL, SUPABASE_KEY)
    #if not token_data:
        #print("Failed to load Gmail token from Supabase.")
        #return
    #with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        #json.dump(token_data, tmp)
        #tmp.flush()
    ##creds_info= load_json_from_supabase(BUCKET_NAME, 'Credentials/SendMailToken/token.json', SUPABASE_URL, SUPABASE_KEY)
    #creds = Credentials.from_authorized_user_info(info=token_data, scopes=SCOPES)
    #if not creds.valid and creds.expired and creds.refresh_token:
        #creds.refresh(Request())
    #return build('gmail', 'v1', credentials=creds)

def create_message(sender, to, subject, message_text):
    message = EmailMessage()
    message.set_content(message_text)
    message['To'] = to
    message['From'] = sender
    message['Subject'] = subject

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': encoded_message}

 
def send_test_email(request,recipient,body):  
    global EMAIL_STATUS
    try:
            service = gmail_authenticate()
            message = create_message(sender='aps@aps.business',to=recipient,subject=request,message_text=body)   
            send_message = service.users().messages().send(userId='me', body=message).execute()
            print(f"Message sent! ID: {send_message['id']}")
            EMAIL_STATUS = True
            return True
    except Exception as e:
            print(f"❌ Failed to send email: {str(e)}")
            EMAIL_STATUS = False
            return False

def convert_datetime_robust_main(dt_str):
    # Handle NaN or missing values
    if pd.isna(dt_str) or str(dt_str).lower() == 'nan':
        return pd.NaT

    dt_str = str(dt_str).strip()

    # Try common date formats
    formats_to_try = [
        '%d/%m/%Y %H:%M',  # 30/04/2025 19:36
        '%d/%m/%y %H:%M',  # 12/4/25 20:46
        '%d/%m/%Y %H:%M',  # 30/04/2025 9:57
        '%d/%m/%y %H:%M',  # 12/4/25 8:54
        '%d/%m/%y %H:%M',  # 9/4/25 9:56
    ]

    for fmt in formats_to_try:
        try:
            dt = pd.to_datetime(dt_str, format=fmt)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            continue

    # If all formats fail, try auto-detection with dayfirst
    try:
        return pd.to_datetime(dt_str, dayfirst=True)
    except Exception:
        return pd.NaT

# Assuming df_filtered is your DataFrame with 'TransactionDatetime' column
def process_transaction_datetime(df_filtered):
    print("Updating TransactionDatetime in df_filtered...")
    print(f"Original data type: {df_filtered['TransactionDatetime'].dtype}")
    print("Sample original values:")
    print(df_filtered['TransactionDatetime'].head(5))

    # Step 1: Convert to datetime
    df_filtered['TransactionDatetime'] = df_filtered['TransactionDatetime'].apply(convert_datetime_robust_main)

    print(f"\nAfter datetime conversion:")
    print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
    print("Sample datetime values:")
    print(df_filtered['TransactionDatetime'].head(5))

    # Step 2: Convert to date only (remove timestamp)
    df_filtered['TransactionDatetime'] = df_filtered['TransactionDatetime'].dt.date

    print(f"\nAfter converting to date (timestamp removed):")
    print(f"Data type: {df_filtered['TransactionDatetime'].dtype}")
    print("Sample date values:")
    print(df_filtered['TransactionDatetime'].head(10))

    # Check conversion results
    conversion_success = df_filtered['TransactionDatetime'].notna().sum()
    conversion_failed = df_filtered['TransactionDatetime'].isna().sum()

    print(f"\nConversion summary:")
    print(f"Successfully converted: {conversion_success}")
    print(f"Failed conversions: {conversion_failed}")
    print(f"Total rows: {len(df_filtered)}")

    # Show date range
    if conversion_success > 0:
        print(f"\nDate range:")
        print(f"From: {df_filtered['TransactionDatetime'].min()}")
        print(f"To: {df_filtered['TransactionDatetime'].max()}")

    # Show final result
    print(f"\nFinal TransactionDatetime column format:")
    print(df_filtered['TransactionDatetime'].head(10))

    return df_filtered

 
if __name__ == "__main__": 

    config_data = load_xml_config_from_supabase(FILE_PATH)
    if config_data:
        bucket_name   = config_data["bucketName"]
        table_name    = config_data["tableName"]
        sender        = config_data["sender"]
        recipient     = config_data["to"]
        subjectdata   = config_data["subject"]
        message_text  = config_data["message_text"]
        download_and_upload_attachments(bucket_name,table_name,sender,recipient,subjectdata,message_text)

 
