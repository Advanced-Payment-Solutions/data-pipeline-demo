import base64
import csv
import io
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

from supabase import create_client, Client
 

SUPABASE_URL = "https://thxvfnachnpgmeottlem.supabase.co"
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRoeHZmbmFjaG5wZ21lb3R0bGVtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Njg2ODQ1OSwiZXhwIjoyMDYyNDQ0NDU5fQ.TBgZdtH3INLZtpnraa4dfPbZ0hZHLdCoY1VKhqEv8FA'
BUCKET_NAME = "apsbucket"



tables = {}  
uploaded_files = []
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
ENV_TABLE_NAME='Dev_LivePayment_Transactions'

 
def authenticate_gmail(GMAIL_TOKEN_PATH):
    creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_PATH, ['https://www.googleapis.com/auth/gmail.readonly'])
    service = build('gmail', 'v1', credentials=creds) 
    return service

######################################################################

def download_and_upload_attachments(XML_PATH):
    token_data = load_json_from_supabase(BUCKET_NAME, 'Credentials/token.json', SUPABASE_URL, SUPABASE_KEY)
    if not token_data:
        print("Failed to load Gmail token from Supabase.")
        return
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        json.dump(token_data, tmp)
        tmp.flush()
        service = authenticate_gmail(tmp.name) 
        #today = datetime.today().strftime('%Y/%m/%d')    
        yesterday = (datetime.today() - timedelta(days=2)).strftime('%Y/%m/%d')    
        fileprocessdate=yesterday
        query = f'after:{fileprocessdate} filename:csv has:attachment subject:"Dealer Transactions Report"'
        results = service.users().messages().list(userId='me', q=query).execute()
        messages = results.get('messages', [])
        removeexistingfiles(BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)        
        for message in messages:
                                msg = service.users().messages().get(userId='me', id=message['id']).execute()
                                for part in msg['payload'].get('parts', []):
                                 filename = part.get("filename") 
                                 body = part.get("body", {})                                 
                                 exists = check_row_exists(supabase_url=SUPABASE_URL,supabase_key=SUPABASE_KEY,table_name="TransactionLog", date_column="filedate",date_value=fileprocessdate,filename_column="filename",filename_value=filename)
                                 if exists:
                                    print("Row already exists duplicate excution for the same date")
                                 else: 
                                    print("Row does not exist for the date of " + fileprocessdate)
                                    if filename and 'attachmentId' in body:
                                        insert_file_record(SUPABASE_URL,SUPABASE_KEY,filename,fileprocessdate,'TransactionLog')
                                        att_id = body['attachmentId']
                                        attachment = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=att_id).execute()
                                        data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8')) 
                                        upload_file_to_supabase(data,'Data/'+filename,BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)                                     
                                        df = load_csv_from_supabase("apsbucket", 'Data/'+filename, SUPABASE_URL, SUPABASE_KEY)
                                        num_rows = len(df)
                                        print(num_rows)
                                        if df is not None: 
                                            send_test_email('GCP Service execution started for the '+ filename + ' with rows of ' + str(num_rows))
                                            push_data_supabase_database(df,SUPABASE_URL,SUPABASE_KEY,ENV_TABLE_NAME)
                                            removeexistingfiles(BUCKET_NAME,SUPABASE_URL,SUPABASE_KEY)
                                            send_test_email('GCP Service execution completed for the '+ filename + ' with rows of ' +str(num_rows))
                                        else:
                                            print("Failed to load CSV from Supabase.")

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

                                        
def insert_file_record(supabase_url: str, supabase_key: str, filename: str, date_str: str,table_name:str):
  
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
        data = {
            "filename": filename,
            "filedate": date_str
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

    def upload_dataframe_in_chunks(df, table_name, chunk_size=20000):
        total_rows = len(df)
        chunks = range(0, total_rows, chunk_size)
        successful_rows = 0
        failed_chunks = []

        print(f"Uploading {total_rows} rows to {table_name} in chunks of {chunk_size}")

        with tqdm(total=total_rows) as pbar:
            for i in chunks:
                end_idx = min(i + chunk_size, total_rows)
                chunk = df.iloc[i:end_idx]

                try:
                    # Convert chunk to JSON records
                    chunk_records = json.loads(json.dumps(chunk.to_dict('records'), cls=NanHandlingEncoder))
                    response = supabase.table(table_name).insert(chunk_records).execute()

                    # Count successful rows
                    successful_rows += len(response.data if hasattr(response, 'data') else chunk_records)
                    print(f"Successfully uploaded chunk {i} to {end_idx-1}")

                except Exception as e:
                    print(f"Error uploading chunk {i} to {end_idx-1}: {e}")
                    failed_chunks.append((i, end_idx))
                pbar.update(len(chunk))

        return {"total_rows": total_rows, "successful_rows": successful_rows, "failed_chunks": failed_chunks}

    # Execute the upload
    #table_name = "Dev_Transaction"
    table_name=ENV_TABLE_NAME
    result = upload_dataframe_in_chunks(df_filtered, table_name, chunk_size=20000)

    # Retry failed chunks with smaller chunk size and collect permanently failed data
    permanently_failed_data = []

    if result['failed_chunks']:
        print("Retrying failed chunks with smaller chunk size...")
        for start_idx, end_idx in result['failed_chunks']:
            failed_chunk = df_filtered.iloc[start_idx:end_idx]
            retry_result = upload_dataframe_in_chunks(failed_chunk, table_name, chunk_size=10000)
            print(f"Retry result: {retry_result['successful_rows']} of {retry_result['total_rows']} rows uploaded successfully")
            
            # If there are still failed chunks after retry, collect the data
            if retry_result['failed_chunks']:
                for retry_start, retry_end in retry_result['failed_chunks']:
                    # Calculate actual indices in the original dataframe
                    actual_start = start_idx + retry_start
                    actual_end = start_idx + retry_end
                    permanently_failed_chunk = df_filtered.iloc[actual_start:actual_end]
                    permanently_failed_data.append(permanently_failed_chunk)

    # Save permanently failed data to CSV
    if permanently_failed_data:
        permanently_failed_df = pd.concat(permanently_failed_data, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_filename = f"failed_upload_data_{table_name}_{timestamp}.csv"
        permanently_failed_df.to_csv(failed_filename, index=False)
        print(f"⚠️ {len(permanently_failed_df)} rows failed permanently and saved to: {failed_filename}")
    else:
        print("✅ All chunks uploaded successfully!")
    



#####################        EMAIL SEND    ###########################
 
def gmail_authenticate():
    token_data = load_json_from_supabase(BUCKET_NAME, 'Credentials/SendMailToken/data_SendMailToken_token.json', SUPABASE_URL, SUPABASE_KEY)
    if not token_data:
        print("Failed to load Gmail token from Supabase.")
        return
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        json.dump(token_data, tmp)
        tmp.flush()
    ##creds_info= load_json_from_supabase(BUCKET_NAME, 'Credentials/SendMailToken/token.json', SUPABASE_URL, SUPABASE_KEY)
    creds = Credentials.from_authorized_user_info(info=token_data, scopes=SCOPES)
    if not creds.valid and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)

def create_message(sender, to, subject, message_text):
    message = EmailMessage()
    message.set_content(message_text)
    message['To'] = to
    message['From'] = sender
    message['Subject'] = subject

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': encoded_message}

def send_email(service, user_id, message):
    send_message = service.users().messages().send(userId=user_id, body=message).execute()
    print(f"Message sent! ID: {send_message['id']}")


def send_test_email(request):
    request_json = {
            "to": "tkmsureshkumar@gmail.com;alana@aps.business;gio@aps.business;aps@aps.business",
            "subject": "Cloud Function Test",
            "body": "This is a test email from GCP Cloud Function."
    }
    # request_json = request.get_json(silent=True)
    recipient = request_json.get('to', 'tkmsureshkumar@gmail.com;alana@aps.business;gio@aps.business;aps@aps.business')
    subject = request_json.get('subject', )
    body = request_json.get('body', 'Hello from the Gmail API via Python!')
    service = gmail_authenticate()
    message = create_message(
        sender='aps@aps.business',
        to=recipient,
        subject=subject,
        message_text=body
    )
    send_email(service, 'me', message)
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
    download_and_upload_attachments('test')

 
