import logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient import errors

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('script_log.log'), # Log to this file
                        logging.StreamHandler()               # Log to console
                    ])

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = ''

credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('sheets', 'v4', credentials=credentials)

# Your Google Sheets IDs and ranges
sheet_id_old = ''
sheet_id_new = ''
range_old = 'Sheet1!A:D'
range_new = 'Sheet1!E:E'

logging.info("Reading data from the old sheet...")
# Read data from the first sheet
sheet = service.spreadsheets()
result_old = sheet.values().get(spreadsheetId=sheet_id_old, range=range_old).execute()
values_old = result_old.get('values', [])

logging.info("Filtering values based on checkbox status...")
# Filter values based on checkbox status
filtered_values_old = [row[3] for row in values_old if len(row) > 3 and row[0] == 'TRUE']

logging.info("Reading data from the new sheet...")
# Read data from the second sheet
result_new = sheet.values().get(spreadsheetId=sheet_id_new, range=range_new).execute()
values_new = result_new.get('values', [])

def extract_last_part_of_url(url):
    return url.split('/')[-1]

def get_last_processed_row():
    try:
        with open('last_processed_row.txt', 'r') as file:
            last_row = file.read().strip()
            return int(last_row) if last_row else 1
    except FileNotFoundError:
        return 1  # Return 1 if file does not exist

def save_last_processed_row(row_number):
    with open('last_processed_row.txt', 'w') as file:
        file.write(str(row_number))

last_row_to_start = get_last_processed_row()

# Prepare batch update data
batch_update_data = []
for i in range(last_row_to_start - 1, len(values_new)):
    row = values_new[i]
    if not row:  # Skip empty rows
        continue
    url = row[0]
    extracted_value = extract_last_part_of_url(url)

    extracted_value_str = str(extracted_value).strip()
    if extracted_value_str in filtered_values_old:
        update_range = f'Sheet1!B{i+1}'
        batch_update_data.append({
            "range": update_range,
            "values": [["TRUE"]]
        })

# Perform batch update if there are changes
if batch_update_data:
    try:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": batch_update_data
        }
        sheet.values().batchUpdate(spreadsheetId=sheet_id_new, body=body).execute()
        logging.info("Batch update completed.")
    except Exception as e:
        logging.error(f"Error during batch update: {e}")
        if isinstance(e, errors.HttpError) and e.resp.status == 429:
            # Save the last processed row and exit
            save_last_processed_row(i + 1)
            logging.info("Quota limit reached, saving state and exiting.")
else:
    logging.info("No updates required.")

logging.info("Script completed.")
