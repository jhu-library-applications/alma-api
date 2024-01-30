import csv
import requests
import secret
import pandas as pd
from datetime import datetime
import json
import argparse


base_url = 'https://api-na.hosted.exlibrisgroup.com'


s = requests.Session()

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

# Use secret files to request from either production or stage.
secretsVersion = input('To edit production server, enter secret filename: ')
if secretsVersion != '':
    try:
        secret = __import__(secretsVersion)
        print('Editing Production')
    except ImportError:
        print('Editing Stage')
else:
    print('Editing Stage')

# From selected secret file, grab the api_key.
api_key = secret.api_key

# Create headers to authorize with api_key and to request output in JSON.
headers = {"Authorization": "apikey "+api_key,
           "Accept": "application/json",
           "Content-Type": "application/json"}

s = requests.Session()
s.headers = headers

df = pd.read_csv(filename, dtype='string')


def get_errors(metadata):
    errors = []
    error_list = metadata['errorList']['error']
    for error in error_list:
        error_message = error['errorMessage']
        errors.append(error_message)
    errors = '|'.join(errors)
    print(errors)
    log['error_message'] = errors


def add_optional_value(field_name, value, dictionary):
    if pd.notna(value):
        dictionary[field_name] = value
    else:
        pass


item_log = []
for count, row in df.iterrows():
    holding_data = {'copy_id': 'c. 1'}
    item_data = {}

    # Get item information from DataFrame.
    # Get values for required fields.
    mms_id = row['mms_id']
    holding_id = row['holding_id']
    log = {'mms_id': mms_id, 'holding_id': holding_id}
    # Get values for optional fields.
    description = row.get('description')
    public_note = row.get('public_note')

    # Build JSON from CSV values.
    holding_data['holding_id'] = holding_id
    add_optional_value('description', description, item_data)
    add_optional_value('public_note', public_note, item_data)
    new_item = {'holding_data': holding_data, 'item_data': item_data}
    new_item_json = json.dumps(new_item)

    # Post new item to Alma.
    endpoint = f'/almaws/v1/bibs/{mms_id}/holdings/{holding_id}/items'
    r = s.post(base_url+endpoint, data=new_item_json, timeout=30)
    status = r.status_code
    print(count, status)
    if status == 200:
        data = r.json()
        updated_item_data = data['item_data']
        updated_holding_data = data['holding_data']

        # Record info about new item in log.
        log['pid'] = updated_item_data['pid']
        log['barcode'] = updated_item_data['barcode']
        log['copy_id'] = updated_holding_data['copy_id']
        log['description'] = updated_item_data.get('description')
        log['public_note'] = updated_item_data.get('public_note')
    else:
        log['error_status'] = status
        data = r.json()
        print(data)
        get_errors(data)
    item_log.append(log)

# Convert item_log to DataFrame.
log_df = pd.DataFrame.from_records(item_log)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('postNewItemsLog_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
