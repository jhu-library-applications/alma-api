import csv
import requests
import secret
import pandas as pd
from datetime import datetime
import json


baseURL = 'https://api-na.hosted.exlibrisgroup.com'

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

filename = 'copy_1_msel.csv'
df = pd.read_csv(filename, dtype='string')


def get_errors(metadata):
    errors = []
    error_list = metadata['errorList']['error']
    for error in error_list:
        error_message = error['errorMessage']
        errors.append(error_message)
    errors = '|'.join(errors)
    print(errors)
    log['error'] = errors


start_row = 0
stop_row = 10000

item_log = []
for count, row in df.iterrows():
    if (count >= start_row) and (count <= stop_row):
        log = row
        # Get item information from DataFrame.
        mms_id = row.get('mms_id')
        item_barcode = row.get('item_barcode')
        holding_id = row.get('holding_id')
        pid = row.get('pid')
        old_description = row.get('description')
        old_description = old_description.strip()
        print(count, mms_id, item_barcode, pid)

        # Using information from DataFrame, get current item JSON from API
        # If pid found in spreadsheet, get item JSON from API using pid endpoint.
        if pd.notna(pid):
            pid_endpoint = '/almaws/v1/bibs/{}/holdings/{}/items/{}'.format(mms_id, holding_id, pid)
            get_item_url = baseURL + pid_endpoint
            # Make request for item.
            item_metadata = requests.get(get_item_url, headers=headers).json()
            try:
                # Try getting 'link' field from item_metadata.
                full_link = item_metadata['link']
            except KeyError:
                # If script cannot find field full_link, assume an error has occurred.
                # Record error in log using get_errors function.
                get_errors(item_metadata)
                continue

        # If no pid found in spreadsheet, get item JSON from API using barcode endpoint.
        else:
            barcode_endpoint = '/almaws/v1/items?item_barcode={}'.format(item_barcode)
            get_item_url = baseURL + barcode_endpoint
            # Make request for item.
            item_metadata = requests.get(get_item_url, headers=headers).json()
            try:
                # Try getting 'link' field from item_metadata.
                full_link = item_metadata['link']
            except KeyError:
                # If script cannot find field full_link, assume an error has occurred.
                # Record error in log using get_errors function.
                get_errors(item_metadata)
                continue
            # From item metadata, record holding_id and pid in log.
            holding_data = item_metadata['holding_data']
            holding_id = holding_data['holding_id']
            log['holding_id'] = holding_id
            item_data = item_metadata['item_data']
            pid = item_data['pid']
            log['pid'] = pid

        # Update item_metadata. Repost to endpoint using requests.put.
        # Remove bib_data from JSON.
        item_metadata.pop('bib_data')
        description = item_data['description']
        log['json_description'] = description
        description = description.strip()
        # Check that the description from spreadsheet matches description from item JSON.
        if old_description == description:
            item_data['description'] = ""
            item_metadata['item_data'] = item_data
            # Convert item_metadata into a json string.
            item_metadata = json.dumps(item_metadata)
            try:
                # Update item JSON using full_link (same as pid endpoint).
                put_response = requests.put(full_link, headers=headers, data=item_metadata).json()
                # With put_response as the updated item, check that description in item is correct now.
                updated_item_data = put_response['item_data']
                updated_description = updated_item_data.get('description')
                # Record updated description in log.
                log['updated_description'] = updated_description
            except json.JSONDecodeError:
                json_error = 'Error posting updated item.'
                print(json_error)
                log['error'] = json_error
                pass
        # If the descriptions do not match, record error in log. Do not update item.
        else:
            mismatch_error = 'Description from CSV did not match description from item JSON.'
            print(mismatch_error)
            log['error'] = mismatch_error
            pass
        item_log.append(log)

# Convert item_log to DataFrame.
log_df = pd.DataFrame.from_dict(item_log)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('updatedItemsFieldsLog_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)