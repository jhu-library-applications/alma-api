import csv
import requests
import secret
import pandas as pd
from datetime import datetime
import json
import argparse


baseURL = 'https://api-na.hosted.exlibrisgroup.com'

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
           "Accept": "application/json"}

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
    log['error'] = errors


item_log = []
for count, row in df.iterrows():
    log = row.copy()
    # Get item information from DataFrame.
    item_barcode = row.get('current_barcode')
    print(count, item_barcode)

    # Using information from DataFrame, get current item JSON from API
    barcode_endpoint = '/almaws/v1/items?item_barcode={}'.format(item_barcode)
    get_item_url = baseURL + barcode_endpoint
    # Make request for item.
    try:
        item_metadata = s.get(get_item_url, timeout=6).json()
    except TimeoutError:
        log['error'] = 'Timeout Error'
        item_log.append(log)
        continue
    except requests.exceptions.ReadTimeout:
        log['error'] = 'Timeout Error'
        item_log.append(log)
        continue
    try:
        # Try getting 'link' field from item_metadata.
        full_link = item_metadata['link']
    except KeyError:
        # If script cannot find field full_link, assume an error has occurred.
        # Record error in log using get_errors function.
        get_errors(item_metadata)
        item_log.append(log)
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
    # Update fields from spreadsheet.
    old_value = row.get('old_description')
    current_value = item_data['description']
    log['current_description'] = current_value
    print(old_value, current_value)
    if old_value == current_value:
        new_description = row.get('new_description')
        item_data['description'] = new_description
        item_metadata['item_data'] = item_data
        # Convert item_metadata into a json string.
        item_metadata = json.dumps(item_metadata)
        # Update item JSON using full_link (same as pid endpoint).
        s.headers.update({"Content-Type": "application/json"})
        try:
            put_response = s.put(full_link, data=item_metadata, timeout=6).json()
        except requests.exceptions.Timeout:
            log['error'] = 'Timeout Error'
            item_log.append(log)
            continue
        except json.JSONDecodeError:
            json_error = 'Error posting updated item.'
            print(json_error)
            log['error'] = json_error
            item_log.append(log)
            continue
        # With put_response as the updated item, check that description in item is correct now.
        print('item updated')
        updated_item_data = put_response['item_data']
        updated_value = updated_item_data.get('description')
        # Record updated description in log.
        log['updated_description'] = updated_value
    else:
        no_update_needed = 'Item field(s) were already updated.'
        log['error'] = no_update_needed
    item_log.append(log)

# Convert item_log to DataFrame.
log_df = pd.DataFrame.from_dict(item_log)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('updatedItemsFieldsLog_10_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
