import requests
import secret
import json
import pandas as pd
import time
from datetime import datetime

start_time = time.time()

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

filename = 'LSC AFA Update Spreadsheet 6-14-24.csv'
df = pd.read_csv(filename, dtype='string')


# Function to extract errors from JSON response.
def get_errors(metadata):
    if isinstance(metadata, dict):
        all_errors = []
        error_list = metadata['errorList']['error']
        for error in error_list:
            error_message = error['errorMessage']
            all_errors.append(error_message)
        all_errors = '|'.join(all_errors)
        error = all_errors
    else:
        error = metadata
    return error


all_items = []
for count, row in df.iterrows():
    row = row
    current_barcode = row['current_barcode']
    new_barcode = row['new_barcode']
    rmst_to_add = row['rmst_number']
    print(count, current_barcode)
    barcode_endpoint = '/almaws/v1/items?item_barcode={}'.format(current_barcode)
    get_item_url = baseURL + barcode_endpoint
    # Make request for item.
    try:
        item_metadata = requests.get(get_item_url, headers=headers, timeout=10).json()
    except requests.exceptions.RequestException as e:
        row['error'] = e
        print(e)
        all_items.append(row)
        continue
    try:
        # Try getting 'link' field from item_metadata.
        full_link = item_metadata['link']
        row['full_link'] = full_link
    except KeyError:
        errors = get_errors(item_metadata)
        row['error'] = errors
        print(errors)
        all_items.append(row)
        continue
    item_data = item_metadata['item_data']
    rmst = item_data['storage_location_id']
    item_barcode = item_data['barcode']
    if new_barcode != item_barcode:
        if rmst_to_add != rmst:
            item_data['storage_location_id'] = rmst_to_add
            item_data['barcode'] = new_barcode
            item_metadata.pop('bib_data')
            # Convert item_metadata into a json string.
            item_metadata = json.dumps(item_metadata)
            update_link = full_link
            print(update_link)
            try:
                updated_metadata = requests.put(update_link, headers=headers, data=item_metadata,
                                                timeout=10).json()
            except requests.exceptions.RequestException as e:
                row['error'] = e
                all_items.append(row)
                continue
            try:
                updated_item = updated_metadata['item_data']
                updated_description = updated_item['description']
                updated_rmst = updated_item['storage_location_id']
                row['updated_description'] = updated_description
                row['updated_rmst'] = updated_rmst
            except KeyError:
                errors = get_errors(item_metadata)
                row['error'] = errors
    else:
        row['error'] = 'Item already updated'
    all_items.append(row)

updated_df = pd.DataFrame.from_dict(all_items)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
updated_df.to_csv('updated_lsc_afa_'+dt+'.csv', index=False)
print("--- %s seconds ---" % (time.time() - start_time))
