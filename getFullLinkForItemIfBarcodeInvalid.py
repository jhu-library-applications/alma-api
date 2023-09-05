import requests
import secret
import pandas as pd
from datetime import datetime
import math
import csv


baseURL = 'https://api-na.hosted.exlibrisgroup.com'
endpoint = '/almaws/v1/bibs/{mms_id}/holdings/{holding_id}/items/{item_pid}'

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

filename = 'bad_copy_1_msel.csv'
df = pd.read_csv(filename, dtype='string')


def get_info(dataframe, start_row, stop_row):
    all_items = []
    for count, row in dataframe.iterrows():
        if (count <= stop_row) and (count >= start_row):
            row = row
            mms_id = row.get('mms_id')
            item_barcode = row.get('item_barcode')
            print(count, mms_id, item_barcode)
            # Create URL to retrieve item using the barcode.
            barcode_endpoint = '/almaws/v1/bibs/{}/holdings/{}/items'.format(mms_id, 'ALL')
            get_item_url = baseURL + barcode_endpoint
            # Make request for item.
            items = requests.get(get_item_url, headers=headers).json()
            try:
                items = items['item']
                for item in items:
                    barcode = item['item_data']['barcode']
                    if barcode == item_barcode:
                        print('success')
                        holding_id = item['holding_data']['holding_id']
                        pid = item['item_data']['pid']
                        row['pid'] = pid
                        row['holding_id'] = holding_id
                        all_items.append(row)
                    else:
                        pass
            except KeyError:
                print(items)
                errors = []
                try:
                    error_list = items['errorList']['error']
                    for error in error_list:
                        error_message = error['errorMessage']
                        errors.append(error_message)
                    errors = '|'.join(errors)
                    print(errors, mms_id)
                    row['error'] = errors
                    continue
                except KeyError:
                    row['error'] = items
                    continue

    updated_df = pd.DataFrame.from_dict(all_items)
    dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
    str_loop = str(loop).zfill(3)
    updated_df.to_csv('updatedItemsFieldsLog_' + str_loop + '_' + dt + '.csv', index=False, quoting=csv.QUOTE_ALL)


rows_left = len(df.index)
rows_left = rows_left
total_rows = len(df.index)
batch_size = 1000
current_row = 15504
loop = 0
stop = current_row+batch_size
while rows_left > 0:
    loop = loop + 1
    start = current_row
    stop = current_row+batch_size
    print(start, stop)
    if stop > total_rows:
        print('True')
        print('loop {}: rows {}-{}'.format(loop, start, total_rows))
        get_info(df, start, total_rows)
        current_row = stop + 1
        rows_left = 0
    else:
        print('loop {}: rows {}-{}'.format(loop, start, stop))
        get_info(df, start, stop)
        current_row = stop + 1
        rows_left = rows_left-batch_size