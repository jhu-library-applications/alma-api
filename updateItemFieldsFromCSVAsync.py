import secret
import pandas as pd
import argparse
import aiohttp
import asyncio
import json
from datetime import datetime
import csv

startTime = datetime.now()

baseURL = 'https://api-na.hosted.exlibrisgroup.com'
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

update_headers = {"Authorization": "apikey "+api_key,
                  "Accept": "application/json",
                  "Content-Type": "application/json"}


df = pd.read_csv(filename, dtype={'pid': int, 'mms_id': int, 'barcode': int, 'holding_id': int})
total_rows = len(df)
item_log = []
update_log = []
metadata_list = []


def get_errors(metadata, log):
    errors = []
    error_list = metadata['errorList']['error']
    for error in error_list:
        error_message = error['errorMessage']
        errors.append(error_message)
    errors = '|'.join(errors)
    print(errors)
    log['error'] = errors


async def update_item(session, update_item_url, metadata):
    updated_metadata = None
    post_error = None
    async with session.put(update_item_url, headers=update_headers, data=metadata) as updated_response:
        if updated_response.status != 200:
            post_error = await updated_response.json()
        else:
            updated_metadata = await updated_response.json()
        return updated_metadata, post_error


async def get_item(session, url):
    metadata = None
    get_error = None
    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            get_error = await response.json()
        else:
            metadata = await response.json()
    return metadata, get_error


async def main():
    session = aiohttp.ClientSession()
    for count, row in df.iterrows():
        actual_count = count + 1
        remaining = total_rows - actual_count
        metadata_count = len(metadata_list)
        if (metadata_count <= 1000) and (remaining > 0):
            log = row
            # Get item information from DataFrame.
            item_barcode = str(row.get('barcode'))
            mms_id = str(row.get('mms_id'))
            holding_id = str(row.get('holding_id'))
            pid = str(row.get('pid'))
            print('Getting item {}. Count: {} of {}'.format(item_barcode, actual_count, total_rows))
            # Using information from DataFrame, get current item JSON from API
            item_endpoint = '/almaws/v1/bibs/{}/holdings/{}/items/{}'.format(mms_id, holding_id, pid)
            get_item_url = baseURL + item_endpoint
            # Make request for item.
            metadata, get_error = await get_item(session, get_item_url)
            if metadata is not None:
                item_data = metadata['item_data']
                current_description = item_data['description']
                old_description = row.get('old_description')
                new_description = row.get('new_description')
                if old_description == current_description:
                    item_data['description'] = new_description
                    metadata['item_data'] = item_data
                    metadata_list.append(metadata)
                else:
                    log['error'] = 'Description already updated'
            else:
                get_errors(get_error, log)
            item_log.append(log)

        else:
            for number, item_metadata in enumerate(metadata_list):
                log = {}
                full_link = item_metadata['link']
                barcode = item_metadata['item_data']['barcode']
                log['full_link'] = full_link
                log['barcode'] = int(barcode)
                print('Posting item {}. Count: {}'.format(barcode, number))
                # Convert item_metadata into a json string.
                metadata = json.dumps(item_metadata)
                # Update item JSON using full_link (same as pid endpoint).
                update_item_url = full_link
                updated_metadata, post_error = await update_item(session, update_item_url, metadata)
                if updated_metadata is not None:
                    updated_item = updated_metadata['item_data']
                    updated_description = updated_item['description']
                    log['updated_description'] = updated_description
                else:
                    get_errors(post_error, log)
                    update_log.append(log)
                update_log.append(log)
            metadata_list.clear()
    await session.close()

asyncio.run(main())

# Convert item_log to DataFrame.
update_log = pd.DataFrame.from_dict(update_log)
log_df = pd.DataFrame.from_dict(item_log)
complete_log = pd.merge(update_log, log_df, how='left', on='barcode')

dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('updatedItemsFieldsLog_10_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
print(datetime.now() - startTime)