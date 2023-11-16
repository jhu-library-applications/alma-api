import secret
import pandas as pd
import argparse
import aiohttp
import asyncio
import json
from asyncio_throttle import Throttler
from datetime import datetime
import csv

throttler = Throttler(rate_limit=25, period=1)

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
df['item_link'] = baseURL+'/almaws/v1/bibs/'+df['mms_id'].astype(str)+'/holdings/'+df['holding_id'].astype(str)\
                  +'/items/'+df['pid'].astype(str)
df = df.set_index('item_link')
item_links = df.index.to_list()
metadata_list = []
update_logs = []
item_logs = []


def get_errors(metadata, log):
    errors = []
    error_list = metadata['errorList']['error']
    for error in error_list:
        error_message = error['errorMessage']
        errors.append(error_message)
    errors = '|'.join(errors)
    print(errors)
    log['error'] = errors


async def update_item(session, metadata):
    updated_metadata = None
    post_error = None
    update_item_url = metadata['link']
    metadata = json.dumps(metadata)
    async with throttler:
        async with session.put(update_item_url, headers=update_headers, data=metadata) as updated_response:
            if updated_response.status != 200:
                post_error = await updated_response.json()
                print('Update error {}.'.format(update_item_url))
            else:
                updated_metadata = await updated_response.json()
                print('Updated {}'.format(update_item_url))
    data = {'link': update_item_url, 'metadata': updated_metadata, 'error': post_error}
    return data


async def get_item(session, url):
    metadata = None
    error = None
    async with throttler:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                error = await response.json()
                print('Retrieval error {}.'.format(url))
            else:
                metadata = await response.json()
                print('Retrieved {}.'.format(url))
    data = {'link': url, 'metadata': metadata, 'error': error}
    return data


async def main():
    session = aiohttp.ClientSession()
    item_requests = [get_item(session, link) for link in item_links]
    responses = await asyncio.gather(*item_requests)
    for response in responses:
        link = response['link']
        error = response['error']
        metadata = response['metadata']
        log = {'link': link, 'get_error': error}
        if metadata is not None:
            item_data = metadata['item_data']
            current_description = item_data['description']
            old_description = df.at[link, 'old_description']
            new_description = df.at[link, 'new_description']
            if old_description == current_description:
                item_data['description'] = new_description
                metadata['item_data'] = item_data
                metadata_list.append(metadata)
            else:
                log['description_error'] = 'Description already updated'
        else:
            get_errors(error, log)
        item_logs.append(log)
    item_updates = [update_item(session, metadata) for metadata in metadata_list]
    update_responses = await asyncio.gather(*item_updates)
    for updated_response in update_responses:
        link = updated_response['link']
        post_error = updated_response['error']
        updated_metadata = updated_response['metadata']
        update_log = {'link': link, 'post_error': post_error}
        if updated_metadata is not None:
            updated_item = updated_metadata['item_data']
            updated_description = updated_item['description']
            update_log['updated_description'] = updated_description
        else:
            get_errors(post_error, update_log)
        update_logs.append(update_log)

    await session.close()

asyncio.run(main())

# Convert item_log to DataFrame.
update_logs = pd.DataFrame.from_dict(update_logs)
item_logs = pd.DataFrame.from_dict(item_logs)
complete_log = pd.merge(update_logs, item_logs, how='left', on='link')

dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
filename = filename.replace('.csv', 'csv')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
complete_log.to_csv('log_'+filename+'_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
print(datetime.now() - startTime)