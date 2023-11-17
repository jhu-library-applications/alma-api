import secret
import pandas as pd
import argparse
import aiohttp
import asyncio
import json
from asyncio_throttle import Throttler
from datetime import datetime
import csv
import time

scriptStart = datetime.now()

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
headers = {"Authorization": "apikey " + api_key,
           "Accept": "application/json"}

update_headers = {"Authorization": "apikey " + api_key,
                  "Accept": "application/json",
                  "Content-Type": "application/json"}

# Set rate for requests at 15 requests per second.
throttler = Throttler(rate_limit=15, period=1)

# Read CSV into DataFrame and create full link for each item in new column 'item_link.'
df = pd.read_csv(filename, dtype={'pid': int, 'mms_id': int, 'holding_id': int})
total_rows = len(df)
df['item_link'] = baseURL + '/almaws/v1/bibs/' + df['mms_id'].astype(str) + '/holdings/' + \
                  df['holding_id'].astype(str) + '/items/' + df['pid'].astype(str)

# Set 'item_link' as index for DataFrame.
df = df.set_index('item_link')
# Create list of all item links.
all_item_links = df.index.to_list()
total_links = str(len(all_item_links))

# Split list into batches of 1000 item links.
d = {}
for i, x in enumerate(all_item_links):
    d.setdefault(i // 1000, []).append(x)
item_link_batches = list(d.values())
total_batches = len(item_link_batches)
print('Total items {}, split into {} batches.'.format(total_links, total_batches))


# Function to extract errors from JSON response.
def get_errors(metadata):
    if isinstance(metadata, dict):
        errors = []
        error_list = metadata['errorList']['error']
        for error in error_list:
            error_message = error['errorMessage']
            errors.append(error_message)
        errors = '|'.join(errors)
        error = errors
    else:
        error = metadata
    return error


async def update_item(session, metadata):
    updated_metadata = None
    post_error = None
    update_item_url = metadata['link']
    metadata = json.dumps(metadata)
    try:
        async with throttler:
            async with session.put(update_item_url, headers=update_headers, data=metadata,
                                   timeout=60) as updated_response:
                if updated_response.status != 200:
                    post_error = await updated_response.json()
                    print('Update error {}.'.format(update_item_url))
                else:
                    updated_metadata = await updated_response.json()
                    # print('Updated {}'.format(update_item_url))
    except aiohttp.ClientError as error:
        post_error = 'ClientError'
        print(repr(error))
    except asyncio.TimeoutError as error:
        post_error = 'TimeoutError'
        print(repr(error))
    data = {'link': update_item_url, 'metadata': updated_metadata, 'error': post_error}
    return data


async def get_item(session, url):
    metadata = None
    error = None
    try:
        async with throttler:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    error = await response.json()
                    print('Retrieval error {}.'.format(url))
                else:
                    metadata = await response.json()
                    # print('Retrieved {}.'.format(url))
    except aiohttp.ClientError:
        error = 'ClientError'
        print(repr(error))
    except asyncio.TimeoutError:
        error = 'TimeoutError'
        print(repr(error))
    data = {'link': url, 'metadata': metadata, 'error': error}
    return data


async def main():
    session = aiohttp.ClientSession()
    # Loop through links in item_links and retrieve JSON via API.
    item_requests = [get_item(session, link) for link in item_links]
    # Gather responses from items.
    responses = await asyncio.gather(*item_requests)
    # Loop through responses.
    for response in responses:
        link = response['link']
        error = response['error']
        metadata = response['metadata']
        # Create item log for record-keeping..
        log = {'link': link}
        # If API returned metadata, update description in JSON based on DataFrame information.
        if metadata is not None:
            item_data = metadata['item_data']
            current_description = item_data['description']
            old_description = df.at[link, 'old_description']
            new_description = df.at[link, 'new_description']
            # Check that description was not already updated.
            if old_description == current_description:
                item_data['description'] = new_description
                metadata['item_data'] = item_data
                # Add metadata chunk to metadata list.
                metadata_list.append(metadata)
            else:
                description_error = 'Description already updated'
                print(description_error)
                update_log = {'description_error': description_error, 'link': link}
                update_logs.append(update_log)
        else:
            formatted_error = get_errors(error)
            log['get_error'] = formatted_error
        # Add log to list 'item_logs'.
        item_logs.append(log)
    # Loop through metadata in metadata_list and post update API.
    item_updates = [update_item(session, metadata) for metadata in metadata_list]
    # Gather update responses.
    update_responses = await asyncio.gather(*item_updates)
    # For each response, create log for record-keeping.
    for updated_response in update_responses:
        link = updated_response['link']
        post_error = updated_response['error']
        updated_metadata = updated_response['metadata']
        update_log = {'link': link}
        if updated_metadata is not None:
            updated_item = updated_metadata['item_data']
            updated_description = updated_item['description']
            update_log['updated_description'] = updated_description
        else:
            formatted_error = get_errors(post_error)
            update_log['post_error'] = formatted_error
        update_logs.append(update_log)

    await session.close()


for batch_count, item_links in enumerate(item_link_batches):
    total_items_in_batch = str(len(item_links))
    startTime = datetime.now()
    batch_count = batch_count + 1
    string_batch_count = str(batch_count)
    if batch_count >= 1:
        time.sleep(2)
        # Create empty logs.
        update_logs = []
        item_logs = []
        # Create list to hold JSON metadata.
        metadata_list = []

        # Run main function.
        print('')
        print('Batch {} of {}, containing {} items.'.format(string_batch_count, total_batches, total_items_in_batch))
        print('')
        asyncio.run(main())

        # Convert logs to DataFrames. Merge into one DataFrame.
        update_logs = pd.DataFrame.from_dict(update_logs)
        item_logs = pd.DataFrame.from_dict(item_logs)
        print(update_logs.head())
        print(item_logs.head())
        complete_log = pd.merge(update_logs, item_logs, how='left', on='link')

        dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
        new_filename = 'log_batch_' + string_batch_count.zfill(2) + '_' + dt + '.csv'
        # Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
        complete_log.to_csv(new_filename, index=False, quoting=csv.QUOTE_ALL)
        print(datetime.now() - startTime)

print(datetime.now() - scriptStart)