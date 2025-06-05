import requests
import secret
import argparse
import pandas as pd
from xml.etree import ElementTree
from fieldstoadd import fields
import csv
from datetime import datetime
import time
import quickalmaxml as qax

start_time = time.time()

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')

baseURL = 'https://api-na.hosted.exlibrisgroup.com'
endpoint = '/almaws/v1/bibs/'

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
           "Accept": "application/xml"}

# Start a requests Session.
s = requests.Session()
s.headers = headers

# Gather list of mms_ids from CSV.
df = pd.read_csv(filename)


item_log = []
for count, row in df.items():
    # Create URL for specific mms_id record.
    mms_id = row.get('mms_id')
    access_url = row.get('url')
    pid = row.get('pid')
    fields['856']['subfields']['g'] = pid
    fields['856']['subfields']['u'] = access_url
    full_url = baseURL+endpoint+str(mms_id)
    print(count, full_url)
    log = {'mms_id': mms_id}
    # Make request for bib.
    try:
        r = s.get(full_url+'?expand=p_avail', headers=headers)
        tree = ElementTree.fromstring(r.content)
        record = tree.find('record')
        # Add new fields to bib record.
        for k, v in fields.items():
            new_field = qax.add_field(k, v)
            record.append(new_field)
        updated_record = ElementTree.tostring(tree)
        try:
            # Update holding record in Alma.
            s.headers.update({"Content-Type": "application/xml"})
            put_response = s.put(full_url, data=updated_record, timeout=20)
            if put_response.status_code == 200:
                print('Bib record updated.')
                updated_tree = ElementTree.fromstring(put_response.content)
                # Gather new values from specified field to ensure update worked.
                for k, v in fields.items():
                    updated = qax.confirm_field_values(updated_tree, 'tag', k)
                    updated_label = 'updated_'+k
                    log[updated_label] = updated
            else:
                error = qax.get_error_message(put_response)
                log['error'] = error
        except requests.exceptions.Timeout:
            put_error = 'PUT Timeout Error'
            print(put_error)
            log['error'] = put_error
    except requests.exceptions:
        print('oh no!')
    item_log.append(log)

# Convert item_log to DataFrame.
log_df = pd.DataFrame.from_records(item_log)
print(log_df.head)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('updateBibsLog_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)

print("--- %s seconds ---" % (time.time() - start_time))
