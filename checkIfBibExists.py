import requests
import secret
import argparse
import pandas as pd
from datetime import datetime

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



# List of mms_ids.

df = pd.read_csv(filename, dtype={'mms_id': int})
mms_id_list = df['mms_id'].to_list()

all_records = []
for count, mms_id in enumerate(mms_id_list):
    # Create URL for specific mms_id record.
    full_url = baseURL+endpoint+str(mms_id)+'?expand=p_avail'
    print(count, full_url)
    record_dict = {'mms_id': mms_id}
    # Make request for bib.
    r = requests.get(full_url, headers=headers)
    if r.status_code == 400:
        record_dict['exists'] = False
    else:
        record_dict['exists'] = True

    all_records.append(record_dict)

updated_df = pd.DataFrame.from_records(all_records)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
updated_df.to_csv('bibMetadata_'+dt+'.csv', index=False)
