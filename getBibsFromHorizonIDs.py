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
endpoint = '/almaws/v1/bibs/?view=brief&expand=None&other_system_id='

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



# List of horizon_ids

df = pd.read_csv(filename)
horizon_list = df['horizon_id'].to_list()
horizon_list = set(horizon_list)
horizon_list = list(horizon_list)

all_records = []
for count, horizon_id in enumerate(horizon_list):
    # Create URL for specific horizon_id record.
    full_url = baseURL+endpoint+str(horizon_id)
    print(count, full_url)
    # Make request for bib.
    try:
        r = requests.get(full_url, headers=headers).json()
        bibs = r.get('bib')
        if bibs:
            for bib in bibs:
                print()
                record_dict = {'horizon_id': horizon_id}
                mms_id = bib['mms_id']
                title = bib['title']
                record_dict['mms_id'] = mms_id
                record_dict['title'] =title
                print(record_dict)
                all_records.append(record_dict)
    except requests.exceptions:
        print('oh no!')
        pass

updated_df = pd.DataFrame.from_records(all_records)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
updated_df.to_csv('bibMetadata_'+dt+'.csv', index=False)
