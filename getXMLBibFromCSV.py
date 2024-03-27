import requests
import secret
import argparse
import pandas as pd
from xml.etree import ElementTree

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

for mms_id in mms_id_list:
    # Create URL for specific mms_id record.
    full_url = baseURL+endpoint+str(mms_id)+'?expand=p_avail'
    print(full_url)
    # Make request for bib.
    try:
        r = requests.get(full_url, headers=headers)
        tree = ElementTree.fromstring(r.content)
        record = tree.find('record')
        string_record = ElementTree.tostring(record, encoding='unicode')
        print(string_record)
        xml_filename = 'bib_'+str(mms_id)+'.xml'
        with open(xml_filename, 'w') as f:
            f.write(string_record)
    except requests.exceptions:
        print('oh no!')

