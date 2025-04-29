import requests
import secret
import argparse
import pandas as pd
from xml.etree import ElementTree as ET
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


tag_list = ['245', '500', '541', '561', '700', '710']

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
    try:
        r = requests.get(full_url, headers=headers)
    except requests.exceptions:
        print('oh no!')
        pass
    tree = ET.fromstring(r.content)
    record = tree.find('record')
    for field in record:
        field_attributes = field.attrib
        tag = field_attributes.get('tag')
        if tag in tag_list:
            indicator_1 = field_attributes['ind1']
            indicator_2 = field_attributes['ind2']
            field_info = tag+'_'+indicator_1+indicator_2
            field_string = []
            for subfield in field:
                subfield_text = subfield.text
                subfield_attributes = subfield.attrib
                code = subfield_attributes['code']
                subfield_string = '$'+code+' '+subfield_text
                field_string.append(subfield_string)
            if record_dict.get(field_info) is None:
                    record_dict[field_info] = ' '.join(field_string)
            else:
                existing_text = record_dict[field_info]
                new_text = existing_text+'|'+' '.join(field_string)
                record_dict[field_info] = new_text
    all_records.append(record_dict)

updated_df = pd.DataFrame.from_records(all_records)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
updated_df.to_csv('bibMetadata_'+dt+'.csv', index=False)
