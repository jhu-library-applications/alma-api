import requests
import argparse
from xml.etree import ElementTree as ET
import pandas as pd
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-c', '-column_name')
parser.add_argument('-s', '--search_field')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')
if args.search_field:
    search_field = args.search_field
else:
    search_field = input('Enter name of search_field: ')
if args.column_name:
    column_name= args.column_name
else:
    column_name = input('Enter name of column_name: ')

baseURL = 'https://jhu.alma.exlibrisgroup.com/view/sru/01JHU_INST'
endpoint = '?version=1.2&operation=searchRetrieve&recordSchema=marcxml&query=alma.'

df = pd.read_csv(filename, dtype='string')

all_records = []
for count, row in df.iterrows():
    record_dict = row.copy()
    search_value = row.get(column_name)
    full_url = baseURL+endpoint+search_field+'='+search_value
    print(count, full_url)
    # Make request for bib.
    try:
        r = requests.get(full_url)
        tree = ET.fromstring(r.content)
        ns = {'srw': 'http://www.loc.gov/zing/srw/'}
        total_results = tree.find('srw:numberOfRecords', ns)
        total_results = total_results.text
        total_results = int(total_results)
        if total_results >= 1:
            mms_ids = []
            record_dict['match'] = True
            records = tree.find('srw:records', ns)
            for record in records:
                mms_id = record.find('srw:recordIdentifier', ns)
                mms_id = mms_id.text
                mms_ids.append(mms_id)
            '|'.join(mms_ids)
            record_dict['mms_ids'] = mms_ids
        else:
            record_dict['match'] = False
        all_records.append(record_dict)
    except requests.exceptions:
        print('oh no!')
        pass

updated_df = pd.DataFrame.from_records(all_records)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
updated_df.to_csv('results_'+dt+'.csv', index=False)
