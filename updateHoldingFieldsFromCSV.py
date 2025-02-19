import requests
import secret
import argparse
import pandas as pd
from xml.etree import ElementTree
import csv
from datetime import datetime
import time

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

# Create headers to authorize with api_key and to request output in XML.
headers = {"Authorization": "apikey "+api_key,
           "Accept": "application/xml"}

# Start a requests Session.
s = requests.Session()
s.headers = headers

# Grab values from specified field in XML record.
def check_field_values(tree_object, xml_attribute, xml_text):
    updated_values = []
    # Find relevant field value in ElementTree object.
    xml_string = ".//datafield[@" + xml_attribute + "='" + xml_text + "']"
    data_fields = tree_object.findall(xml_string)
    for data_field in data_fields:
        subfield = data_field.find('subfield')
        updated_value = subfield.text
        updated_values.append(updated_value)

    updated_values =';'.join(updated_values)
    return updated_values

# Find and replace values from specified field in XML record.
def update_field_values(tree_object, xml_attribute, xml_text, value_pair):
    # Find relevant field value in ElementTree object and replace
        xml_string = ".//datafield[@"+xml_attribute+"='"+xml_text+"']"
        data_fields = tree_object.findall(xml_string)
        for data_field in data_fields:
            subfield = data_field.find('subfield')
            text_866 = subfield.text
            new = value_pair.get(text_866)
            if new is not None:
                subfield.text = new

# Read CSV as a DataFrame.
df = pd.read_csv(filename, dtype={'mms_id': int, 'holding_id': int,
                                  'old_value': str, 'new_value': str})
# Convert values into list using delimiter.
df['old_value'] = df['old_value'].str.split(';')
df['new_value'] = df['new_value'].str.split(';')
total_holdings = len(df)

# Match old_value with new_value using dictionary.
value_dictionary = []
for count, row in df.iterrows():
    old_value = row['old_value']
    old_value = [val.strip() for val in old_value]
    new_value = row['new_value']
    new_value = [val.strip() for val in new_value]
    combined = dict(zip(old_value, new_value))
    value_dictionary.append(combined)
df['value_dictonary'] = value_dictionary

item_log = []
for count, row in df.iterrows():
    print('')
    log = row.copy()
    # Get holding information from DataFrame.
    mms_id = row['mms_id']
    holding_id = row['holding_id']
    values = row['value_dictonary']
    # Create URL for specific holding record.
    full_link = baseURL+endpoint+str(mms_id)+'/holdings/'+str(holding_id)+'?expand=p_avail'
    print("Retrieving {} of {}. mms_id: {}, holding_id: {}.".format(count, total_holdings, mms_id, holding_id))
    # Make request for holding record.
    try:
        get_response = s.get(full_link, headers=headers)
        retrieved_tree = ElementTree.fromstring(get_response.content)
        record_tree = ElementTree.fromstring(get_response.content)
        print('Holding record retrieved.')
    except TimeoutError:
        get_error = 'GET Timeout Error'
        print(get_error)
        log['error'] = get_error
        item_log.append(log)
        continue
    except requests.exceptions.ReadTimeout:
        get_error = 'GET Timeout Error'
        print(get_error)
        log['error'] = 'GET Timeout Error'
        item_log.append(log)
        continue
    # Convert XML string to an ElementTree object and find and replace old_values.
    update_field_values(record_tree, 'tag', '866', values)
    if record_tree != retrieved_tree:
        # Convert ElementTree object to XML string.
        updated_holding = ElementTree.tostring(record_tree)
        # Update Session headers to send XML string.
        s.headers.update({"Content-Type": "application/xml"})
        try:
            # Update holding record in Alma.
            put_response = s.put(full_link, data=updated_holding, timeout=6)
            if put_response.status_code == 200:
                print('Holding record updated')
                updated_tree = ElementTree.fromstring(put_response.content)
                # Gather new values from specified field to ensure update worked.
                updated_fields = check_field_values(updated_tree, 'tag', '866')
                log['updated_field'] = updated_fields
            else:
                put_error = 'PUT General Error'
                print(put_error)
                log['error'] = put_error
        except requests.exceptions.Timeout:
            put_error = 'PUT Timeout Error'
            print(put_error)
            log['error'] = put_error
    else:
        error = 'No updates to XML detected. PUT command skipped.'
        print(error)
        log['updated_field'] = error
    item_log.append(log)

# Convert item_log to DataFrame.
log_df = pd.DataFrame.from_records(item_log)
log_df['old_value'] = log_df['old_value'].str.join(';')
log_df['new_value'] = log_df['new_value'].str.join(';')
print(log_df)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
# Create CSV using DataFrame log. Quote all fields to avoid barcodes converting to scientific notation.
log_df.to_csv('updatedHoldingFieldsLog_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)

print("--- %s seconds ---" % (time.time() - start_time))


                



