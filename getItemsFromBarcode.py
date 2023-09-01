import requests
import secret
import pandas as pd

baseURL = 'https://api-na.hosted.exlibrisgroup.com'

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
           "Accept": "application/json",
           "Content-Type": "application/json"}

filename = 'lsc_afa_freezed_copy_2023-07-17.csv'
df = pd.read_csv(filename, dtype='string')


for count, row in df.iterrows():
    current_barcode = row['current_barcode']
    print(count, current_barcode)
    barcode_endpoint = '/almaws/v1/items?item_barcode={}'.format(current_barcode)
    get_item_url = baseURL + barcode_endpoint
    # Make request for item.
    item_metadata = requests.get(get_item_url, headers=headers).json()
    try:
        # Try getting 'link' field from item_metadata.
        full_link = item_metadata['link']
        print(full_link)
    except KeyError:
        print(item_metadata)