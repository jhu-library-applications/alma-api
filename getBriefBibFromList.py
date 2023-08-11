import requests
import secret

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

# Create headers to authorize with api_key. and to accept request output in JSON.
headers = {"Authorization": "apikey "+api_key,
           "Accept": "application/json"}
# Create parameters variable to ask for only brief bibs.
params = {'view': 'brief'}

# List of mms_ids.
mms_id_list = ['991007192979707861', '991040869259707861', '991003844629707861']

for mms_id in mms_id_list:
    # Create URL to ask for specific mms_id record.
    full_url = baseURL+endpoint+mms_id
    print(full_url)
    r = requests.get(full_url, headers=headers, params=params).json()
    for key, value in r.items():
        print(key, value)