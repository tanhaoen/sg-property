from email.mime import base
from azure_connector import AzureConn
import requests
import pandas as pd
from datetime import datetime
import urllib.parse
from transaction_cleaner import cleaner
import sqlalchemy
import numpy as np
from onemap_io import OneMap, get_address_details

dbc = AzureConn('sg_property')

resource_ids = {"resale_flat_prices": "f1765b54-a209-4718-8d38-a39237f502b3"}

# Get last updated month from transactions table
update_month = dbc.query("SELECT MAX(transaction_month) FROM transactions").iat[0,0]
current_month = datetime.today().strftime("%Y-%m-%d")
month_list = pd.date_range(update_month,current_month,freq='MS').strftime('%Y-%m').tolist()

# Load data from data.gov API
base_api_url = f"https://data.gov.sg/api/action/datastore_search?resource_id={resource_ids['resale_flat_prices']}&limit=5000"

data = []
for month in month_list[1:-1]:
    month_query = '{"month":"' + month + '"}'
    month_query = urllib.parse.quote(month_query)
    api_url = base_api_url + f"&q={month_query}"
    monthly_data = pd.DataFrame(requests.get(api_url).json()['result']['records'])
    data.append(monthly_data)

data = pd.concat(data)

# Reformat data to DB table
data = data.drop(["_id"], axis=1)
data = cleaner(data)

# Get list of new addresses
address_list_db = dbc.query('SELECT DISTINCT address FROM building_info').iloc[:,0].tolist()
address_list_data = (data['block']+' '+data['street']).unique().tolist()
new_addresses = np.setdiff1d(address_list_data,address_list_db)

# Query postcode, building name 
om = OneMap('')
address_df = get_address_details(om,new_addresses)
# Append dataframes to respective SQL tables
dbc.append(data,'transactions')