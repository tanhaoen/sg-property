import pandas as pd
import numpy as np
import requests
import re

"""
Process flow
- Get last date available from transaction table
- Get list of addresses and postal codes from address table
- Query transaction data from data.gov API until last available date in DB
- Clean transaction data
- Append to transaction table

OneMap query module
- Query null values from left join
(SELECT address_street FROM transactions LEFT JOIN address USING(address_street) WHERE postal_code ISNULL)
- Query OneMap API for correct postal code of missing addresses (include user selection for ambiguous addresses)
"""

def split_lease(x):
    """
    Convert the lease remaining from "{x} years {y} months" into a float
    """
    try:
        return float(x)
    except:
        years = re.findall(r'\d+(?= years)',x)
        months = re.findall(r'\d+(?= months)',x)
        years,months = map(lambda a: int(a[0]) if len(a)>0 else 0,[years,months])
        remaining = years+(months/12)

        return round(remaining,2)

datagov_api = "https://data.gov.sg/api/action/datastore_search?resource_id=f1765b54-a209-4718-8d38-a39237f502b3&limit=400000"

#Query the API for the latest dataset
df = pd.DataFrame(requests.get(datagov_api).json()['result']['records'])

#Convert string to month
df['month'] = pd.to_datetime(df['month'])

#Convert resale price and floor area to int
df['resale_price'] = df['resale_price'].astype(float).round(0).astype(int)
df['floor_area_sqm'] = df['floor_area_sqm'].astype(float)

#Convert floor area from sq metres to sq foot (more commonly used)
df['floor_area_sqft'] = df['floor_area_sqm'].apply(lambda x: x*10.7639).round(2)
df.drop('floor_area_sqm',axis=1,inplace=True)
    
#Convert remaining lease to years
df['remaining_lease'] = df['remaining_lease'].apply(split_lease)

#Add resale price per square metre
df['psf'] = df['resale_price']/df['floor_area_sqft']

#Rename columns
df.rename(columns={'street_name':'street'},inplace=True)

#Output to transaction table
