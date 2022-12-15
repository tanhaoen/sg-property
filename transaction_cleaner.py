import pandas as pd
import numpy as np
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

def hdb_cleaner(data):
    #Convert string to month
    data['month'] = pd.to_datetime(data['month'])

    #Convert resale price and floor area to int
    data['resale_price'] = data['resale_price'].astype(float).round(0).astype(int)
    data['floor_area_sqm'] = data['floor_area_sqm'].astype(float)

    #Convert floor area from sq metres to sq foot (more commonly used)
    data['floor_area_sqft'] = data['floor_area_sqm'].apply(lambda x: x*10.7639).round(2)
    data.drop('floor_area_sqm',axis=1,inplace=True)
        
    #Convert remaining lease to years
    data['remaining_lease'] = data['remaining_lease'].apply(split_lease)

    #Add resale price per square metre
    data['psf'] = data['resale_price']/data['floor_area_sqft']

    #Rename columns
    data = data.rename(columns={'street_name':'street', 'month':'transaction_month'})

    #Reorder columns
    data = data.reindex(columns=['transaction_month','town','street','block','flat_type','flat_model','floor_area_sqft','storey_range','lease_commence_date','remaining_lease','resale_price','psf'])
    data = data.reset_index(drop=True)

    return data

def ura_cleaner(data):
    column_rename_map = {"area":"floor_area_sqft","floorRange":"storey_range","noOfUnits":"num_units",
    "contractDate":"transaction_month","typeOfSale":"transaction_type","propertyType":"property_type",
    "typeOfArea":"area_type","marketSegment":"market_segment","median":"median_psf",
    "psf75":"75_percentile_psf","psf25":"25_percentile_psf","refPeriod":"quarter",
    "areaSqm":"floor_area_range_sqm","areaSqft":"floor_area_range_sqft","noOfBedRoom":"num_bedrooms",
    "rent":"rental_price",""}