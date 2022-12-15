from azure_connector import AzureConn
import requests
import pandas as pd
from datetime import datetime
import urllib.parse
import numpy as np
from api_center import OneMap, URA, get_address_details
import json

dbc = AzureConn('sg_property')

api = URA('/Users/nathaniel/ura_accesskey.txt')

ura_sale = api.load("private sale")
dbc.df_to_table(ura_sale,"ura_private_sale")

ura_median_rental = api.load("private median rental")
dbc.df_to_table(ura_median_rental,"ura_median_rental")

ura_rental_contract = []
refPeriod = ['22','q','3']
while True:
    print(f"Loading rental contract for {''.join(refPeriod)}")
    data = api.load("private rental contract",refPeriod=''.join(refPeriod))
    if data is None:
        break
    ura_rental_contract.append(data)
    if int(refPeriod[-1])>1:
        refPeriod[-1] = str(int(refPeriod[-1])-1)
    else:
        refPeriod[-1] = "4"
        refPeriod[0] = str(int(refPeriod[0])-1)
ura_rental_contract = pd.concat(ura_rental_contract)
dbc.df_to_table(ura_rental_contract,"ura_rental_contract")

ura_developer_sale = []
refPeriod = ['10','22']
while True:
    print(f"Loading developer sales for {''.join(refPeriod)}")
    data = api.load("private developer sale",refPeriod=''.join(refPeriod))
    if data is None:
        break
    ura_developer_sale.append(data)
    if int(refPeriod[0])>1:
        refPeriod[0] = str(int(refPeriod[0])-1).zfill(2)
    else:
        refPeriod[0] = "12"
        refPeriod[1] = str(int(refPeriod[1])-1).zfill(2)
ura_developer_sale = pd.concat(ura_developer_sale)
dbc.df_to_table(ura_developer_sale,"ura_developer_sale")

ura_pipeline = api.load("private pipeline")
dbc.df_to_table(ura_pipeline,"ura_pipeline")

ura_planning_decision = []
year = 2022
while True:
    print(f"Loading planning decisions for {year}")
    data = api.load("planning decision",year=year)
    if data is None:
        break
    ura_planning_decision.append(data)
    year -= 1
ura_planning_decision = pd.concat(ura_planning_decision)
dbc.df_to_table(ura_planning_decision,"ura_planning_decision")
