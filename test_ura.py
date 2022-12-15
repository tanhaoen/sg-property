import requests
import pandas as pd
import numpy as np

access_key = "24cadd12-2a4c-4e65-a853-824b5dcc2510"

token_url = "https://www.ura.gov.sg/uraDataService/insertNewToken.action"
token = requests.post(token_url,headers={"AccessKey": access_key,'User-Agent': 'Mozilla/5.0'}).json()['Result']

url = "https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental_Median"
data = requests.post(url,headers={"AccessKey":access_key, "token":token, "User-Agent":"Mozilla/5.0"},params={"batch":1}).json()["Result"]

df = pd.DataFrame(data)


print('done')
