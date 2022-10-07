from audioop import add
from re import search
import requests
import pandas as pd
from tqdm import tqdm

class OneMap:
    def __init__(self,token):
        self.token = token
        self.common = "https://developers.onemap.sg/commonapi"
        self.private = "https://developers.onemap.sg/privateapi"
    def address(self,searchVal,returnGeom='Y',getAddrDetails='Y',pageNum=None):
        url = self.common+f"/search?searchVal={searchVal}&returnGeom={returnGeom}&getAddrDetails={getAddrDetails}"
        if pageNum is not None:
            try:
                url = url+f"&pageNum={pageNum}"
            except:
                pass
        return requests.get(url).json()
    
    def routing(self):
        """
        method still in progress
        """
        pass

def get_address_details(om,address_list):
    final_list = []
    unknown_counter = 0
    for address in tqdm(address_list, desc="OneMap address query"):
        searchdict = om.address(address)
        results = searchdict["results"]
        if searchdict["found"]==1:
            estate = address if results[0]["BUILDING"]=="NIL" else results[0]["BUILDING"]
            postcode = results[0]["POSTAL"]
        else:
            for i in results:
                if i["BUILDING"]=="NIL":
                    estate = address
                    postcode = i["POSTAL"]
                else:
                    estate = "UNKNOWN"
                    postcode = "UNKNOWN"
        if estate == "UNKNOWN":
            unknown_counter += 1
        final_list.append((address,estate,postcode))
    print(f"{unknown_counter} addresses could not be found via OneMap")
    return pd.DataFrame(final_list, columns=["address","estate","postcode"])
