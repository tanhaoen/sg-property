from re import search
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm

class OneMap:
    def __init__(self,token):
        self.token = token
        self.common = "https://developers.onemap.sg/commonapi"
        self.popapi = "https://developers.onemap.sg/privateapi/popapi"
        self.pop_queries = {"Economic Status":"getEconomicStatus",
        "Education Status":"getEducationAttending",
        "Ethnic Distribution":"getEthnicGroup",
        "Work Income for Household (Monthly)":"getHouseholdMonthlyIncomeWork",
        "Household Size":"getHouseholdSize",
        "Household Structure":"getHouseholdStructure",
        "Income from Work":"getIncomeFromWork",
        "Industry of Population":"getIndustry",
        "Language Literacy":"getLanguageLiterate",
        "Marital Status":"getMaritalStatus",
        "Mode of Transport to School":"getModeOfTransportSchool",
        "Mode of Transport to Work":"getModeOfTransportWork",
        "Occupation":"getOccupation",
        "Age":"getPopulationAgeGroup",
        "Religion":"getReligion",
        "Spoken Language":"getSpokenAtHome",
        "Tenancy":"getTenancy",
        "Dwelling Type Household":"getTypeOfDwellingHousehold",
        "Dwelling Type Population":"getTypeOfDwellingPop",
        }

    def address(self,searchVal,returnGeom='Y',getAddrDetails='Y',pageNum=None):
        url = self.common+f"/search?searchVal={searchVal}&returnGeom={returnGeom}&getAddrDetails={getAddrDetails}"
        if pageNum is not None:
            try:
                url = url+f"&pageNum={pageNum}"
            except:
                pass
        return requests.get(url).json()

    def population(self,**kwargs):
        print(kwargs)
        if "query" not in kwargs:
            newdict = dict(zip(list(range(len(self.pop_queries))),self.pop_queries.keys()))
            for k,v in newdict.items():
                print(f"{k}: {v}")
            querynum = input("Choose a query: ")
            query = newdict[querynum]
            query = self.pop_queries[query]
            planningArea = input("Input a planning area: ")
            year = input("Input a year: ")
        else:
            query = kwargs["query"]
            planningArea = kwargs["planningArea"]
            year = kwargs["year"]
                
        url = self.popapi+f"/{query}?token={self.token}&planningArea={planningArea}&year={year}"
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

class URA:
    def __init__(self,filepath):
        #read access key from file
        with open(filepath) as f:
            self.access_key = f.readline().rstrip()
        token_url = "https://www.ura.gov.sg/uraDataService/insertNewToken.action"
        token_response = requests.post(token_url, headers={"AccessKey":self.access_key,"User-Agent":"Mozilla/5.0"})
        if token_response.status_code==200:
            self.token = token_response.json()["Result"]
        else:
            print("Failed to load token")

    def load(self,query,**kwargs):
        query_list = {"private sale": "PMI_Resi_Transaction",
        "private median rental": "PMI_Resi_Rental_Median",
        "private rental contract": "PMI_Resi_Rental",
        "private developer sale": "PMI_Resi_Developer_Sales",
        "private pipeline": "PMI_Resi_Pipeline",
        "planning decision": "Planning_Decision"}
        query = query_list[query]
        query_url = f"https://www.ura.gov.sg/uraDataService/invokeUraDS?service={query}"
        if query=="PMI_Resi_Transaction":
            batchlist = []
            for batch in range(1,5):
                data = self.post_query(query_url,params={"batch":batch})
                batchlist.append(data)
            batchdata = pd.concat(batchlist)
            df = self.data_expander(batchdata,'transaction')
            return df
        elif query=="PMI_Resi_Rental_Median":
            data = self.post_query(query_url)
            df = self.data_expander(data,'rentalMedian')
            return df
        elif query=="PMI_Resi_Rental":
            data = self.post_query(query_url,params=kwargs)
            df = self.data_expander(data,'rental')
            return df
        elif query=="PMI_Resi_Developer_Sales":
            data = self.post_query(query_url,params=kwargs)
            df = self.data_expander(data,'developerSales')
            return df
        elif query=="Planning_Decision":
            data = self.post_query(query_url,params=kwargs)
            if not data.empty:
                return data
        else:
            data = self.post_query(query_url)
            if not data.empty:
                return data
    
    def post_query(self,query_url,**kwargs):
        data = requests.post(query_url,**kwargs,headers={"AccessKey":self.access_key, "token":self.token, "User-Agent":"Mozilla/5.0"}).json()["Result"]
        return pd.DataFrame(data)

    def data_expander(self,data,expand_col):
        def row_expander(row):
            data = pd.DataFrame(row[expand_col])
            data['idx'] = row.name
            return data
        if not data.empty:
            df = pd.concat(data.apply(row_expander,axis=1).tolist()).merge(data.drop(expand_col,axis=1),left_on='idx',right_index=True)
            df = df.drop('idx',axis=1)
            return df
        