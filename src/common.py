from ast import List
from neo4j import Driver,GraphDatabase
from graphdatascience import GraphDataScience
from dotenv import load_dotenv
from enum import Enum
import json,os,spacy,math, pandas as pd
from spacy.matcher import Matcher

class NeoDB:
    DB_Name:str
    def __init__(self):
        load_dotenv()
        self.DB_Name = os.getenv('NEO4J_DB')

    def neoDriver(self) -> Driver:
        driver = GraphDatabase.driver(
        os.getenv('NEO4J_URI'),
        auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')),
        database=os.getenv('NEO4J_DB')
        )
        return driver
    
    def neoGDS(self)-> GraphDataScience:
        driver = self.neoDriver()
        gds = GraphDataScience.from_neo4j_driver(driver=driver)
        gds.set_database(os.getenv('NEO4J_DB'))
        return gds

class Sheet(Enum):
    FTA_Region = "FTA_Region"
    FTA_Country_Region = "FTA_Country_Region"
    FTA_Count = "FTA_Count"
    FTA_Airport_State_Wise = "FTA_Airport_State_Wise"
    FTA_Gender_Wise = "FTA_Gender_Wise"
    FTA_Age_Wise = "FTA_Age_Wise"
    FTA_Stay="FTA_Stay"

class Common():
    filePath:str
    region:List
    def __init__(self):
        load_dotenv()
        self.filePath = os.getenv('NEO4J_USERNAME')
        self.region = ['North America',
                        'Central And South America',
                        'Western Europe',
                        'Eastern Europe',
                        'Africa',
                        'West Asia',
                        'South Asia',
                        'South East Asia',
                        'East Asia',
                        'Australasia'
                        ]
        
    def _is_convertible_to_number(self,s:any) -> bool:
        try:
            float(s)  # Try converting to float (handles both integers and floats)
            return True
        except ValueError:
            return False
        
    def calcPerc(self,val:any, perc:any) -> int|None:
        if self._is_convertible_to_number(val) and self._is_convertible_to_number(perc):
            return math.ceil(float(val) * (float(perc) / 100))
        else:
            return None
    
    #return a tuple (isregion,iscountry,isother,istotal)
    def CheckFirstRowCol(self,input:pd.Series):
        region = [str(item).lower() for item in Common().region] 
        data = str(input.iloc[0]).lower().strip()
        
        if data in region:
            return True,False,False,False
        elif 'other' in data:
            return False,False,True,False
        elif 'total' in data:
            return False,False,False,True
        else:
            return False,True,False,False
    
    def transformDataFrame(self,df:pd.DataFrame)->pd.DataFrame|None:
        df_Result = pd.DataFrame(columns=["Region","Country","Key","Val"])
        hm = HeaderMatcher()
        hdrList = {}
        region=''

        #remove all NAN / Blank rows from input

        df = df.dropna(how='all')

        for index,row in df.iterrows():
            country=''
            isRegion,isCountry,isOther,isTotal = self.CheckFirstRowCol(row)
            if isRegion:
                region = row.iloc[0]
            if isCountry:
                country = row.iloc[0]       
            if isOther:
                country = region+"-"+row.iloc[0]  
            if isTotal:
                continue
            if country != '':
                for col_index in range(df.shape[1]):
                    if col_index > 1 and col_index < len(df):
                        _hdr = df.columns[col_index]
                        _headerText = ''
                        #print(_hdr)
                        if _hdr in hdrList:
                            _headerText = hdrList[_hdr]
                        else: 
                            _headerText = hm.getHeaderMatched(_hdr)
                            hdrList[_hdr] = _headerText
                        #print(f"{country} - {_headerText}")
                        newRow = {'Region' : region, 
                                'Country' : country, 
                                'Key' : _headerText,
                                'Val' : Common().calcPerc(row.iloc[1],row.iloc[col_index])
                                    }
                        dfNewRow= pd.DataFrame([newRow])
                        df_Result = pd.concat([df_Result,dfNewRow])
        
        if len(df_Result) > 0:
            return df_Result
        else:
            return None

class HeaderMatcher():
    header_config:str
    header_matcher:Matcher
    nlp = spacy.load("en_core_web_sm")

    def __init__(self):
        self.header_config='header_config.txt'
        self.header_matcher = Matcher(self.nlp.vocab)
        self.header_matcher = self._createHeaderMatcher()

    def _createHeaderMatcher(self)->Matcher:
        matcherlist = self.header_matcher
        with open(self.header_config, "r",encoding="utf-8") as f:
            for line in f:
                patterndata = json.loads(line.strip())
                if(isinstance(patterndata["pattern"],list)):
                    matcherlist.add(key= patterndata["label"],patterns=[patterndata["pattern"]])
        return matcherlist

    #get skills
    def getHeaderMatched(self,hdr_txt:str)->str|None:
        header_val=""
        doc = self.nlp(hdr_txt)
        matches = self.header_matcher(doc)
        for match_id,start,end in matches:
            if header_val == "":
                header_val = self.nlp.vocab.strings[match_id]
                break
        return header_val

    if __name__ == "__main__":
        pass