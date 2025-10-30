from datetime import datetime
from ipaddress import ip_address
from itertools import starmap
from operator import call
import string
from pydantic import BaseModel,Field
from urllib3 import response

class Country(BaseModel):
    name:str
    
class Airport_Stat(BaseModel):
    country:str
    airport_name:str
    val: int

class AgeGroup_Stat(BaseModel):
    country:str
    age_group:str
    val: int



class ReqTracker():
    def __init__(self,method,calldate,user,param) -> None:
        self.method = method
        self.calldate = calldate
        self.user = user
        self.param = param



    #latency:int = 0
    # method:str
    # calldate:datetime
    # user:str = 'somabrata'
    # #source:str|None
    # param : str = Field(pattern=ASCII_PATTERN)

