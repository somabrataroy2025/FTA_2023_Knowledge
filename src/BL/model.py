from itertools import starmap
from pydantic import BaseModel

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
