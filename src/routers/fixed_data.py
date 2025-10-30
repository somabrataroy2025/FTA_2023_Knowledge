from fastapi.routing import APIRouter
from BL.controller import actions
from BL.model import Country
from fastapi import APIRouter,Request
from .tracker import tracker

apirouter = APIRouter()

@apirouter.get('/data/countries',
operation_id='get_countries',
response_model=list[Country],
name = 'get_countries')
@tracker
async def get_countries(req:Request)->list[Country]:    
    return actions.fetch_all_Countries()
