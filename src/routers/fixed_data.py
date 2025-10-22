from fastapi.routing import APIRouter
from BL.controller import actions
from BL.model import Country
from fastapi import APIRouter

apirouter = APIRouter()

@apirouter.get('/data/countries',operation_id='get_countries',response_model=list[Country])
async def get_countries()->list[Country]:
    return actions.fetch_all_Countries()
