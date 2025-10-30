from fastapi.routing import APIRouter
from httpx import request
from BL.controller import actions
from BL.model import AgeGroup_Stat, Airport_Stat
from fastapi import APIRouter,status,Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse,Response
from .tracker import tracker

apirouter = APIRouter()

@apirouter.get('/data/agewise',
operation_id='get_stat_agewise',
response_model=list[AgeGroup_Stat],
name = 'get_stat_agewise')
@tracker
async def get_stat_agewise(country:str,age_from:int,age_to:int,req:Request)->list[AgeGroup_Stat]:
    data =  actions.fetch_agewise_stat(country,age_from,age_to)
    if data:
        json_data = jsonable_encoder(data)
        #return data
        return JSONResponse(
            content= json_data,
            status_code= status.HTTP_200_OK
        )
    else:
        return JSONResponse(
            content= {},
            status_code= status.HTTP_204_NO_CONTENT
        )



@apirouter.get('/data/airport',
operation_id='get_stat_airport',
response_model=list[Airport_Stat],
name = 'get_stat_airport')
@tracker
async def get_stat_airport(country:str,airport:str,req:Request)->list[Airport_Stat]:
    data = actions.fetch_airport_stat(country,airport)
    if data:
        json_data = jsonable_encoder(data)
        #return data
        return JSONResponse(
            content= json_data,
            status_code= status.HTTP_200_OK
        )
    else:
        return JSONResponse(
            content= {},
            status_code= status.HTTP_204_NO_CONTENT
        )
