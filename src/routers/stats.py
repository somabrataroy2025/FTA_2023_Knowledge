from fastapi.routing import APIRouter
from BL.controller import actions
from BL.model import AgeGroup_Stat, Airport_Stat
from fastapi import APIRouter,status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse,Response

apirouter = APIRouter()

@apirouter.get('/data/agewise',operation_id='get_stat_agewise',response_model=list[AgeGroup_Stat])
async def get_stat_agewise(country:str,age_from:int,age_to:int)->list[AgeGroup_Stat]:
    return actions.fetch_agewise_stat(country,age_from,age_to)

@apirouter.get('/data/airport',operation_id='get_stat_airport',response_model=list[Airport_Stat])
async def get_stat_airport(country:str,airport:str)->list[Airport_Stat]:
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
