from fastapi.routing import APIRouter
from BL.controller import actions
from BL.model import AgeGroup_Stat
from fastapi import APIRouter

apirouter = APIRouter()

@apirouter.get('/data/agewise',operation_id='get_stat_agewise',response_model=list[AgeGroup_Stat])
async def get_stat_agewise()->list[AgeGroup_Stat]:
    return actions.fetch_agewise_stat()
