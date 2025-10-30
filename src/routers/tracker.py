from textwrap import indent
import datetime,time
from fastapi import Request
from functools import wraps
from BL.model import ReqTracker
from BL.controller import actions

import json as js

def tracker(func):
    @wraps(func)
    async def wrapper(*args,**kwargs):
        for kw in kwargs:
            if isinstance(kwargs[kw],Request):
                rq = Request(kwargs[kw])
         
        st_time = time.time()
        result = await func(*args,**kwargs)
        elapsedTime = time.time() - st_time

        reqItem = {
                'method':rq.scope['route'].name,
                'param':rq.query_params.items(),
                'calldate': datetime.datetime.now(),
                'user':'Somabrata',
                'latency' : elapsedTime
            }
        actions.update_tracker(reqItem)
        return result
    return wrapper


