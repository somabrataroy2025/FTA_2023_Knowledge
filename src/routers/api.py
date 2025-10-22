from fastapi import FastAPI
from routers import stats,fixed_data
from fastapi_mcp import FastApiMCP

app = FastAPI()

app.include_router(stats.apirouter)
app.include_router(fixed_data.apirouter)
    