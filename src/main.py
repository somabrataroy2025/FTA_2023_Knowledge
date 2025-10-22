from BL.controller import actions
import routers.stats as st
import uvicorn
from routers.api import app
from fastapi_mcp import FastApiMCP


if __name__ == "__main__":
    #actions.fetch_agewise_stat()
    #st.myFunc()
    mcp = FastApiMCP(app,
    include_operations=["get_stat_agewise","get_countries"])
    mcp.mount()
    uvicorn.run(app,host="0.0.0.0",port=8000)