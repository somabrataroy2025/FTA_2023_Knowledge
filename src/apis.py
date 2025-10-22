from fastapi import FastAPI,Depends
from fastapi.security import OAuth2PasswordBearer,OAuth2PasswordRequestForm

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

@app.post('/token')
async def token(formdata: OAuth2PasswordRequestForm = Depends()):
    return { 'access_token' :  formdata.username }

@app.get('/')
async def index(token:str = Depends(oauth2_scheme)):
    return {'the_token' : token}
