from api.db import get_epa
import api.load as load
import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
from upstash_qstash import Client

load_dotenv()

#client = Client(os.environ.get("QSTASH_TOKEN"))
#schedules = client.schedules()
#res = schedules.create({
#    "destination": "https://fastapi-test-inky.vercel.app/tempo",
#    "cron": "* 1 * * *",
#    "method": "GET"
#})

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)   


@app.get('/epa')
async def read_epa():
    return JSONResponse(content=jsonable_encoder(get_epa(year=2022)))

@app.get('/tempo')
async def return_time():
    print((datetime.datetime.now()))
    return str(datetime.datetime.now())

@app.post('/requests')
async def return_time():
    return load.requests_opt(2022)

@app.post('/fsspec')
async def return_time():
    return load.fsspec_opt(2022)
