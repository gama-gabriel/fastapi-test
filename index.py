from db import get_epa
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import datetime
from dotenv import load_dotenv
import os

def check_time():
    print(datetime.datetime.now())

load_dotenv()

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
    print(os.environ.get("QSTASH_URL"))
    return JSONResponse(content=jsonable_encoder(get_epa()))

