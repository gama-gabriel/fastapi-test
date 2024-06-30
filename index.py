from db import get_epa
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from fastapi_utilities import repeat_at
import datetime

def check_time():
    print(datetime.datetime.now())

# @asynccontextmanager
# async def lifespan(app:FastAPI):
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(check_time,"cron", second = '*/5')
#     scheduler.start()
#     yield

# app = FastAPI(lifespan=lifespan)
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)   

times = []

@app.on_event('startup')
@repeat_at(cron='* * * * *')
async def hi():
    with open('time.txt', 'a') as file:
        tempo = str(datetime.datetime.now())
        file.write(tempo)
        print(tempo)

@app.get('/epa')
async def read_root():
    return JSONResponse(content=jsonable_encoder(get_epa()))


@app.get('/tempos')
async def read_tempos():
    tempo = str(datetime.datetime.now())
    times.append(tempo)
    print(tempo)
    return times

