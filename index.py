from db import get_epa
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import datetime

def check_time():
    print(datetime.datetime.now())

@asynccontextmanager
async def lifespan(app:FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_time,"cron", second = '*/5')
    scheduler.start()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)   

@app.get('/epa')
async def read_root():
    return JSONResponse(content=jsonable_encoder(get_epa()))


