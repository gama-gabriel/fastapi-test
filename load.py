import polars as pl
import pandas as pd
import nfl_data_py as nfl
import fsspec
import requests
import time

def requests_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = time.perf_counter()

    pbp = requests.get(url)
    pbp.raise_for_status()
    pbp = pl.read_parquet(pbp.content)
    
    end = time.perf_counter()
    return (end - start)

def fsspec_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = time.perf_counter()

    with fsspec.open(url) as file:
        pbp = pl.read_parquet(file)
    
    end = time.perf_counter()
    return (end - start)
    
def pandas_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = time.perf_counter()

    pbp = pandas.read_parquet(url)
    
    end = time.perf_counter()
    return (end - start)

def nfl_opt():

    start = time.perf_counter()

    pbp = nfl.import_pbp_data([2023], downcast=False)
    
    end = time.perf_counter()
    return (end - start)

