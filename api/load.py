from polars import read_parquet as pl_read_parquet
from pandas import read_parquet
from nfl_data_py import import_pbp_data
from fsspec import open as fs_open
from requests import get
from time import perf_counter

def requests_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = perf_counter()

    pbp = get(url)
    pbp.raise_for_status()
    pbp = pl_read_parquet(pbp.content)
    
    end = perf_counter()
    return (end - start)

def fsspec_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = perf_counter()

    with fs_open(url) as file:
        pbp = pl.read_parquet(file)
    
    end = perf_counter()
    return (end - start)
    
def pandas_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = perf_counter()

    pbp = read_parquet(url)
    
    end = perf_counter()
    return (end - start)

def nfl_opt():

    start = perf_counter()

    pbp = import_pbp_data([2023], downcast=False)
    
    end = perf_counter()
    return (end - start)

