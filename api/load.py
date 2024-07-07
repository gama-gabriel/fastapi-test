from polars import read_parquet as pl_read_parquet
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
        pbp = pl_read_parquet(file)
    
    end = perf_counter()
    return (end - start)
