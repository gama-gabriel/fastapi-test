from fsspec import open as fs_open
from requests import get
import polars as pl
from time import perf_counter

def get_opt():
    start = perf_counter()

    response = get('https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet')
    response.raise_for_status()

    end = perf_counter()
    return (end - start)

def open_opt():
    start = perf_counter()

    content = fs_open('https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet')

    end = perf_counter()
    return (end - start)


