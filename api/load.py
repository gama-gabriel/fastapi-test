import polars as pl
from fsspec import open as fs_open
from requests import get
from time import perf_counter
from datetime import datetime
from os import path

def requests_opt(year: int):
    start = perf_counter()

    part_years = [i for i in range(2016, 2024)]

    url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet'

    raw = get(url)
    raw.raise_for_status()
    raw = pl.read_parquet(raw.content)
    raw = raw.cast({'play_id': pl.Int32})

    url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.parquet'

    if year in part_years:
        part = get(url)
        part.raise_for_status()
        part = pl.read_parquet(part.content)
        pbp = raw.join(part, on=['old_game_id', 'play_id'], how='left')
    else:
        pbp = raw

    if not (path.exists(f'api/pbp_{year}.parquet')):
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        end = perf_counter
        print(f'{year} data created at {datetime.now()}')
        return (end - start)

    prev_pbp = pl.read_parquet(f'api/pbp_{year}.parquet')

    if pbp.shape != prev_pbp.shape:
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        end = perf_counter()
        print(f'{year} data updated at {datetime.now()}')
        return (end - start)

    if not (pbp.equals(prev_pbp)):
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        print(f'{year} data updated at {datetime.now()}')
    else:
        print(f'{year} data not updated')

    end = perf_counter()
    return (end - start)

def fsspec_opt(year: int):
    start = perf_counter()

    part_years = [i for i in range(2016, 2024)]

    url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet'
    with fs_open(url) as file:
        raw = pl.read_parquet(file)
        raw = raw.cast({'play_id': pl.Int32})

    if year in part_years:  
        url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.parquet'
        with fs_open(url) as file:
            part = pl.read_parquet(file)

        pbp = raw.join(part, on=['old_game_id', 'play_id'], how='left')
    else:
        pbp = raw

    if not (path.exists(f'api/pbp_{year}.parquet')):
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        end = perf_counter
        print(f'{year} data created at {datetime.now()}')
        return (end - start)

    prev_pbp = pl.read_parquet(f'api/pbp_{year}.parquet')

    if pbp.shape != prev_pbp.shape:
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        end = perf_counter()
        print(f'{year} data updated at {datetime.now()}')
        return (end - start)

    if not (pbp.equals(prev_pbp)):
        pbp.write_parquet(f'api/pbp_{year}.parquet')
        print(f'{year} data updated at {datetime.now()}')
    else:
        print(f'{year} data not updated')
        
    end = perf_counter()
    return (end - start)
requests_opt(2022)
