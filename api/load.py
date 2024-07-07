import polars as pl
from fsspec import open as fs_open
from requests import get
from time import perf_counter

def requests_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = perf_counter()

    raw = get(url)
    raw.raise_for_status()
    raw = pl.read_parquet(raw.content)
    raw = raw.cast({'play_id': pl.Int32})

    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_2023.parquet'

    part = get(url)
    part.raise_for_status()
    part = pl.read_parquet(part.content)

    pbp = raw.join(part, on=['old_game_id', 'play_id'], how='left')
    
    print(pbp.shape)
    end = perf_counter()
    return (end - start)

def fsspec_opt():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet'

    start = perf_counter()

    with fs_open(url) as file:
        raw = pl.read_parquet(file)
        raw = raw.cast({'play_id': pl.Int32})

    url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_2023.parquet'

    with fs_open(url) as file:
        part = pl.read_parquet(file)

    pbp = raw.join(part, on=['old_game_id', 'play_id'], how='left')
    
    print(pbp.shape)
    end = perf_counter()
    return (end - start)
