from requests import get
import polars as pl
from time import perf_counter

def write(url, path):
    response = get(url)
    response.raise_for_status()

    with open(path, 'wb') as file:
        file.write(response.content)

def get_opt():
    start = perf_counter()

    url1 = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2022.parquet'
    url2 = 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_2022.parquet'
    url3 = 'https://github.com/nflverse/nflverse-data/releases/download/ftn_charting/ftn_charting_2022.parquet'

    ws = perf_counter()
    write(url1, '/tmp/pbp_2022.parquet')
    write(url2, '/tmp/part_2022.parquet')
    write(url3, '/tmp/ftn_2022.parquet')
    we = perf_counter()
    print(f'write time: {we - ws}')
    
    pbp = pl.scan_parquet('/tmp/pbp_2022.parquet')
    part = pl.scan_parquet('/tmp/part_2022.parquet')
    ftn = pl.scan_parquet('/tmp/ftn_2022.parquet')
    ftn = ftn.with_columns(pl.col('nflverse_play_id').alias('play_id'))

    qs = perf_counter()
    q = (
        pbp
        .cast({'play_id': pl.Int32})
        .with_columns(pl.col('game_id').alias('nflverse_game_id'))
        .join(part, on=['nflverse_game_id', 'play_id'], how='inner')
        .join(ftn, on=['nflverse_game_id', 'play_id'], how='inner')
        .filter(((pl.col('pass') == 1) & (pl.col('week') <= 18)) )
        .group_by(pl.col('posteam').alias("Team"))
        .agg(pl.col('epa').mean().alias("Offensive EPA"))
        .sort('Offensive EPA', descending = True)
    )    

    df = q.collect()
    qe = perf_counter()
    print(f'query time: {qe - qs}')
    print(df.shape)

    end = perf_counter()
    return (end - start)
