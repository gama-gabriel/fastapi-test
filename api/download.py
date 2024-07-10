import fsspec
from requests import get
import polars as pl
from time import perf_counter

def get_opt():
    start = perf_counter()

    response = get('https://utfs.io/f/915b30e3-15b9-4e2d-b0b6-ced13c566bf3-x1kd0f.parquet')

    response.raise_for_status()

    ws = perf_counter()
    with open('/tmp/pbp_2022.parquet', 'wb') as f:
        f.write(response.content)
    we = perf_counter()
    print(f'write time: {we - ws}')

    qs = perf_counter()
    q = (
        pl.scan_parquet('/tmp/pbp_2022.parquet')
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

print(get_opt())
