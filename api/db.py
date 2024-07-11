import time
import polars as pl
from requests import get
import os.path

def write(url: str, path: str):
    response = get(url)
    response.raise_for_status()

    path = os.path.join('/tmp', path)

    with open(path, 'wb') as file:
        file.write(response.content)

def get_epa(year: int, down=[1,2,3,4]):
    start = time.perf_counter()

    pbp_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet'
    write(pbp_url, f'pbp_{year}.parquet')

    desc = (
        pl.scan_parquet('api/desc.parquet')
        .select(pl.col('team_abbr').alias('Team'), pl.col('team_name'), pl.col('team_color'), pl.col('team_logo_espn'))
    )

    pbp = (
        pl.scan_parquet(f'/tmp/pbp_{year}.parquet')
        .filter(((pl.col('pass') == 1) & (pl.col('week') <= 18)) )
        .group_by(pl.col('posteam').alias("Team"))
        .agg(pl.col('epa').mean().alias("Offensive EPA"))
        .sort('Offensive EPA', descending = True)
    )

    df = pbp.join(desc, on="Team", how="inner")

    epa = df.collect()

    offensive_epa = epa['Offensive EPA'].to_list()
    teams = epa['Team'].to_list()
    logos = epa['team_logo_espn'].to_list()
    colors = epa['team_color'].to_list()

    data_list = [{'data': {'x': x}, 'name': name, 'logo': logo, 'color': color} 
		 for x, name, logo, color in zip(offensive_epa, teams, logos, colors)]

    epa_json = str(data_list)

    end = time.perf_counter()
    print(end - start)
    return epa_json
