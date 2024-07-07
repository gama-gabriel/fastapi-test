import time
import polars as pl

def get_epa(down=[1,2,3,4]):
    start = time.perf_counter()

    d = (
    pl.scan_parquet('api/desc.parquet')
    .select(pl.col('team_abbr').alias('Team'), pl.col('team_name'), pl.col('team_color'), pl.col('team_logo_espn'))
    )
    desc = d.collect()

    q = (
    pl.scan_parquet('api/raw.parquet')
    .filter(((pl.col('pass') == 1) & (pl.col('week') <= 17)) )
    .group_by(pl.col('posteam').alias("Team"))
    .agg(pl.col('epa').mean().alias("Offensive EPA"))
    .sort('Offensive EPA', descending = True)
    )
    df = q.collect()

    epa = df.join(desc, on="Team", how="inner")


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
