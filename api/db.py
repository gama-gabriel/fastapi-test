from time import perf_counter
import polars as pl
from requests import get
import os.path
from typing import Union, List
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException

class Play_by_play(pl.LazyFrame):
    def __init__(self, df: pl.LazyFrame):
        super.__init__(df._ldf)

    def filter_weeks(self, weeks: Union[str, List[int]], include_playoffs = False):
        def validade_weeks(weeks: Union[str, List[int]]):
            if isinstance(weeks, str):
                raise HTTPException(status_code=400, detail="""Week parameter, when string, should be set to "all".""")
            if isinstance(weeks, List):
                if (len(weeks)) != 2:
                    raise HTTPException(status_code=400, detail="Week parameter, when list, should be composed of the first and the last weeks of a range.")

        if weeks == "all":
            if not include_playoffs:
                return self.filter((pl.col('season_type') == 'REG'))
        else:
            validade_weeks(weeks)
            week_start, week_end = weeks
            if include_playoffs:
                return self.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)) | (pl.col('season_type') == 'POST'))
            else:
                return self.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)))
    
    def filter_quarter(self, quarters: Union[str, List[int]]):
        


def write(url: str, path: str):
    response = get(url)
    response.raise_for_status()

    path = os.path.join('/tmp', path)

    with open(path, 'wb') as file:
        file.write(response.content)

def get_epa(
     year: int, 
     down: Union[str, List[int]]= "all", 
     quarter: Union[str, List[int]] = "all", 
     weeks: Union[str, List[int]] = "all", 
     include_playoffs: bool =False, 
     wp_offset: float = 0, 
     vegas_wp_offset: float = 0
):

    if year == 2024:
        write_start = perf_counter()

        pbp_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet'
        write(pbp_url, f'pbp_{year}.parquet')
        path = os.path.join('/tmp', f'pbp_{year}.parquet')

        write_end = perf_counter()
        print(f'write time: {write_end - write_start}')
    else:
        path = f'api/data/pbp_{year}.parquet'


    start = perf_counter()

    desc = (
        pl.scan_parquet('api/desc.parquet')
        .select(pl.col('team_abbr').alias('Team'), pl.col('team_name'), pl.col('team_color'), pl.col('team_logo_espn'))
    )

    pbp = pl.scan_parquet(path)

    #filtering weeks
    if weeks == "all":
        if not include_playoffs:
            pbp = pbp.filter((pl.col('season_type') == 'REG'))
    else:
        week_start, week_end = weeks
        if include_playoffs:
            pbp = pbp.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)) | (pl.col('season_type') == 'POST'))
        else:
            pbp = pbp.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)))

    #filtering downs
    if down != "all":
        pbp = pbp.filter(pl.col('down').is_in(down))

    #filtering quarters
    if quarter != "all":
        pbp = pbp.filter(pl.col('qtr').is_in(quarter))

    #filtering win percentage
    if wp_offset > 0:
        pbp = pbp.filter((pl.col('wp') >= wp_offset) & (pl.col('wp') <= 1 - wp_offset))
    if vegas_wp_offset > 0:
        pbp = pbp.filter((pl.col('vegas_wp') >= vegas_wp_offset) & (pl.col('vegas_wp') <= 1 - vegas_wp_offset))

    off_epa = (
        pbp
        .filter(((pl.col('pass') == 1) | (pl.col('rush') == 1)))
        .group_by(pl.col('posteam').alias('Team'))
        .agg(pl.col('epa').mean().alias('Offensive EPA'))
    )

    def_epa = (
        pbp
        .filter(((pl.col('pass') == 1) | (pl.col('rush') == 1)))
        .group_by(pl.col('defteam').alias('Team'))
        .agg(pl.col('epa').mean().alias('Defensive EPA'))
    )

    both = off_epa.join(def_epa, on='Team', how='inner')

    df = both.join(desc, on='Team', how='inner')

    epa = df.collect()

    offensive_epa = epa['Offensive EPA'].to_list()
    defensive_epa = epa['Defensive EPA'].to_list()
    teams = epa['Team'].to_list()
    logos = epa['team_logo_espn'].to_list()
    colors = epa['team_color'].to_list()

    data_list = [{'data': {'x': x, 'y': y}, 'name': name, 'logo': logo, 'color': color} 
		 for x, y, name, logo, color in zip(offensive_epa, defensive_epa, teams, logos, colors)]

    epa_json = str(data_list)

    end = perf_counter()
    print(f'query time: {end - start}')
    return JSONResponse(content=jsonable_encoder(epa_json))

#def get_side_epa(side: str,
