from time import perf_counter
import polars as pl
from requests import get
import os.path
from typing import Union, List
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException

#the namespace cannot be type checked by the code editor
@pl.api.register_lazyframe_namespace("nfl")
class Play_by_play:
    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf

    def filter_weeks(self, weeks: Union[str, List[int]], include_playoffs: bool = False):
        def validade_weeks(weeks: Union[str, List[int]]):
            if isinstance(weeks, List):
                if (len(weeks)) != 2:
                    raise HTTPException(status_code=400, detail="Week parameter, when list, should be composed of the first and the last weeks of a range.")
                if weeks[0] < 1:
                    raise HTTPException(status_code=400, detail="Week out of minimum range.")
                if weeks[1] > 22:
                    raise HTTPException(status_code=400, detail="Week out of maximum range.")


        validade_weeks(weeks)
        if weeks == "all":
            if not include_playoffs:
                return self._lf.filter((pl.col('season_type') == 'REG'))
            return self._lf
        else:
            week_start, week_end = weeks
            if include_playoffs:
                return self._lf.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)) | (pl.col('season_type') == 'POST'))
            else:
                return self._lf.filter(((pl.col('week') >= week_start) & (pl.col('week') <= week_end)))
    

    def filter_quarters(self, quarters: Union[str, List[int]]):
        def validate_quarters(quarters: Union[str, List[int]]):
            if isinstance(quarters, List):
                if len(quarters) > 5:
                    raise HTTPException(status_code = 400, detail="Only five quarters are available for filtering (including overtime).")
                if max(quarters) > 5:
                    raise HTTPException(status_code = 400, detail="Quarters filter cannot be bigger than 5.")
                if min(quarters) < 1:
                    raise HTTPException(status_code = 400, detail="Quarters filter cannot be smaller than 1.")


        if quarters != "all":
            validate_quarters(quarters)
            return self._lf.filter(pl.col('qtr').is_in(quarters))
        return self._lf

        
    def filter_downs(self, downs: Union[str, List[int]]):
        def validate_downs(downs: Union[str, List[int]]):
            if isinstance(downs, List):
                if len(downs) > 4:
                    raise HTTPException(status_code = 400, detail="Only four downs are available for filtering.")
                if max(downs) > 4:
                    raise HTTPException(status_code = 400, detail="Downs filter cannot be bigger than 4.")
                if min(downs) < 1:
                    raise HTTPException(status_code = 400, detail="Downs filter cannot be smaller than 1.")


        if downs != "all":
            validate_downs(downs)
            return self._lf.filter(pl.col('down').is_in(downs))
        return self._lf


    def filter_wp(self, wp_offset: float = 0):
        if wp_offset >= 0 and wp_offset <= 0.25:
            return self._lf.filter((pl.col('wp') >= wp_offset) & (pl.col('wp') <= 1 - wp_offset))
        raise HTTPException(status_code = 400, detail="Win percentage offset values should be between 0 and 0.25.")


    def filter_vegas_wp(self, wp_offset: float = 0):
        if wp_offset >= 0 and wp_offset <= 0.25:
            return self._lf.filter((pl.col('vegas_wp') >= wp_offset) & (pl.col('vegas_wp') <= 1 - wp_offset))
        raise HTTPException(status_code = 400, detail="Win percentage offset values should be between 0 and 0.25.")


def write(url: str, path: str):
    response = get(url)
    response.raise_for_status()

    path = os.path.join('/tmp', path)

    with open(path, 'wb') as file:
        file.write(response.content)


def get_epa(
     year: int, 
     weeks: Union[str, List[int]] = "all", 
     quarters: Union[str, List[int]] = "all", 
     downs: Union[str, List[int]]= "all", 
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
    print(weeks, include_playoffs, quarters, downs, wp_offset, vegas_wp_offset)
    off_epa = (
        pbp
        .filter(((pl.col('pass') == 1) | (pl.col('rush') == 1)))
        .nfl.filter_weeks(weeks, include_playoffs) #type: ignore (because the type is created at runtime)
        .nfl.filter_quarters(quarters)
        .nfl.filter_downs(downs)
        .nfl.filter_wp(wp_offset)
        .nfl.filter_vegas_wp(vegas_wp_offset)
        .group_by(pl.col('posteam').alias('Team'))
        .agg(pl.col('epa').mean().alias('Offensive EPA'))
    )

    def_epa = (
        pbp
        .filter(((pl.col('pass') == 1) | (pl.col('rush') == 1)))
        .nfl.filter_weeks(weeks, include_playoffs) #type: ignore (because the type is created at runtime)
        .nfl.filter_quarters(quarters)
        .nfl.filter_downs(downs)
        .nfl.filter_wp(wp_offset)
        .nfl.filter_vegas_wp(vegas_wp_offset)
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
    return(JSONResponse(content=jsonable_encoder(epa_json)))


#def get_side_epa(side: str,
