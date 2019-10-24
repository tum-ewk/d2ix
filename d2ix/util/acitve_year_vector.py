import itertools
from typing import List, NamedTuple

import pandas as pd


class YearVector(NamedTuple):
    vintage_years: List[int]
    act_years: List[int]


def get_act_year_vector(duration_period_sum: pd.DataFrame, vtg_year: int, life_time: int, first_model_year: int,
                        last_tech_year: int,
                        years_no_hist_cap: List[int]) -> YearVector:
    dps = duration_period_sum.T
    act_years = list(dps[dps[vtg_year] < life_time][vtg_year].index)
    act_years = [i for i in act_years if i <= last_tech_year]
    # remove undefined historical years
    act_years = sorted(list(set(act_years) - set(years_no_hist_cap)))
    act_years = [y for y in act_years if y >= vtg_year]
    year_pairs = [(y_v, y_a) for y_v, y_a in itertools.product(act_years, act_years) if
                  (y_v <= y_a) and (y_a >= first_model_year)]

    vintage_years, act_years = zip(*year_pairs)
    years_vector = YearVector(list(vintage_years), list(act_years))
    return years_vector


def get_years_no_hist_cap(loc: str, tech: str, historical_years: List[int], tech_hist: pd.DataFrame) -> List[int]:
    if not tech_hist.empty:
        _hist_cap_loc_tech = tech_hist[
            (tech_hist['node_loc'] == loc)
            & (tech_hist['technology'] == tech)
            & (tech_hist['value'] > 0)]
        if not _hist_cap_loc_tech.empty:
            tech_hist_years = _hist_cap_loc_tech['year_vtg'].tolist()
        else:
            tech_hist_years = []
    else:
        tech_hist_years = []

    if historical_years:
        years_no_hist_cap = list(set(historical_years) - set(tech_hist_years))
    else:
        years_no_hist_cap = []

    return years_no_hist_cap
