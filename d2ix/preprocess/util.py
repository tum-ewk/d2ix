from typing import List

import pandas as pd


def get_year_vector(year_vector: List[int], first_model_year: int, life_time: int, duration_period_sum: pd.DataFrame,
                    first_tech_year: int, last_tech_year: int) -> List[int]:
    dps = duration_period_sum
    first_vtg_year = dps[dps[first_model_year] < life_time][first_model_year]
    first_vtg_year = first_vtg_year.index[0]
    return [y for y in year_vector if (y >= first_vtg_year) and (y <= last_tech_year) and (y >= first_tech_year)]
