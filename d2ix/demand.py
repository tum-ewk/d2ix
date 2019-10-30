import logging
from typing import Dict

import pandas as pd
from pandas.io.json import json_normalize

from d2ix import ModelPar
from d2ix.util import split_columns

logger = logging.getLogger(__name__)


def add_demand(data: Dict, model_par: ModelPar, loc: str) -> ModelPar:
    if data['demand'].get(loc):
        df = model_par['demand']
        df_dem = _create_df(data, loc, 'demand')
        model_par['demand'] = pd.concat([df, df_dem], sort=False)
        logger.debug(f'Create demand in location \'{loc}\'')
    return model_par


def _create_df(data: Dict, dem_loc: str, par: str) -> pd.DataFrame:
    # create DataFrame for location and parameter
    d = pd.DataFrame.from_dict(data[par][dem_loc])
    d.index = d.index.astype(str)
    # extract year info
    df_par = json_normalize(d.year)
    df_par.index = d.index
    df_par.columns = split_columns(df_par.columns)
    df_par = pd.concat([pd.DataFrame((df_par.loc[:][i])) for i in df_par.columns.levels[0]])
    df_par.index.rename('year', inplace=True)
    df_par.reset_index(inplace=True)
    return df_par
