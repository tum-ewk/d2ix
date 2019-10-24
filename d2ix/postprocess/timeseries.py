import logging
import pandas as pd
from d2ix.postprocess.utils import group_data

logger = logging.getLogger(__name__)


def create_timeseries_df(results):
    logger.info('Create timeseries')
    results.check_out(timeseries_only=True)
    for var in ['ACT', 'CAP', 'CAP_NEW', 'EMISS']:
        df = group_data(var, results)
        if var != 'EMISS':
            df['variable'] = ([f'{df.loc[i, "technology"]}|{df.loc[i, "variable"]}' for i in df.index])
        else:
            df['variable'] = [f'{df.loc[i, "emission"]}|{df.loc[i, "variable"]}' for i in df.index]
        df['node'] = 'World'  # TODO: wenn #6 gelöst, dann implementieren
        df = df.rename(columns={'node': 'region'})
        ts = pd.pivot_table(df, values='lvl', index=['region', 'variable', 'unit'], columns=['year']).reset_index(
            drop=False)
        results.add_timeseries(ts)
    results.commit('timeseries added')
    return results
