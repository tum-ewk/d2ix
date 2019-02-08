import logging

from d2ix.util import df_to_nested_dict

logger = logging.getLogger(__name__)


def process_units(_data):
    df = _data['base_input']['unit'].copy()
    df = df.set_index(['parameter'], drop=True)
    data = df_to_nested_dict(df)
    data = {'units': data}

    logger.debug('Created helper data structure: \'unit_techs\'')
    return data['units']
