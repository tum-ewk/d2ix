import logging

from d2ix.util import df_to_nested_dict

logger = logging.getLogger(__name__)


def process_spatial_locations(_data):
    df = _data['base_input']['locations'].copy()
    df['tech'] = 'technology'
    df['override'] = 'override'

    df = df.set_index(['location', 'tech', 'technology', 'override'], drop=True)
    data = df_to_nested_dict(df)
    data = {'locations': data}

    logger.debug('Created helper data structure: \'locations\'')
    return data['locations']
