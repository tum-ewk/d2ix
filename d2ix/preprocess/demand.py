import logging

from d2ix.util import df_to_nested_dict

logger = logging.getLogger(__name__)


def process_demand(raw_data):
    df = raw_data['base_input']['demand'].copy()

    df = df.assign(y='year')
    df = df.assign(c=df.commodity)
    df = df.assign(d='demand')
    df = df.assign(n=df.node)

    df = df.set_index(['d', 'n', 'y', 'year', 'c'])

    # create nested dict from DataFrame
    data = df_to_nested_dict(df)

    logger.debug('Created helper data structure: \'demand\'')
    return data['demand']
