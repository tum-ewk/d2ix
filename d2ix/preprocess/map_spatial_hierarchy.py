import logging

logger = logging.getLogger(__name__)


def process_map_spatial_hierarchy(_data):
    df = _data['base_input']['map_spatial_hierarchy'].copy()
    map_spatial_hierarchy = []
    for index, row in df.iterrows():
        map_spatial_hierarchy.append(f'{row["lvl_spatial"]}.{row["node"]}.{row["node_parent"]}')

    data = {'map_spatial_hierarchy': map_spatial_hierarchy}

    logger.debug('Created helper data structure: \'map_spatial_hierarchy\'')
    return data['map_spatial_hierarchy']
