import logging
import sys

import numpy as np

logger = logging.getLogger(__name__)


def check_input_data(raw_data, model_par):
    logger.debug('Checking input data sanity')
    spec_techs = raw_data['base_input']['spec_techs']
    locations = raw_data['base_input']['locations'].copy()

    # test if all technologies tat are defines in spec_tec appear in locations
    not_in_locations = np.setdiff1d(spec_techs.technology, locations.technology)
    if len(not_in_locations) > 0:
        logger.error(f'\n\n The technologies {not_in_locations} are not mentioned in the locations sheet.')
        sys.exit(0)

    # test if all technologies that feed into peak_load level have a rating
    # assigned
    if 'peak_load_factor' in raw_data.get('manual_input', {}).keys():
        rel_and_flex = raw_data['base_input']['rel_and_flex'].copy()
        input_data = model_par['input']
        output_data = model_par['output']

        peak_load_factor = raw_data['manual_input']['peak_load_factor']
        plf = peak_load_factor[['node', 'commodity', 'level']].drop_duplicates()
        plf = plf.reset_index(drop=True)

        for i, row in plf.iterrows():
            _in = input_data[(input_data.node_loc == row['node']) & (input_data.level == row['level']) &
                             (input_data.commodity == row['commodity'])]['technology'].tolist()
            _out = output_data[(output_data.node_loc == row['node']) & (output_data.level == row['level']) &
                               (output_data.commodity == row['commodity'])]['technology'].tolist()
            technologies = list(set(_in + _out))
            rel_felx_techs = \
                rel_and_flex[(rel_and_flex.commodity == row['commodity']) & (rel_and_flex.node == row['node'])][
                    'technology'].tolist()

            not_in_rel_flex = list(set(technologies) - set(rel_felx_techs))
            if not_in_rel_flex:
                logger.error(f'\n\n ERROR: \'peak_load_factor\' for node: \' {row["node"]}\', commodity: '
                             f'\'{row["commodity"]}\' and level: \'{row["level"]}\'.\n\n The technologies '
                             f'{not_in_rel_flex} are not mentioned in the rel_and_flex sheet.')
                sys.exit(0)

    # TODO: test if there are any demands (com & level combinations) that cannot be supplied to
