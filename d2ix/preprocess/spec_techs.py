import copy
import logging
from typing import Tuple, List, Dict

import pandas as pd

from d2ix import RawData
from d2ix.preprocess.base_techs import get_base_techs
from d2ix.preprocess.util import get_year_vector
from d2ix.util import df_to_nested_dict

logger = logging.getLogger(__name__)


def process_spec_techs(raw_data: RawData, model_data: dict, year_vector: List[int], first_model_year: int,
                       par_list: List[str], duration_period_sum: pd.DataFrame) -> dict:
    df = raw_data['base_input']['spec_techs'].copy()
    df = df.set_index('technology', drop=True)
    data = df_to_nested_dict(df)

    # load base techs default settings
    _base_tech = model_data['technology']
    default = raw_data['base_tech']['default']

    # load units
    units = model_data['units']

    technology = {}
    for k, v in data.items():
        if 'base_techs' in v.keys():
            tech = _base_tech[v['base_techs']]
        else:
            tech = get_base_techs(default, v['commodity_out1'], year_vector, first_model_year, duration_period_sum)
        tech_dict = copy.deepcopy(dict(tech))
        tech_dict = _parse_spec_techs(tech_dict, v, units, year_vector, first_model_year, par_list, duration_period_sum)

        technology[k] = tech_dict

    logger.debug('Created helper model_data structure: \'spec techs\'')
    return technology


def _parse_spec_techs(tech: dict, options: dict, unit: dict, year_vector: List[int], first_model_year: int,
                      par_list: List[str], duration_period_sum: pd.DataFrame) -> dict:
    if options.get('base_techs'):
        del options['base_techs']
    first_tech_year = options.pop('first_year', True)
    last_tech_year = options.pop('last_year', True)

    # efficiency calculation: single input multiple output
    tech, options = __in_out_efficiency(tech, options)

    # distinguish different emissions
    emission_type = [i for i in options.keys() if 'emission_factor_' in i]
    emission = {i.replace('emission_factor_', ''): options[i] for i in emission_type}

    for _em in emission_type:
        options.pop(_em, None)

    if emission:
        for k, v in emission.items():
            if tech.get('emission'):
                _tmp = list([tech.get('emission')])
                _tmp.append(k)
                tech['emission'] = _tmp

                _tmp = options.get('emission_factor', [])
                options['emission_factor'] = [_tmp, v]
            else:
                tech['emission'] = k
                options['emission_factor'] = v

    # create tech years
    year_vtg = get_year_vector(year_vector, first_model_year, options['technical_lifetime'], duration_period_sum,
                               first_tech_year, last_tech_year)

    # fill year_vtg historical data
    exist_year = [int(i) for i in tech['year_vtg'].keys()]
    year_fill = set(year_vtg).difference(set(exist_year))
    if len(year_fill):
        for i in year_fill:
            tech['year_vtg'][i] = tech['year_vtg'][exist_year[0]]

    # remove years from base technology if is not defined in this period
    year_remove = set(exist_year).difference(set(year_vtg))
    if len(year_remove):
        for i in year_remove:
            tech['year_vtg'].pop(i)

    # post process options
    keys = [i for i in options.keys() if 'postprocess_' in i]

    if keys:
        tech['postprocess'] = {i.replace('postprocess_', ''): options[i] for i in keys}
        for i in keys:
            del options[i]

    # remove helper parameters
    additional_pars = {k: v for k, v in options.items() if k not in par_list}
    if additional_pars:
        tech['additional_pars'] = {k: v for k, v in additional_pars.items() if v != 0}

    options = {k: v for k, v in options.items() if k in par_list}

    _tmp_tech: Dict[str, dict] = {}
    for k, v in options.items():
        _tmp_tech[k] = {'unit': unit[k]['unit'], 'value': v}

    tech['year_vtg'] = dict.fromkeys(year_vtg, _tmp_tech)

    return tech


def __in_out_efficiency(tech: dict, options: dict) -> Tuple[dict, dict]:
    efficiency1 = options.pop('efficiency_1', None)
    out_lvl1 = options.pop('level_out1', None)
    out_com1 = options.pop('commodity_out1', None)
    in_lvl1 = options.pop('level_in1', None)
    in_com1 = options.pop('commodity_in1', None)

    if out_lvl1:
        tech['output']['level'] = out_lvl1
    if out_com1:
        tech['output']['commodity'] = out_com1
    if in_lvl1:
        tech['input'] = {'level': in_lvl1}
    if in_com1:
        tech['input']['commodity'] = in_com1

    if efficiency1:
        options['input'] = 1.0 / efficiency1
        options['output'] = 1.0

    if not in_com1:
        del options['input']

    efficiency2 = options.pop('efficiency_2', None)
    out_lvl2 = options.pop('level_out2', None)
    out_com2 = options.pop('commodity_out2', None)

    if out_lvl2:
        if tech['output'].get('level'):
            _tmp = list([tech['output'].get('level')])
            _tmp.append(out_lvl2)
            tech['output']['level'] = _tmp
        else:
            tech['output']['level'] = out_lvl2
    if out_com2:
        if tech['output'].get('commodity'):
            _tmp = list([tech['output'].get('commodity')])
            _tmp.append(out_com2)
            tech['output']['commodity'] = _tmp
        else:
            tech['output']['commodity'] = out_com2

    if efficiency2:
        _out2 = options['input'] * efficiency2
        options['output'] = [options['output'], _out2]

    return tech, options
