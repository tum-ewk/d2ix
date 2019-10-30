import logging
from typing import Dict, List

import message_ix
import pandas as pd

from d2ix import ModelPar, Data

logger = logging.getLogger(__name__)

SYN_DICT = {'node': ['node', 'node_loc', 'node_origin', 'node_dest'], 'year': ['year', 'year_act', 'year_vtg'],
            'emission': ['emission'], 'commodity': ['commodity'], 'technology': ['technology'], 'level': ['level'],
            'mode': ['mode'], 'time': ['time'], 'time_origin': ['time_origin'], 'time_dest': ['time_dest']}


def add_sets(data: Data, model_par: ModelPar, first_model_year: int) -> ModelPar:
    # lvl_spatial
    model_par['lvl_spatial'] = data['lvl_spatial'][0]

    # map_spatial_hierarchy
    hierarchical_nodes = [i.split('.') for i in data['map_spatial_hierarchy']]
    model_par['map_spatial_hierarchy'] = hierarchical_nodes

    # nodes
    nodes = [item for sublist in hierarchical_nodes for item in sublist[1:]]
    model_par['node'] = list(set(model_par['node'] + nodes))

    # set first model year
    if 'type_year' in model_par.keys():
        type_year = ['firstmodelyear'] + model_par['type_year']
    else:
        type_year = ['firstmodelyear']
    model_par['type_year'] = type_year

    model_par['cat_year'] = ['firstmodelyear', str(first_model_year)]

    # add level renewable - if used
    if data['level_renewable'] in model_par['level']:
        model_par['level_renewable'] = [data['level_renewable']]
    # add level resource - if used
    if data['level_resource'] in model_par['level']:
        model_par['level_resource'] = [data['level_resource']]

    return model_par


def extract_sets(scenario: message_ix.Scenario, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, list]:
    _sets: Dict[str, list] = {}
    for par, df in data_dict.items():
        par_set = _extract_sets_df(scenario, data=df)
        for k in par_set.keys():
            _sets.setdefault(k, []).extend(par_set[k])

    _sets['year'] = [int(y) for y in _sets['year']]
    _sets.update({k: sorted(set(_sets[k])) for k in _sets.keys() if k == 'year'})
    return _sets


def _extract_sets_df(scenario: message_ix.Scenario, data: pd.DataFrame) -> Dict[str, list]:
    logger.debug(f'Get sets for \'{data.columns}\'')

    scenario_sets = set(scenario.set_list())
    scenario_dict = {i: [i] for i in scenario_sets}
    set_synonyms = {**scenario_dict, **SYN_DICT}

    data_sets = [i for i, v in set_synonyms.items() for k in v if k in set(data)]
    sets = scenario_sets.intersection(data_sets)
    sets_dict = {i: [k for k in set_synonyms[i] for s in set(data) if s == k][0] if i in set_synonyms.keys() else i for
                 i in sets}

    _sets: Dict[str, list] = {i: sorted(data[k].dropna().drop_duplicates().tolist()) for i, k in sets_dict.items()}

    return _sets


def set_order() -> List[str]:
    return ['year', 'node', 'technology', 'relation', 'emission', 'time', 'mode', 'grade', 'level', 'commodity',
            'rating', 'lvl_spatial', 'lvl_temporal', 'type_node', 'type_tec', 'type_year', 'type_emission',
            'type_relation', 'level_resource', 'level_renewable', 'level_stocks', 'cat_node', 'cat_tec', 'cat_year',
            'cat_emission', 'cat_relation', 'map_spatial_hierarchy', 'map_node', 'map_temporal_hierarchy', 'map_time',
            'land_scenario', 'land_type', 'type_tec_land']


def set_frame_list(scenario: message_ix.Scenario, set_dict: dict) -> Dict[str, list]:
    _sets = {(k if isinstance(scenario.set(k), pd.Series) else k): (
        v[0].tolist() if isinstance(scenario.set(k), pd.Series) else v) for k, v in set_dict.items()}
    return _sets
