import logging
from d2ix.preprocess.util import get_year_vector

logger = logging.getLogger(__name__)


def process_base_techs(raw_data, year_vector, first_model_year,
                       duration_period_sum):
    dem = raw_data['base_input']['demand'].copy()
    node = dem['node'].unique().tolist()

    commodity = {}
    for n in node:
        commodity[n] = dem[dem['node'] == n]['commodity'].unique().tolist()
    default = _change_unit_default_techs(raw_data)

    base_techs = {'technology': {}}
    for n, com in sorted(commodity.items()):
        for c in sorted(com):
            tmp = get_base_techs(default, c, year_vector, first_model_year,
                                 duration_period_sum)

            base_techs['technology']['slack_' + c] = tmp

    logger.debug('Created helper data structure: \'base techs\'')
    return base_techs['technology']


def get_base_techs(default, com, year_vector, first_model_year,
                   duration_period_sum):
    life_time = default['year_vtg']['technical_lifetime']['value']
    first_tech_year = first_model_year
    last_tech_year = year_vector[-1]
    years = get_year_vector(year_vector, first_model_year, life_time,
                            duration_period_sum, first_tech_year,
                            last_tech_year)

    year_info = ['last_year', 'first_year']
    options = set(default.keys()).difference(set(year_info))

    tech = {}
    for k in sorted(options):
        if k == 'output':
            tech['output'] = default[k]
            tech['output']['commodity'] = com
        elif k == 'year_vtg':
            tech['year_vtg'] = dict.fromkeys(years, default['year_vtg'])
        else:
            tech[k] = default[k]

    return tech


def _change_unit_default_techs(_data):
    _units = _data['base_input']['unit'].copy()
    _units = _units.set_index('parameter')
    _default = _data['base_tech']['default']
    for k in _default['year_vtg'].keys():
        _default['year_vtg'][k]['unit'] = _units.loc[k].unit
    return _default
