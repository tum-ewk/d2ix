import logging
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from message_ix.utils import make_df
from pandas.io.json import json_normalize

from d2ix import Data, ModelPar, RawData
from d2ix.util import split_columns
from d2ix.util.acitve_year_vector import get_act_year_vector, get_years_no_hist_cap

logger = logging.getLogger(__name__)

YearVector = List[int]


def add_technology(data: Data, model_par: ModelPar, first_model_year: int, active_years: YearVector,
                   historical_years: YearVector, duration_period_sum: pd.DataFrame, loc: str, par: str,
                   slack: bool = False) -> ModelPar:
    if slack is True:
        technology, technology_exist = _get_slack_techs(data, loc, par)
    else:
        technology, technology_exist = _get_location_techs(data, loc, par)

    if technology_exist:
        for tech in technology.keys():
            params: Dict = _get_df_tech(technology, tech)
            tech_hist = model_par.get('historical_new_capacity')
            years_no_hist_cap = get_years_no_hist_cap(loc, tech, historical_years, tech_hist)

            tech_parameters = _get_active_model_par(data, params)
            for tech_par in tech_parameters:
                model_par[tech_par] = _add_parameter(model_par, params, tech_par, tech, loc, active_years,
                                                     first_model_year, duration_period_sum, years_no_hist_cap)
    return model_par


def _add_parameter(model_par: ModelPar, params: Dict[str, pd.DataFrame], tech_par: str, tech: str, loc: str,
                   active_years: YearVector, first_model_year: int, duration_period_sum: pd.DataFrame,
                   years_no_hist_cap: YearVector) -> pd.DataFrame:
    df = model_par[tech_par]

    # emission helper
    _emissions_is_list = _check_emissions_is_list(params)

    # single input - double output
    if isinstance(params['in_out'].loc['output', 'level'], list) and tech_par == 'output':
        _level = params['in_out'].loc['output', 'level']
        _com = params['in_out'].loc['output', 'commodity']
        _df_out = params['year_vtg'][
            (params['year_vtg']['par_name'] == 'output') & (params['year_vtg']['par'] == 'value')]
        _df_out_val = pd.DataFrame(_df_out.val.values.tolist(), index=_df_out.index)
        _df_list = []
        for _out in range(len(_com)):
            params['year_vtg'].loc[_df_out_val.index, 'val'] = _df_out_val[_out]
            params['in_out'].loc['output', 'level'] = _level[_out]
            params['in_out'].loc['output', 'commodity'] = _com[_out]

            df_base_dict = _create_parameter_df(params, tech_par, df, first_model_year, active_years,
                                                duration_period_sum, years_no_hist_cap)
            _df_list.append(df_base_dict)
        df_base_dict = pd.concat(_df_list, ignore_index=True)

    # emissions: C02 and CH4
    elif _emissions_is_list and tech_par == 'emission_factor':
        _emission = params['others'].loc['emission'].val
        _df_em = params['year_vtg'][
            (params['year_vtg']['par_name'] == 'emission_factor') & (params['year_vtg']['par'] == 'value')]
        _df_em_val = pd.DataFrame(_df_em.val.values.tolist(), index=_df_em.index)

        _df_list = []
        for _emi in range(len(_emission)):
            params['others'].loc['emission'].val = _emission[_emi]
            params['year_vtg'].loc[_df_em_val.index, 'val'] = _df_em_val[_emi]

            df_base_dict = _create_parameter_df(params, tech_par, df, first_model_year, active_years,
                                                duration_period_sum, years_no_hist_cap)
            _df_list.append(df_base_dict)
        df_base_dict = pd.concat(_df_list, ignore_index=True)

    else:
        df_base_dict = _create_parameter_df(params, tech_par, df, first_model_year, active_years, duration_period_sum,
                                            years_no_hist_cap)

    model = pd.concat([df, df_base_dict])
    logger.debug(f'Create parameter in location \'{loc}\' for \'{tech}\': \'{tech_par}\'')
    return model


def _create_parameter_df(params: Dict[str, pd.DataFrame], model_par: str, df: pd.DataFrame, first_model_year: int,
                         active_years: YearVector, duration_period_sum: pd.DataFrame,
                         years_no_hist_cap: YearVector) -> pd.DataFrame:
    model_par_vtg = params['year_vtg'][
        (params['year_vtg']['par_name'] == model_par) & (~params['year_vtg']['year_vtg'].isin(years_no_hist_cap))]

    model_par_act = params['year_vtg'][
        (params['year_vtg']['par_name'] == model_par) & (params['year_vtg']['year_vtg'].isin(active_years))]

    # fill DataFrame
    base_dict = dict.fromkeys(df.columns)
    # base_dict = df.to_dict()
    keys = base_dict.keys()
    for i in keys:
        if i in params['others'].index:
            # load data not depends on year_vtg or year_act
            base_dict[i] = params['others'].loc[i].val

        elif i in params['in_out'].columns:
            # load output and/or input data
            base_dict[i] = params['in_out'].loc[model_par][i]
        else:
            if ('year_act' in keys and 'year_vtg' in keys) or ('year_vtg' in keys):
                # parameter depends on year_vtg or (year_act and year_vtg)
                if i == 'year_act':
                    # load data for year tuples (year_vtg, year_act)
                    base_dict.update(
                        _create_dict_year_act(params, model_par, base_dict, first_model_year, duration_period_sum,
                                              years_no_hist_cap))
                elif i == 'year_vtg' and 'year_act' not in keys:
                    # load year_vtg data if only depends on year_vtg
                    base_dict[i] = model_par_vtg.year_vtg[model_par_vtg.par == 'value'].tolist()

                elif (i == 'unit' or i == 'value') and (
                        'year_act' not in keys):
                    # load value and unit data
                    base_dict[i] = model_par_vtg.val[model_par_vtg.par == i].tolist()

            if i == 'year_act' and 'year_vtg' not in keys:
                # parameter depends only on year_act
                base_dict['year_act'] = model_par_act.year_vtg[model_par_act.par == 'value'].tolist()
                base_dict['unit'] = model_par_act.val[model_par_act.par == 'unit'].tolist()
                base_dict['value'] = model_par_act.val[model_par_act.par == 'value'].tolist()

    df = pd.DataFrame(base_dict)
    if 'additional_pars' in params['others'].index:
        add_pars = params['others'].loc['additional_pars', 'val']
        if [k for k in add_pars if model_par in k]:
            df = _calc_delta_change(active_years, df, model_par, add_pars)

    return df


def _create_dict_year_act(params: Dict[str, pd.DataFrame], model_par: str, base_dict: Dict, first_model_year: int,
                          duration_period_sum: pd.DataFrame, years_no_hist_cap: List[int]) -> Dict:
    tec_life = params['year_vtg'][params['year_vtg'].par_name == 'technical_lifetime']

    life_val_unit = tec_life[tec_life.par == 'value']
    life_val_unit = life_val_unit.assign(unit=tec_life[tec_life.par == 'unit']['val'].values)

    _year_vtg = []
    _year_act = []
    _value = []
    _unit = []
    _par_data = params['year_vtg'][params['year_vtg'].par_name == model_par].copy()
    _par_data_val = _par_data[_par_data['par'] == 'value'].set_index('year_vtg').to_dict(orient='index')
    _par_data_unit = _par_data[_par_data['par'] == 'unit'].set_index('year_vtg').to_dict(orient='index')

    tech_years = [i for i in life_val_unit['year_vtg'].values if i not in years_no_hist_cap]
    last_tech_year = tech_years[-1]

    for y in tech_years:
        year_life_time = list(life_val_unit[life_val_unit.year_vtg == y]['val'])[0]
        year_vec = get_act_year_vector(duration_period_sum, y, year_life_time, first_model_year, last_tech_year,
                                       years_no_hist_cap)

        _year_vtg.extend(year_vec.vintage_years)
        _year_act.extend(year_vec.act_years)
        _value.extend(len(year_vec.vintage_years) * [_par_data_val[y]['val']])
        _unit.extend(len(year_vec.vintage_years) * [_par_data_unit[y]['val']])

    base_dict['year_vtg'] = _year_vtg
    base_dict['year_act'] = _year_act
    base_dict['value'] = _value
    base_dict['unit'] = _unit
    return base_dict


def _get_active_model_par(data: Data, _param: Dict[str, pd.DataFrame]) -> List[str]:
    tech_model_par = ['technology']
    tech_model_par.extend(_param['in_out'].index.tolist())
    tech_model_par.extend(_param['year_vtg'].par_name[~_param['year_vtg'].par_name.duplicated()].values.tolist())

    tech_model_par = sorted(list(set(tech_model_par)))
    tech_model_par = [model_par for model_par in data['technology_parameter'] if model_par in tech_model_par]

    return tech_model_par


def _calc_delta_change(active_years: YearVector, df: pd.DataFrame, par: str,
                       add_pars: Dict[str, float]) -> pd.DataFrame:
    reference_year = __get_ref_year(df, active_years)
    if f'd_{par}_vtg' in add_pars.keys():
        df = _comp_int(reference_year, df, par, add_pars, 'vtg')
    if f'd_{par}_act' in add_pars.keys():
        df = _comp_int(reference_year, df, par, add_pars, 'act')

    return df


def add_reliability_flexibility_parameter(data: Data, model_par: ModelPar,
                                          raw_data: RawData) -> Dict[str, pd.DataFrame]:
    rel_flex = raw_data['base_input']['rel_and_flex']
    model = {}
    _rating_bin = []
    _reliability_factor = []
    _flexibility_factor = []
    rating_bin_unit = data['units']['rating_bin']['unit']
    reliability_factor_unit = data['units']['reliability_factor']['unit']
    flexibility_factor_unit = data['units']['flexibility_factor']['unit']

    output: pd.DataFrame = model_par['output'].copy()

    for i in rel_flex.index:
        node = rel_flex.at[i, 'node']
        technology = rel_flex.at[i, 'technology']
        mode = data['technology'][technology]['mode']
        commodity = rel_flex.at[i, 'commodity']
        level = rel_flex.at[i, 'level']
        time = rel_flex.at[i, 'time']
        rating = rel_flex.at[i, 'rating']

        logger.debug(f'Create reliability flexibility parameters for {technology} in {node}')

        rating_bin_value = rel_flex.at[i, 'rating_bin']
        reliability_factor_value = rel_flex.at[i, 'reliability_factor']
        flexibility_factor_value = rel_flex.at[i, 'flexibility_factor']

        _output_technology = output[output['technology'] == technology]
        _model_years = list(_output_technology['year_act'].unique())
        _active_years = list(_output_technology['year_act'])
        _vintage_years = list(_output_technology['year_vtg'])

        base_par = pd.DataFrame(
            {'node': node, 'technology': technology, 'year_act': _model_years, 'commodity': commodity, 'level': level,
             'time': time, 'rating': rating})

        _rating_bin.append(make_df(base_par, value=rating_bin_value, unit=rating_bin_unit))

        _reliability_factor.append(make_df(base_par, value=reliability_factor_value, unit=reliability_factor_unit))

        base_flex = pd.DataFrame(
            {'node_loc': node, 'technology': technology, 'year_act': _active_years, 'year_vtg': _vintage_years,
             'commodity': commodity, 'level': level, 'mode': mode, 'time': time, 'rating': rating})

        _flexibility_factor.append(make_df(base_flex, value=flexibility_factor_value, unit=flexibility_factor_unit))

    rating_bin = pd.concat(_rating_bin, sort=False, ignore_index=True)
    rating_bin['year_act'] = rating_bin['year_act'].astype(int)
    model['rating_bin'] = rating_bin

    reliability_factor = pd.concat(_reliability_factor, sort=False, ignore_index=True)
    reliability_factor['year_act'] = reliability_factor['year_act'].astype(int)
    model['reliability_factor'] = reliability_factor

    flexibility_factor = pd.concat(_flexibility_factor, sort=False, ignore_index=True)
    flexibility_factor['year_act'] = flexibility_factor['year_act'].astype(int)
    flexibility_factor['year_vtg'] = flexibility_factor['year_vtg'].astype(int)
    model['flexibility_factor'] = flexibility_factor
    return model


def create_renewable_potential(raw_data: RawData, data: Data, active_years: YearVector) -> Dict[str, pd.DataFrame]:
    renewable_potential = raw_data['base_input']['renewable_potential']
    model = {}

    re_year = renewable_potential.iloc[
        np.repeat(np.arange(len(renewable_potential)), len(active_years))]
    re_year = re_year.assign(
        year=active_years * renewable_potential.shape[0])

    re_potential = re_year.copy()
    re_potential['unit'] = data['units']['renewable_potential']['unit']
    re_potential['value'] = re_potential['potential']
    model['renewable_potential'] = re_potential[['commodity', 'level', 'grade', 'value', 'node', 'year', 'unit']]

    re_cap_factor = re_year.copy()
    re_cap_factor['unit'] = data['units']['renewable_capacity_factor']['unit']
    re_cap_factor['value'] = re_cap_factor['capacity_factor']
    model['renewable_capacity_factor'] = re_cap_factor[['commodity', 'level', 'grade', 'value', 'node', 'year', 'unit']]
    return model


def change_emission_factor(raw_data: RawData, model_par: ModelPar) -> Dict[str, pd.DataFrame]:
    emissions = raw_data['base_input']['emissions']
    emission_factor: pd.DataFrame = model_par['emission_factor']
    model = {}

    def apply_change(row):
        loc = emissions['node_loc'] == row['node_loc']
        tech = emissions['technology'] == row['technology']
        year = emissions['year_act'] == row['year_act']
        emission = emissions['emission'] == row['emission']
        ef = emissions[loc & tech & year & emission]
        if not ef.empty:
            value = ef['value'].values[0]
        else:
            value = row['value']
        return value

    emission_factor['value'] = emission_factor.apply(apply_change, axis=1)
    model['emission_factor'] = emission_factor
    return model


# help functions
def _check_emissions_is_list(params: Dict[str, pd.DataFrame]) -> bool:
    if 'emission' in params['others'].index:
        if isinstance(params['others'].loc['emission'].val, list):
            _emissions_is_list = True
        else:
            _emissions_is_list = False
    else:
        _emissions_is_list = False
    return _emissions_is_list


def _get_location_techs(data: Data, loc: str, par: str) -> Tuple[Dict[str, dict], bool]:
    loc_techs = data['locations'][loc].get(par)
    if loc_techs:
        techs = [*loc_techs.keys()]
        technology = {k: v for k, v in data['technology'].items() if k in techs}
        technology = _override_techs(technology, loc_techs, techs)
        technology_exist = True

    else:
        technology = {}
        technology_exist = False

    return technology, technology_exist


def _override_techs(technology: Dict[str, dict], loc_techs: Dict[str, dict], techs: List[str]) -> Dict[str, dict]:
    override = {t: {k: v for k, v in loc_techs[t]['override'].items()} for t in techs}
    for t in techs:
        technology[t].update(override[t])

    return technology


def _get_slack_techs(data, loc: str, par: str) -> Tuple[Dict[str, dict], bool]:
    commodity = [data[par][loc]['year'][i].keys() for i in data[par][loc]['year'].keys()]
    commodity = [i for l in commodity for i in l]
    commodity = sorted(list(set(commodity)))
    technology = {}
    for c in commodity:
        technology['slack_' + c] = data['technology']['slack_' + c]
        technology['slack_' + c]['node_loc'] = loc
        technology['slack_' + c]['node_dest'] = loc
        technology['slack_' + c]['node_origin'] = loc
    technology_exist = True

    return technology, technology_exist


def _get_df_tech(technology: Dict[str, dict], t: str) -> Dict[str, dict]:
    _df = pd.DataFrame.from_dict(technology)
    _param = {}

    # create pandas Series for a given technology from a dict
    df = _df[t].dropna()
    if 'year_vtg' in df.keys():
        _param['year_vtg'] = __get_df_year(df, year_type='year_vtg')
        df = df.drop('year_vtg')

    if 'input' in df.index:
        in_out = {'input': df['input'],
                  'output': df['output']}
        df_par_in_out = pd.DataFrame.from_dict(in_out, orient='index')
        df = df.drop(['input', 'output'])
    else:
        in_out = {'output': df['output']}
        df_par_in_out = pd.DataFrame.from_dict(in_out, orient='index')
        df = df.drop(['output'])
    _param['in_out'] = df_par_in_out

    df_par = df.reset_index()
    df_par.columns = ['par', 'val']
    df_par.loc[len(df_par)] = ['technology', t]
    _param['others'] = df_par.set_index('par', drop=True)

    return _param


def __get_df_year(df: pd.Series, year_type: str) -> pd.DataFrame:
    df[year_type] = {str(k): v for k, v in df[year_type].items()}
    df_par_year = json_normalize(df[year_type])
    df_par_year.columns = split_columns(df_par_year.columns, '.')
    df_par_year = df_par_year.unstack().to_frame()
    df_par_year.reset_index(inplace=True)
    df_par_year['level_0'] = (df_par_year['level_0'].str.extract(r'(\d+)', expand=True).astype(int))
    df_par_year.drop(['level_3'], axis=1, inplace=True)
    df_par_year.columns = [year_type, 'par_name', 'par', 'val']

    return df_par_year


def __get_ref_year(df: pd.DataFrame, active_years: YearVector) -> int:
    if 'year_vtg' in df.columns:
        year = df.at[0, 'year_vtg']
    else:
        year = df.at[0, 'year_act']

    first_model_year = sorted(active_years)[0]
    if year >= first_model_year:
        ref_year = year
    else:
        ref_year = first_model_year

    return ref_year


def _comp_int(reference_year: int, df: pd.DataFrame, par: str, add_pars: dict, y_typ: str) -> pd.DataFrame:
    def calc_val(row):
        if reference_year >= row[_y_type]:
            val = row['value']
        else:
            n = 0
            if y_typ == 'vtg':
                n = row[_y_type] - reference_year
            elif y_typ == 'act':
                if 'year_vtg' in df.columns:
                    n = row[_y_type] - row['year_vtg']
                else:
                    n = row[_y_type] - reference_year

            val = row['value'] * (p ** n)
            if row['value'] >= 0:
                if val < 0:
                    val = 0
        return val

    _y_type = f'year_{y_typ}'
    p = 1 + add_pars[f'd_{par}_{y_typ}']
    if p != 1:
        if par == 'input':
            # if efficiency is increasing the input goes down and vice versa
            p = 1 / p
        df['value'] = df.apply(calc_val, axis=1)
    return df
