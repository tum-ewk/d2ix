import logging
import sys
from pathlib import Path
from typing import Dict, Optional, List, Union

import ixmp as ix
import message_ix
import pandas as pd
from pandas import ExcelWriter

from d2ix import _CONFIG_BASE_TECHNOLOGY, ModelPar, Data, RawData
from d2ix import _LOG_CONFIG_FILE
from d2ix.demand import add_demand
from d2ix.manual_parameter import add_parameter_manual
from d2ix.postprocess import create_timeseries_df, create_barplot, create_plotdata_df, extract_synonyms_colors
from d2ix.preprocess import process_demand, process_base_techs, process_spec_techs, process_spatial_locations, \
    process_units, process_lvl_spatial, process_map_spatial_hierarchy, process_level
from d2ix.sets import add_sets, extract_sets, set_frame_list, set_order
from d2ix.technology import add_technology, add_reliability_flexibility_parameter, create_renewable_potential, \
    change_emission_factor
from d2ix.util import model_data_yml, YAMLd2ix, check_input_data, setup_logging

logger = logging.getLogger(__name__)


class MessageInterface(object):
    LOG_LEVEL = 'INFO'
    config: dict = {}

    def __init__(self, run_config: Optional[str] = None, verbose: bool = False) -> None:

        self._create_logger(verbose)
        self._init_run_config(run_config)
        self._mp = self.Platform(self.config['db'])
        self._mp.set_log_level(level=self.LOG_LEVEL)
        self._local_db = self._mp.dbtype == 'HSQLDB'

    def _init_run_config(self, run_config: Optional[str]) -> None:
        logger.info('Load data base configurations')

        if run_config:
            p = Path(run_config)
            if p.exists():
                _conf = YAMLd2ix().load(path=run_config)
                self.config['db'] = _conf
            else:
                self.config['db'] = {'dbprops': None, 'dbtype': 'HSQLDB'}
        else:
            self.config['db'] = {'dbprops': None, 'dbtype': 'HSQLDB'}

    def _create_logger(self, verbose: bool) -> None:
        setup_logging(path=_LOG_CONFIG_FILE, level=getattr(logging, self.LOG_LEVEL))
        if self.LOG_LEVEL == 'NOTSET':
            logging.getLogger('d2ix').propagate = False
        elif verbose is True:
            self.LOG_LEVEL = 'DEBUG'
            logging.getLogger('d2ix').setLevel(logging.DEBUG)

    def close_db(self) -> None:
        logger.debug(f'> Close database at \'{self.config["db"]["dbprops"]}\'')
        self._mp.close_db()

    def open_db(self) -> None:
        logger.debug(f'> Open database at \'{self.config["db"]["dbprops"]}\'')
        self._mp.open_db()

    @staticmethod
    def Platform(db_config: Dict[str, str]) -> ix.Platform:
        return ix.Platform(dbprops=db_config.get('dbprops'), dbtype=db_config.get('dbtype'))

    def Scenario(self, model: str, scen: str, version: Optional[Union[int, str]] = None,
                 annotation: Optional[str] = None, cache: bool = False) -> message_ix.Scenario:
        """Initialize a new message_ix.Scenario (structured input data and
        solution) or get an existing scenario from the ixmp database instance
        """
        if self._local_db:
            self.open_db()
        return message_ix.Scenario(self._mp, model, scen, version, annotation, cache)


class DBInterface(MessageInterface):
    scenario: message_ix.Scenario
    model_type: str
    year_vector: List[int]
    data: Data
    model_par: ModelPar
    sets: dict

    def __init__(self, run_config: Optional[str], verbose: bool, yaml_export: bool = True) -> None:
        super().__init__(run_config, verbose)
        self.yaml_export = yaml_export

    def model2db(self) -> message_ix.Scenario:
        logger.info('Prepare model input data')
        # remove NaN row from data
        for k, v in self.model_par.items():
            if isinstance(v, list):
                self.model_par[k] = [x for x in v if str(x) != 'nan']
            else:
                self.model_par[k] = v.dropna().reset_index(drop=True)

        if self.model_type == 'new':
            # check units if exists
            logger.info('Unit check')
            units = set([self.data['units'][p]['unit'] for p in self.data['units'].keys()])
            units_to_add = units.difference(set(self._mp.units()))
            if units_to_add:
                text = f'Units: {units_to_add} are not defined - will be added'
                logger.info(text)
                for unit in units_to_add:
                    self._mp.add_unit(unit)

            _sets = {k: v for k, v in self.model_par.items() if k in self.scenario.set_list()}
            _sets['year'] = self.year_vector
        else:
            # model_tye == 'modify'
            _sets = {k: v for k, v in self.model_par.items() if k in self.scenario.set_list()}
            _sets = set_frame_list(self.scenario, _sets)

        logger.info('Add sets to scenario')
        for i in set_order():
            if i in _sets.keys():
                self.scenario.add_set(i, _sets[i])

        logger.info('Add parameter to scenario')
        _pars = {k: v for k, v in self.model_par.items() if k in self.scenario.par_list()}
        for k, v in _pars.items():
            self.scenario.add_par(k, v)

        self.scenario.commit(f'Model {self.scenario} created')
        self.scenario.set_as_default()

        if self.yaml_export:
            logger.info('Write yaml output files')
            model_data_yml(self.config, self.model_par)
        return self.scenario

    def pull_results(self, model: str, scen: str, version: Optional[Union[int, str]]) -> message_ix.Scenario:
        logger.info(f'Load results for model: \'{model}\', scenario: \'{scen}\', version: \'{version}\'')
        return self.Scenario(model, scen, version)

    def get_parameter(self, par: str) -> pd.DataFrame:
        return self.model_par.get(par, None)

    def set_parameter(self, par: str, name: str) -> None:
        self.model_par[name] = par

    def create_timeseries(self, scenario: message_ix.Scenario) -> None:
        self.scenario = create_timeseries_df(results=scenario)


class Model(DBInterface):
    """ GHD Model linked to the IX modeling platform (IXMP).

    Parameters
    ----------
    run_config : string
        config file for ixmp parameter 'dbprops' and 'dbtype', and the
        optional parameter 'model' path/name to the model definition

    verbose : boolean
    """
    data: Data = {}
    raw_data: RawData = {}
    model_par: ModelPar = {}
    active_years: list = []
    historical_years: list = []
    year_vector: list = []
    duration_period: dict = {}
    duration_period_sum: pd.DataFrame

    ENABLE_SLACK_TECHS = True

    def __init__(self, model: str, scen: str, base_xls: str, historical_range_year: int, first_historical_year: int,
                 model_range_year: int, first_model_year: int, last_model_year: int,
                 manual_parameter_xls: Optional[str] = None,
                 annotation: Optional[str] = None, historical_data: bool = True,
                 run_config: Optional[str] = None, verbose: bool = False,
                 yaml_export: bool = True) -> None:
        super().__init__(run_config, verbose, yaml_export)

        self.config['base_xls'] = base_xls
        self.config['manual_parameter_xls'] = manual_parameter_xls
        self.config['input_path'] = str(
            Path(base_xls).parent.joinpath('yaml_export'))
        self.model_type = 'new'
        self.model = model
        self.scen = scen
        self.version = 'new'
        self.annotation = annotation

        self.historical_data = historical_data
        self.historical_range_year = historical_range_year
        self.first_historical_year = first_historical_year
        self.first_model_year = first_model_year
        self.last_model_year = last_model_year
        self.model_range_year = model_range_year

        self._create_year_vectors()
        self._calc_duration_period()

        self.manual_input = False

        # create new message scenario
        self.scenario = self.Scenario(model, scen, 'new', annotation)

        # load raw input data
        self._load_raw_input_data()

        self._preprocess()
        self._create_model()

    def _create_year_vectors(self) -> None:
        if self.historical_data:
            if not (self.first_historical_year < self.first_model_year
                    < self.last_model_year):
                logger.error(
                    f'Wrong year settings: first_historical_year:'
                    f'{self.first_historical_year} < first_model_year:'
                    f'{self.first_model_year} < last_model_year:'
                    f'{self.last_model_year}')
                sys.exit()
        else:
            if not self.first_model_year < self.last_model_year:
                logger.error(
                    f'Wrong year settings: first_model_year:'
                    f'{self.first_model_year} < last_model_year:'
                    f'{self.last_model_year}')
                sys.exit()

        self.active_years = list(
            range(self.first_model_year, self.last_model_year + 1, self.model_range_year))
        if self.historical_data:
            self.historical_years = list(
                range(self.first_historical_year, self.first_model_year - self.model_range_year + 1,
                      self.historical_range_year))
            self.year_vector = self.historical_years + self.active_years
        else:
            self.year_vector = self.active_years

    def _calc_duration_period(self) -> None:
        _years = self.year_vector

        self.duration_period[_years[0]] = None
        self.duration_period.update({_years[i + 1]: _years[i + 1] - _years[i] for i in range(0, len(_years) - 1)})
        self.duration_period[_years[0]] = self.duration_period[_years[1]]

        _d_p_sum = {_years[1]: self.duration_period[_years[0]]}
        _tmp = list(self.duration_period.values())[1:]
        _d_p_sum.update(
            {_years[i + 1]: sum(_tmp[0:i]) + self.duration_period[_years[0]] for i in range(1, len(_years) - 1)})

        df = pd.DataFrame(columns=self.year_vector[1:], index=self.year_vector[1:-1])

        idx = list(df.index)
        for y2 in df.columns:
            df.loc[:, y2] = [int(_d_p_sum[y2] - _d_p_sum[y1]) if y1 < y2 else 0 for y1 in idx]
        df.loc[_years[0], :] = [v for k, v in _d_p_sum.items()]
        df.loc[_years[-1], :] = 0
        df[_years[0]] = 0
        df = df.sort_index().astype(int)
        df = df[self.year_vector]
        self.duration_period_sum = df

    def _load_raw_input_data(self) -> None:
        logger.info(f'Load model input data from: \'{self.config["base_xls"]}\'')
        pars = ['demand', 'spec_techs', 'unit', 'locations', 'lvl_spatial', 'map_spatial_hierarchy', 'level',
                'rel_and_flex', 'renewable_potential', 'emissions']

        _tmp = pd.read_excel(self.config['base_xls'], sheet_name=None)
        _tmp = {k: v for k, v in _tmp.items() if (k in pars) and not v.empty}
        self.raw_data['base_input'] = _tmp

        # load default techs
        self.raw_data['base_tech'] = YAMLd2ix().load(_CONFIG_BASE_TECHNOLOGY)

        p = Path(self.config['manual_parameter_xls'])
        if p.exists():
            self.manual_input = True
            logger.info(f'Load model input data from: \'{self.config["manual_parameter_xls"]}\'')
            _tmp = pd.read_excel(p, sheet_name=None)
            _tmp = {k: v for k, v in _tmp.items() if not v.empty}
            self.raw_data['manual_input'] = _tmp
        elif self.config['manual_parameter_xls'] is not None:
            logger.error(f'Path \'{p}\'does not exist')

    def _preprocess(self) -> None:
        logger.debug('Create helper dict structure')
        self.data['demand'] = process_demand(self.raw_data)
        self.data['technology'] = process_base_techs(self.raw_data, self.year_vector, self.first_model_year,
                                                     self.duration_period_sum)
        self.data['units'] = process_units(self.raw_data)
        self.data['technology'].update(
            process_spec_techs(self.raw_data, self.data, self.year_vector, self.first_model_year,
                               list(self.scenario.par_list()), self.duration_period_sum))
        self.data['locations'] = process_spatial_locations(self.raw_data)
        self.data['lvl_spatial'] = process_lvl_spatial(self.raw_data)
        self.data['map_spatial_hierarchy'] = process_map_spatial_hierarchy(self.raw_data)

        _levels = process_level(self.raw_data)
        for k, v in _levels.items():
            getattr(self, 'data')[k] = v

    def _create_model(self) -> None:
        # get used parameter
        par_list = list(self.data['units'].keys())
        par_list.remove('demand')
        self.data['technology_parameter'] = par_list
        par_list.extend(['demand'])
        self.model_par = {i: self.scenario.par(i) for i in par_list}

        # add parameters manual to the model
        if self.manual_input:
            logger.info(f'Create parameters from: \'{self.config["manual_parameter_xls"]}\'')
            self.model_par.update(add_parameter_manual(self.raw_data['manual_input']))

        # add technologies over locations
        logger.info(f'Create parameters from: \'{self.config["base_xls"]}\'')
        for loc in self.data['locations'].keys():
            self.model_par.update(add_technology(self.data, self.model_par, self.first_model_year, self.active_years,
                                                 self.historical_years, self.duration_period_sum, loc,
                                                 par='technology'))

        # add demand to the model over locations
        logger.info(f'Create demands from: \'{self.config["base_xls"]}\'')
        for loc in sorted(self.data['demand'].keys()):
            self.model_par.update(add_demand(self.data, self.model_par, loc))
            if self.ENABLE_SLACK_TECHS is True:
                self.model_par.update(
                    add_technology(self.data, self.model_par, self.first_model_year, self.active_years,
                                   self.historical_years, self.duration_period_sum, loc, par='demand', slack=True))

        # add rel and flex parameter
        if 'rel_and_flex' in self.raw_data['base_input'].keys():
            logger.info(f'Create parameters from: \'{self.config["base_xls"]}\' - \'rel_and_flex\'')
            self.model_par.update(add_reliability_flexibility_parameter(self.data, self.model_par, self.raw_data))

        # add renewable potential parameter
        if 'renewable_potential' in self.raw_data['base_input'].keys():
            logger.info(f'Create parameters from: \'{self.config["base_xls"]}\' - \'renewable_potential\'')
            self.model_par.update(create_renewable_potential(self.raw_data, self.data, self.active_years))

        # change emission factor
        if 'emissions' in self.raw_data['base_input'].keys():
            logger.info(f'Change emission factor from: \'{self.config["base_xls"]}\' - \'emissions\'')
            self.model_par.update(change_emission_factor(self.raw_data, self.model_par))

        # add sets
        logger.info(f'Create sets from: \'{self.config["base_xls"]}\' and \'{self.config["manual_parameter_xls"]}\'')
        self.model_par.update(extract_sets(self.scenario, self.model_par))
        self.model_par.update(add_sets(self.data, self.model_par, self.first_model_year))

        # sanity checks
        check_input_data(self.raw_data, self.model_par)


class ModifyModel(DBInterface):
    model_par: ModelPar = {}
    scenario: message_ix.Scenario

    def __init__(self, model: str, scen: str, run_config: Optional[str] = None,
                 xls_dir: str = 'scen2xls', file_name: str = 'data.xlsx', verbose: bool = False,
                 yaml_export: bool = True) -> None:
        super().__init__(run_config, verbose, yaml_export)
        self.model = model
        self.scen = scen
        self.version: Optional[Union[int, str]] = None
        self.annotation: Optional[str] = None
        self.xls_dir: Path = Path(xls_dir)
        self.xls_dir.mkdir(exist_ok=True)
        self.file_name = self.xls_dir.joinpath(file_name)

        self.config['input_path'] = str(self.xls_dir.joinpath('yaml_export'))
        self.model_type = 'modify'

    def get_model_pars(self, version: Optional[Union[int, str]] = None) -> None:
        self.scenario = self.pull_results(self.model, self.scen, version)
        self.model_par.update({par: self.scenario.par(par) for par in self.scenario.par_list()})
        self.model_par.update({sets: self.scenario.set(sets) for sets in self.scenario.set_list()})

    def scen2xls(self, version: Optional[Union[int, str]] = None) -> None:
        self.get_model_pars(version)

        logger.info('Write model to excel')
        with ExcelWriter(str(self.file_name)) as writer:
            for k in self.model_par.keys():
                _data: pd.DataFrame = self.model_par[k]
                if not _data.empty:
                    _data.to_excel(writer, sheet_name=k)

    def xls2model(self, annotation: Optional[str] = None) -> None:
        logger.info('Create model from excel')
        self.model_par = pd.read_excel(self.file_name, sheet_name=None)
        self.version = 'new'
        self.annotation = annotation
        self.scenario = self.Scenario(self.model, self.scen, self.version, self.annotation)


class PostProcess(DBInterface):
    raw_data: RawData
    attributes: dict = {}

    def __init__(self, run_config: Optional[str], model: str, scen: str, version: Optional[Union[int, str]] = None,
                 base_xls: Optional[str] = None, verbose: bool = False) -> None:
        super().__init__(run_config, verbose)
        self.model = model
        self.scen = scen
        self.version = version
        if isinstance(base_xls, str):
            self.base_xls = base_xls
            self._get_synonyms_colors()

    def _get_synonyms_colors(self) -> None:
        logger.info(f'Load model input data from: \'{self.base_xls}\'')
        self.raw_data['spec_techs'] = pd.read_excel(self.base_xls, sheet_name='spec_techs')
        _tmp = self.raw_data['spec_techs'].get(['technology', 'postprocess_color', 'postprocess_synonym'])
        if isinstance(_tmp, pd.DataFrame):
            if not _tmp.empty:
                _tmp = _tmp.rename(columns={k: k.replace('postprocess_', '') for k in _tmp.columns})
                _tmp = _tmp.reset_index(drop=True)
                self.attributes = extract_synonyms_colors(_tmp)

    def get_results(self) -> message_ix.Scenario:
        return self.pull_results(self.model, self.scen, self.version)

    def barplot(self, df, filters, title, other_bin_size=0.03, other_name='other', synonyms=False, colors=False,
                tech_order=None, colormap=None, set_title=True):
        if isinstance(colormap, str):
            self.attributes['colormap'] = colormap
        create_barplot(df, filters, title, self.attributes, other_bin_size, other_name, synonyms, colors, tech_order,
                       set_title)

    @staticmethod
    def create_plotdata(results: message_ix.Scenario) -> pd.DataFrame:
        return create_plotdata_df(results)
