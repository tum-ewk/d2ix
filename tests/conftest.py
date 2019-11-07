import logging
from contextlib import contextmanager
from typing import NamedTuple

import ixmp
import message_ix

from d2ix.core import MessageInterface

logger = logging.getLogger(__name__)

RUN_CONFIG = '../config/run_config_server.yml'


class TechnologyOut(NamedTuple):
    technology: str
    commodity: str
    level: str


class Costs(NamedTuple):
    inv_cost: float
    fix_cost: float
    var_cost: float


class TestScenario(NamedTuple):
    model: str
    scenario: str
    first_test_year: int


class TestConfig(NamedTuple):
    model: str
    scenario: str
    base_xls: str
    manual_parameter_xls: str
    historical_data: bool
    first_historical_year: int
    first_model_year: int
    last_model_year: int
    historical_range_year: int
    model_range_year: int
    run_config: str
    verbose: bool
    yaml_export: bool


class RunScenario(object):
    mp: ixmp.Platform
    base_parallel_scenario: message_ix.Scenario
    db_server: bool = False

    def __init__(self, run_config: str, log_level: str = 'INFO'):
        logger.setLevel(log_level)
        self.log_level = log_level
        self.run_config = run_config
        self._define_platform()

    def _define_platform(self) -> None:
        _config = MessageInterface(self.run_config)
        db_config = _config.config['db']
        self.mp = ixmp.Platform(dbprops=db_config.get('dbprops'), dbtype=db_config.get('dbtype'))
        self.mp.set_log_level(self.log_level)
        if not db_config.get('dbtype'):
            self.db_server = True

    @contextmanager
    def make_scenario(self, clone_model: str, clone_scenario: str, new_scenario_name: str) -> message_ix.Scenario:
        logger.info(f'Clone scenario: \'{clone_scenario}\' from model: \'{clone_model}\' to \'{new_scenario_name}\'.')
        if not self.db_server:
            self.mp.open_db()
        base_ds = message_ix.Scenario(self.mp, clone_model, clone_scenario)
        scenario = base_ds.clone(scenario=new_scenario_name, keep_solution=False)
        scenario.check_out()
        yield scenario
        scenario.commit(f'Changes committed by \'{clone_model}\' - \'{new_scenario_name}\'')
        scenario.set_as_default()
        scenario.solve()
        if not self.db_server:
            self.mp.close_db()

    @contextmanager
    def read_scenario(self, model: str, scenario_name: str):
        if not self.db_server:
            self.mp.open_db()
        scenario = message_ix.Scenario(mp=self.mp, model=model, scenario=scenario_name)
        yield scenario
        if not self.db_server:
            self.mp.close_db()
