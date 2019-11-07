import tempfile

import pytest

from d2ix import Model
from d2ix import ModifyModel
from example import RUN_CONFIG
from tests.conftest import TestConfig


@pytest.fixture(scope='module')
def baseline_model_config() -> TestConfig:
    test_config = TestConfig(model='MESSAGE_Indonesia', scenario='Indonesia baseline',
                             base_xls='../input/modell_data.xlsx',
                             manual_parameter_xls='../input/manual_input_parameter.xlsx', historical_data=True,
                             first_historical_year=2010,
                             first_model_year=2020, last_model_year=2030, historical_range_year=1, model_range_year=5,
                             run_config=RUN_CONFIG, verbose=False, yaml_export=False)
    return test_config


def test_model(baseline_model_config):
    model = Model(model=baseline_model_config.model, scen=baseline_model_config.scenario, annotation='first model test',
                  base_xls=baseline_model_config.base_xls,
                  manual_parameter_xls=baseline_model_config.manual_parameter_xls,
                  historical_data=baseline_model_config.historical_data,
                  first_historical_year=baseline_model_config.first_historical_year,
                  first_model_year=baseline_model_config.first_model_year,
                  last_model_year=baseline_model_config.last_model_year,
                  historical_range_year=baseline_model_config.historical_range_year,
                  model_range_year=baseline_model_config.model_range_year, run_config=baseline_model_config.run_config,
                  verbose=baseline_model_config.verbose, yaml_export=baseline_model_config.yaml_export)

    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.create_timeseries(scenario)
    object_value = scenario.var('OBJ')['lvl']
    assert object_value == pytest.approx(398_080.469)
    model.close_db()


def test_model_without_historical(baseline_model_config):
    model = Model(model=baseline_model_config.model, scen=baseline_model_config.scenario, annotation='first model test',
                  base_xls=baseline_model_config.base_xls,
                  manual_parameter_xls=baseline_model_config.manual_parameter_xls,
                  historical_data=False,
                  first_historical_year=baseline_model_config.first_historical_year,
                  first_model_year=baseline_model_config.first_model_year,
                  last_model_year=baseline_model_config.last_model_year,
                  historical_range_year=baseline_model_config.historical_range_year,
                  model_range_year=baseline_model_config.model_range_year, run_config=baseline_model_config.run_config,
                  verbose=baseline_model_config.verbose, yaml_export=baseline_model_config.yaml_export)

    hist_parameter = ['historical_new_capacity', 'historical_activity']
    for par in hist_parameter:
        model.model_par.pop(par, None)

    model.model_par['year'] = list(
        range(baseline_model_config.first_model_year, baseline_model_config.last_model_year + 1,
              baseline_model_config.model_range_year))
    model.model_par['initial_activity_up']['value'] = 25.0

    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.create_timeseries(scenario)
    model.close_db()


def test_modify_scenario(baseline_model_config):
    with tempfile.TemporaryDirectory() as directory:
        mod_model = ModifyModel(model=baseline_model_config.model, scen=baseline_model_config.scenario,
                                xls_dir=directory, file_name='data.xlsx', verbose=baseline_model_config.verbose,
                                run_config=baseline_model_config.run_config,
                                yaml_export=baseline_model_config.yaml_export)

        mod_model.scen2xls()
        mod_model.xls2model()
        scenario = mod_model.model2db()
        scenario.solve(model='MESSAGE')
        mod_model.create_timeseries(scenario)
        mod_model.close_db()
