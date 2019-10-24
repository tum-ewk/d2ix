from typing import NamedTuple

import pytest


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


@pytest.fixture(scope='session')
def baseline_model_config() -> TestConfig:
    test_config = TestConfig(model='MESSAGE_Indonesia', scenario='Indonesia baseline',
                             base_xls='../input/modell_data.xlsx',
                             manual_parameter_xls='../input/manual_input_parameter.xlsx', historical_data=True,
                             first_historical_year=2010,
                             first_model_year=2020, last_model_year=2030, historical_range_year=1, model_range_year=5,
                             run_config='../config/run_config.yml', verbose=False, yaml_export=False)
    return test_config
