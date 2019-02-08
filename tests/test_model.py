"""
Tests with pytest
"""

from d2ix import Model
from d2ix import ModifyModel

MODEL = 'MESSAGE_Indonesia'
SCEN = 'Indonesia baseline'
BASE_XLS = '../input/modell_data.xlsx'
MANUAL_PAR_XLS = '../input/manual_input_parameter.xlsx'
F_HIST_Y = 2010
F_MOD_Y = 2020
L_MOD_Y = 2030
HIST_RANGE = 1
MOD_RANGE = 5
VERBOSE = False
RUN_CONFIG = 'config/run_config.yml'


def test_model():
    model = Model(model=MODEL, scen=SCEN, annotation='first model test',
                  base_xls=BASE_XLS, manual_parameter_xls=MANUAL_PAR_XLS,
                  historical_data=True, first_historical_year=F_HIST_Y,
                  first_model_year=F_MOD_Y, last_model_year=L_MOD_Y,
                  historical_range_year=HIST_RANGE, model_range_year=MOD_RANGE,
                  run_config=RUN_CONFIG, verbose=VERBOSE, yaml_export=True)

    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.create_timeseries(scenario)


def test_modify_scenario():
    mod_model = ModifyModel(model=MODEL,
                            scen=SCEN,
                            xls_dir='../input/scen2xls',
                            file_name='data.xlsx',
                            verbose=False)

    mod_model.scen2xls(version=None)
    mod_model.xls2model(annotation=None)

    scenario = mod_model.model2db()
    scenario.solve(model='MESSAGE')
    mod_model.create_timeseries(scenario)


def test_model_without_historical():
    model = Model(model=MODEL, scen=SCEN, annotation='first model test',
                  base_xls=BASE_XLS, manual_parameter_xls=MANUAL_PAR_XLS,
                  historical_data=False, first_historical_year=F_HIST_Y,
                  first_model_year=F_MOD_Y, last_model_year=L_MOD_Y,
                  historical_range_year=HIST_RANGE, model_range_year=MOD_RANGE,
                  run_config=RUN_CONFIG, verbose=VERBOSE, yaml_export=True)

    hist_parameter = ['historical_new_capacity', 'historical_activity']
    for par in hist_parameter:
        model.model_par.pop(par, None)

    model.model_par['year'] = list(range(F_MOD_Y, L_MOD_Y + 1, MOD_RANGE))
    model.model_par['initial_activity_up']['value'] = 25.0

    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.create_timeseries(scenario)
