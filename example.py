from d2ix import Model
from d2ix import ModifyModel
from d2ix import PostProcess

MODEL = 'MESSAGE_Indonesia'
SCEN = 'Indonesia baseline'
BASE_XLS = 'input/modell_data.xlsx'
MANUAL_PAR_XLS = 'input/manual_input_parameter.xlsx'
F_HIST_Y = 2010
F_MOD_Y = 2020
L_MOD_Y = 2030
HIST_RANGE = 1
MOD_RANGE = 5
VERBOSE = False
RUN_CONFIG = 'config/run_config.yml'


def run_scenario():
    # launch the IX modeling platform using a local database
    model = Model(model=MODEL, scen=SCEN, annotation='first model test', base_xls=BASE_XLS,
                  manual_parameter_xls=MANUAL_PAR_XLS, historical_data=True, first_historical_year=F_HIST_Y,
                  first_model_year=F_MOD_Y, last_model_year=L_MOD_Y, historical_range_year=HIST_RANGE,
                  model_range_year=MOD_RANGE, run_config=RUN_CONFIG, verbose=VERBOSE, yaml_export=False)

    # Example on how to access and edit parameters manually if neccessary
    # data_par = model.get_parameter(par='demand')
    # model.set_parameter(par=data_par, name='demand')
    scenario = model.model2db()
    scenario.solve(model='MESSAGE', case='di2x_example')
    model.create_timeseries(scenario)
    model.close_db()


def modify_scenario():
    mod_model = ModifyModel(run_config=RUN_CONFIG, model=MODEL, scen=SCEN, xls_dir='input/scen2xls',
                            file_name='data.xlsx', verbose=VERBOSE)

    mod_model.scen2xls(version=None)
    mod_model.xls2model(annotation=None)

    scenario = mod_model.model2db()
    scenario.solve(model='MESSAGE')
    mod_model.create_timeseries(scenario)
    mod_model.close_db()


def run_postprocessing(version=None):
    # Crate an instance of the d2ix post process class:
    # Post process for a specific scenario: model, scen, version
    pp = PostProcess(RUN_CONFIG, MODEL, SCEN, version)

    # Load results for
    results = pp.get_results()

    # Prepare data for plotting
    df = pp.create_plotdata(results)

    # Create plots
    tecs = ['coal_ppl', 'bio_ppl', 'electricity_imp', 'slack_electricity']

    pp.barplot(df=df, filters={'technology': tecs, 'variable': ['ACT']}, title='ACT - PPL')
    pp.barplot(df=df, filters={'technology': tecs, 'variable': ['CAP']}, title='CAP - PPL')
    pp.barplot(df=df, filters={'technology': tecs, 'variable': ['CAP_NEW']}, title='CAP_NEW - PPL')


if __name__ == '__main__':
    # Option1: create a model from scratch
    run_scenario()
    run_postprocessing(version=None)

    # # Option 2: download a scenario from the db and write it to a xlsx file this file can then be edited and
    # # uploaded again.
    # modify_scenario()
    # run_postprocessing(version=None)
