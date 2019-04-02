from d2ix import Model
from d2ix import PostProcess

MODEL = 'MESSAGE_Westeros'
BASE_XLS = 'input/modell_data_westeros.xlsx'
MANUAL_PAR_XLS = 'input/manual_input_parameter_westeros.xlsx'

VERBOSE = False
RUN_CONFIG = 'config/run_config.yml'


def run_baseline():
    # launch the IX modeling platform using a local database
    model = Model(model=MODEL, scen='baseline', base_xls=BASE_XLS,
                  manual_parameter_xls=MANUAL_PAR_XLS,
                  historical_data=True, first_historical_year=690,
                  first_model_year=700, last_model_year=720,
                  historical_range_year=10, model_range_year=10,
                  run_config=RUN_CONFIG, verbose=VERBOSE, yaml_export=False)

    # Example on how to access and edit parameters manually if neccessary
    # data_par = model.get_parameter(par='demand')
    # model.set_parameter(par=data_par, name='demand')
    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.close_db()


def run_emission_tax():
    model = Model(model=MODEL, scen='emission_tax', base_xls=BASE_XLS,
                  manual_parameter_xls=MANUAL_PAR_XLS,
                  historical_data=True, first_historical_year=690,
                  first_model_year=700, last_model_year=720,
                  historical_range_year=10, model_range_year=10,
                  run_config=RUN_CONFIG, verbose=VERBOSE, yaml_export=False)

    # Add a emission tax
    tax_emission = model.get_parameter(par='tax_emission')
    tax_emission['value'] = [0.264, 0.429, 0.699]
    model.set_parameter(par=tax_emission, name='tax_emission')
    scenario = model.model2db()
    scenario.solve(model='MESSAGE')
    model.close_db()


def run_postprocessing(version, scen):
    # Crate an instance of the d2ix post process class:
    # Post process for a specific scenario: model, scen, version
    pp = PostProcess(RUN_CONFIG, MODEL, scen, version)

    # Load results for
    results = pp.get_results()

    # Prepare data for plotting
    df = pp.create_plotdata(results)

    # Create plots
    tecs = ['coal_ppl', 'wind_ppl']

    pp.barplot(df=df, filters={'technology': tecs, 'variable': ['ACT']},
               title=f'ACT-{scen}', set_title=False)
    pp.barplot(df=df, filters={'technology': tecs, 'variable': ['CAP']},
               title=f'CAP-{scen}', set_title=False)


if __name__ == '__main__':
    run_baseline()
    run_postprocessing(version=None, scen='baseline')

    run_emission_tax()
    run_postprocessing(version=None, scen='emission_tax')
