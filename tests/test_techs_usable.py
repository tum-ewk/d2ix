from typing import Dict, List, Union

import message_ix
import pytest

from tests.conftest import TestScenario, Costs, TechnologyOut, RunScenario, RUN_CONFIG

REF_SCENARIO = TestScenario(model='MESSAGE_Indonesia', scenario='Indonesia baseline', first_test_year=2020)
NODES = ['Indonesia']

BASE_COSTS = Costs(inv_cost=10_000_000.0, fix_cost=10_000_000.0, var_cost=10_000_000.0)
SELECTED_COSTS = Costs(inv_cost=0.01, fix_cost=0.01, var_cost=0.01)

EXCLUDE_TECHS_NODES: Dict[str, List[str]] = {'coal_extr': NODES}

SELECTED_TECHNOLOGIES = False
technology_selection = [TechnologyOut(technology='coal_imp', commodity='coal', level='primary')]


@pytest.fixture(scope='session')
def scenario_runner() -> RunScenario:
    runner = RunScenario(RUN_CONFIG, log_level='NOTSET')
    return runner


def change_tech_costs(scenario: message_ix.Scenario, techs: Union[str, List[str]], costs: Costs) -> message_ix.Scenario:
    if isinstance(techs, str):
        techs = [techs]

    inv = scenario.par('inv_cost')
    if not inv.empty:
        inv.loc[inv['technology'].isin(techs), 'value'] = costs.inv_cost
        scenario.add_par('inv_cost', inv)

    fix = scenario.par('fix_cost')
    if not fix.empty:
        fix.loc[fix['technology'].isin(techs), 'value'] = costs.fix_cost
        scenario.add_par('fix_cost', fix)

    var = scenario.par('var_cost')
    if not var.empty:
        var.loc[var['technology'].isin(techs), 'value'] = costs.var_cost
        scenario.add_par('var_cost', var)
    return scenario


def techs_same_lvl_com(scenario: message_ix.Scenario, level: str, commodity: str) -> List[str]:
    scenario.commodity = commodity
    output = scenario.par('output')
    _techs = output[(output.level == level) & (output.commodity == commodity)]
    _techs = _techs[~_techs.technology.str.contains('slack')]
    techs = sorted(list(set(_techs.technology.values)))
    return techs


def id_func(param):
    return repr(param)


def baseline_techs() -> List[TechnologyOut]:
    if SELECTED_TECHNOLOGIES:
        _techs = technology_selection
    else:
        baseline = RunScenario(RUN_CONFIG, log_level='NOTSET')
        with baseline.read_scenario(model=REF_SCENARIO.model, scenario_name=REF_SCENARIO.scenario) as scenario:
            output = scenario.par('output')
        _data = output[['level', 'commodity', 'technology']]
        _data = _data.dropna()
        _data = _data.drop_duplicates()
        _techs = [TechnologyOut(level=x[0], commodity=x[1], technology=x[2]) for x in _data.values]
    _techs = [i for i in _techs if i.technology not in EXCLUDE_TECHS_NODES.keys()]
    return _techs


@pytest.mark.parametrize('tech', baseline_techs(), ids=id_func)
def test_tech_usable(tech: TechnologyOut, scenario_runner: RunScenario):
    with scenario_runner.make_scenario(clone_model=REF_SCENARIO.model, clone_scenario=REF_SCENARIO.scenario,
                                       new_scenario_name='py-test') as scenario:
        techs = techs_same_lvl_com(scenario=scenario, level=tech.level, commodity=tech.commodity)
        scenario = change_tech_costs(scenario=scenario, techs=techs, costs=BASE_COSTS)
        change_tech_costs(scenario=scenario, techs=tech.technology, costs=SELECTED_COSTS)

    with scenario_runner.read_scenario(model=REF_SCENARIO.model, scenario_name='py-test') as scenario:
        act = scenario.var('ACT')

    act = act[act['year_act'] > REF_SCENARIO.first_test_year]

    # act same com lvl
    act_tech = act[act['technology'] == tech.technology]

    years = len(act['year_act'].unique())
    expected_test_data = {k: (years * [1] if k not in EXCLUDE_TECHS_NODES.get(tech.technology, []) else years * [0]) for
                          k in NODES}
    test_data = dict.fromkeys(NODES)
    for n in NODES:
        act_tech_val = act_tech[act_tech['node_loc'] == n].groupby('year_act').sum().lvl.values
        test_data[n] = [1 if k > 0.0 else int(k) for k in act_tech_val]

    different_nodes = [k for k in expected_test_data.keys() if expected_test_data[k] != test_data[k]]
    assert expected_test_data == test_data, f'Technology: {tech} not usable in node: \'{different_nodes}\'.'
