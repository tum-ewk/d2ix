import os
from pathlib import Path
from typing import Dict, Union

import pandas as pd
from mypy_extensions import TypedDict

os.environ['NUMEXPR_MAX_THREADS'] = '8'

p = Path(__file__)
_LOG_CONFIG_FILE = str(p.parent.joinpath('logging.yaml'))
_CONFIG_BASE_TECHNOLOGY = str(p.parent.joinpath('config/base_technology.yml'))

ModelPar = Dict[str, Union[pd.DataFrame, list]]

Data = TypedDict('Data', {'demand': dict, 'technology': dict, 'units': dict, 'locations': dict, 'lvl_spatial': list,
                          'map_spatial_hierarchy': list, 'level_renewable': str, 'level_resource': str,
                          'technology_parameter': list})

RawData = TypedDict('RawData', {'base_input': dict, 'base_tech': dict, 'manual_input': dict})

from d2ix.core import Model
from d2ix.core import PostProcess
from d2ix.core import ModifyModel
