import os
from pathlib import Path
from typing import Dict, Union

import pandas as pd

os.environ['NUMEXPR_MAX_THREADS'] = '8'

p = Path(__file__)
_LOG_CONFIG_FILE = str(p.parent.joinpath('logging.yaml'))
_CONFIG_BASE_TECHNOLOGY = str(p.parent.joinpath('config/base_technology.yml'))

ModelPar = Dict[str, Union[pd.DataFrame, list]]
Data = Dict[str, Union[dict, list, str]]

from d2ix.core import Model
from d2ix.core import PostProcess
from d2ix.core import ModifyModel
