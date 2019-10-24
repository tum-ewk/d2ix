import os
from pathlib import Path
from typing import TypeVar, Dict

import pandas as pd

os.environ['NUMEXPR_MAX_THREADS'] = '8'

p = Path(__file__)
_LOG_CONFIG_FILE = str(p.parent.joinpath('logging.yaml'))
_CONFIG_BASE_TECHNOLOGY = str(p.parent.joinpath('config/base_technology.yml'))

D2IX_Data = TypeVar('D2IX_Data', pd.DataFrame, str, list)
ModelPar = Dict[str, D2IX_Data]
Data = Dict[str, D2IX_Data]

from d2ix.core import Model
from d2ix.core import PostProcess
from d2ix.core import ModifyModel
