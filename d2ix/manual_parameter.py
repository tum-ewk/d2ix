import logging
from typing import Dict
import pandas as pd

logger = logging.getLogger(__name__)


def add_parameter_manual(parameters: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    data = {}
    for k, v in parameters.items():
        if not v.empty:
            data[k] = v
            logger.debug(f'Create manual parameter \'{k}\' manual')
    return data
