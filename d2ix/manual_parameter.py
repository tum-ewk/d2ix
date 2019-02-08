import logging

logger = logging.getLogger(__name__)


def add_parameter_manual(parameters):
    data = {}
    for k, v in parameters.items():
        if not v.empty:
            data[k] = v
            logger.debug(f'Create manual parameter \'{k}\' manual')
    return data
