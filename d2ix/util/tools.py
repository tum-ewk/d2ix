import collections
import logging
import logging.config
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML, StringIO

logger = logging.getLogger(__name__)


class YAMLd2ix(YAML):
    def dump(self, data, stream, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()

    def load(self, path):
        loader = YAML(typ='safe')
        path = Path(path)
        return loader.load(path)


def dict_to_yml(data, path, default_flow_style=False, yml_name=None):
    path_dest = Path(path)
    if yml_name:
        path_dest = path_dest.parent.joinpath(str(yml_name) + '.yml')
    to_yml = YAMLd2ix()
    to_yml.indent(offset=2)
    to_yml.default_flow_style = default_flow_style
    to_yml.dump(data=data, stream=path_dest)


def model_data_yml(config, model_par):
    dir_dest = Path(config['input_path'])
    dir_dest.mkdir(exist_ok=True)

    for k, v in model_par.items():
        path_dest = dir_dest.joinpath(k + '.yml')
        if isinstance(v, list):
            if v:
                if not isinstance(v[0], list):
                    v = list(set(v))
                dict_to_yml(sorted([str(i) for i in v]), path_dest)
                logger.debug(f'Created yaml output file: \'{k}\'')
        elif not v.empty:
            v.columns = [str(i) for i in v.columns]
            v = v.to_dict(orient='index')

            dict_to_yml(v, path_dest)
            logger.debug(f'Created yaml output file: \'{k}\'')


def split_columns(columns: pd.core.indexes.base.Index, sep: str = '.') -> pd.MultiIndex:
    if len(columns) == 0:
        return columns
    column_tuples = [tuple(col.split(sep)) for col in columns]
    return pd.MultiIndex.from_tuples(column_tuples)


def _retro_dictify(frame: pd.DataFrame) -> dict:
    d: dict = {}
    for row in frame.values:
        here = d
        for elem in row[:-2]:
            if elem not in here:
                here[elem] = {}
            here = here[elem]
        here[row[-2]] = row[-1]
    return d


def _df_to_dict_struct(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    if type(df.columns) == pd.core.index.MultiIndex:
        df = df.stack(level=0).stack()
    else:
        df = df.stack()

    df = df.to_frame()
    df.reset_index(inplace=True)
    return df


def df_to_nested_dict(df: pd.DataFrame) -> dict:
    # create nested dict from DataFrame
    df = _df_to_dict_struct(df)
    d = _retro_dictify(df)
    return d


def xls_to_yml(path, sheet_name, index=None, yml_name=None):
    p = Path(path)
    if index:
        df = pd.read_excel(p.absolute(), sheet_name=sheet_name).set_index(index)
    else:
        df = pd.read_excel(p.absolute(), sheet_name=sheet_name)

    d = df_to_nested_dict(df)

    if yml_name:
        p_out = p.parent.joinpath(str(yml_name) + '.yml')
    else:
        p_out = p.parent.joinpath(p.stem + '.yml')

    to_yml = YAMLd2ix()
    to_yml.dump(data=d, stream=p_out)


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.

    Parameters
    ----------
    dct: dict onto which the merge is executed
    merge_dct: dct merged into dct

    return dct
    """
    for k, v in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], collections.Mapping):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]
    return dct


def setup_logging(path: str = 'logging.yaml', level: int = logging.INFO) -> None:
    """Setup logging configuration

    """
    config = YAMLd2ix().load(path)
    logging.config.dictConfig(config)
    logging.getLogger().setLevel(level)
