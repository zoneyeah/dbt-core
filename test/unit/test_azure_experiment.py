
import unittest
#from dbt.utils import deep_map
from typing import (
    Tuple, Type, Any, Optional, TypeVar, Dict, Union, Callable, List, Iterator,
    Mapping, Iterable, AbstractSet, Set, Sequence
)
from dbt.exceptions import RecursionException, DbtConfigError


def _deep_map(
    func: Callable[[Any, Tuple[Union[str, int], ...]], Any],
    value: Any,
    keypath: Tuple[Union[str, int], ...],
) -> Any:
    atomic_types: Tuple[Type[Any], ...] = (int, float, str, type(None), bool)

    ret: Any
    print(1)
    if isinstance(value, list):
        ret = [
            _deep_map(func, v, (keypath + (idx,)))
            for idx, v in enumerate(value)
        ]
    elif isinstance(value, dict):
        ret = {
            k: _deep_map(func, v, (keypath + (str(k),)))
            for k, v in value.items()
        }
    elif isinstance(value, atomic_types):
        ret = func(value, keypath)
    else:
        container_types: Tuple[Type[Any], ...] = (list, dict)
        ok_types = container_types + atomic_types
        raise DbtConfigError(
            'in _deep_map, expected one of {!r}, got {!r}'
            .format(ok_types, type(value))
        )

    return ret


def deep_map(
    func: Callable[[Any, Tuple[Union[str, int], ...]], Any],
    value: Any
) -> Any:
    """map the function func() onto each non-container value in 'value'
    recursively, returning a new value. As long as func does not manipulate
    value, then deep_map will also not manipulate it.

    value should be a value returned by `yaml.safe_load` or `json.load` - the
    only expected types are list, dict, native python number, str, NoneType,
    and bool.

    func() will be called on numbers, strings, Nones, and booleans. Its first
    parameter will be the value, and the second will be its keypath, an
    iterable over the __getitem__ keys needed to get to it.

    :raises: If there are cycles in the value, raises a
        dbt.exceptions.RecursionException
    """
    try:
        return _deep_map(func, value, ())
    except RuntimeError as exc:
        if 'maximum recursion depth exceeded' in str(exc):
            raise RecursionException(
                'Cycle detected in deep_map'
            )
        raise

def is_cyclic(
    graph,
    edges = []):
    pass



class TestAzureExperiment(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_test(self):
        case_1 = {}
        case_1['foo'] = case_1

        cases = [case_1]
        #with self.assertRaises(RecursionException):
        #    deep_map(lambda x, _: x, case)
        for case in cases:
            print(is_cyclic(case))
