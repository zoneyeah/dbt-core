
import unittest
from dbt.utils import deep_map
from dbt.exceptions import RecursionException

class TestAzureExperiment(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass

    def test_test(self):
        case = {}
        case['foo'] = case
        with self.assertRaises(RecursionException):
            deep_map(lambda x, _: x, case)