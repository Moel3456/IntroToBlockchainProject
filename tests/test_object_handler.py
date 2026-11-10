import unittest

from src.object_handler import ObjectHandler
from src.canonical_json_handler import CanonicalJSONHandler


class TestObjectHandler(unittest.TestCase):
    def test_to_object_id(self):
        genesis_block = {
            "T": "0000abc000000000000000000000000000000000000000000000000000000000",
            "created": 1671062400,
            "miner": "Marabu",
            "nonce": "00000000000000000000000000000000000000000000000000000000005bb0f2",
            "note": "The New York Times 2022-12-13: Scientists Achieve Nuclear Fusion Breakthrough With Blast of 192 Lasers",
            "previd": None,
            "txids": [],
            "type": "block"
        }

        cl_json = CanonicalJSONHandler.to_canonical_json(genesis_block)

        expected = "00002fa163c7dab0991544424b9fd302bb1782b185e5a3bbdf12afb758e57dee"
        actual = ObjectHandler.to_object_id(cl_json)

        self.assertEqual(expected, actual)
