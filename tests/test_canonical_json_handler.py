import unittest

from src.canonical_json_handler import CanonicalJSONHandler


class TestCanonicalJsonHandler(unittest.TestCase):
    def test_to_dict_multiple_messages(self):
        input_json = (
            '{"type":"peers","peers":["kermanode.net:18017"]}\n'
            '{"type":"block"}\n'
        )

        expected = [
            {"type": "peers", "peers": ["kermanode.net:18017"]},
            {"type": "block"},
        ]

        self.assertEqual(
            CanonicalJSONHandler.to_dict(input_json),
            expected,
        )

    def test_to_dict_single_message(self):
        input_json = (
            '{"type":"peers","peers":["kermanode.net:18017"]}\n'
        )

        expected = [
            {"type": "peers", "peers": ["kermanode.net:18017"]}
        ]

        self.assertEqual(
            CanonicalJSONHandler.to_dict(input_json),
            expected,
        )

    def test_to_canonical_json(self):
        data = {"type": "peers", "peers": ["node.example:18017"]}

        result = CanonicalJSONHandler.to_canonical_json(data)

        self.assertEqual(
            result,
            '{"peers":["node.example:18017"],"type":"peers"}',
        )

    def test_is_canonical_json(self):
        canonical = '{"peers":[],"type":"peers"}'
        non_canonical = '{"peers":[],"type":"peers" }'
        non_canonical2 = '{"peers":[],"type"    :"peers"}'
        non_canonical3 = '{ "type": "peers", "peers": [] }'

        self.assertTrue(
            CanonicalJSONHandler.is_canonical_json(canonical)
        )
        self.assertFalse(
            CanonicalJSONHandler.is_canonical_json(non_canonical)
        )
        self.assertFalse(
            CanonicalJSONHandler.is_canonical_json(non_canonical2)
        )
        self.assertFalse(
            CanonicalJSONHandler.is_canonical_json(non_canonical3)
        )

    def test_is_valid_json(self):
        self.assertTrue(
            CanonicalJSONHandler.is_valid_json('{"type":"peers"}')
        )
        self.assertFalse(
            CanonicalJSONHandler.is_valid_json('{"type":')
        )

        self.assertFalse(
            CanonicalJSONHandler.is_valid_json('{"type":"peers}')
        )

        self.assertFalse(
            CanonicalJSONHandler.is_valid_json('{"type":"peers"')
        )

        self.assertFalse(
            CanonicalJSONHandler.is_valid_json(None)
        )

    def test_is_valid_message(self):
        self.assertTrue(
            CanonicalJSONHandler.is_valid_message({"type": "peers"})
        )
        self.assertFalse(
            CanonicalJSONHandler.is_valid_message({"peers": []})
        )
