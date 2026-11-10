import unittest

from src.object_db import ObjectDB
from src.object_handler import ObjectHandler


class TestObjectHandler(unittest.TestCase):
    def setUp(self):
        self.db = ObjectDB()
        self.db._execute("DELETE FROM known_objects")

    def test_init(self):
        # Check database file exists
        self.assertTrue(self.db.DB_PATH.exists())

        # Check table exists
        result = self.db._execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = ?",
            ("known_objects",)
        )

        self.assertEqual(len(result), 1)

    def test_check_object_contained(self):
        obj_json = '{"test": "test"}'
        obj_id = ObjectHandler.to_object_id(obj_json)

        contained = self.db.check_object_contained(obj_id)
        self.assertFalse(contained)

        success = self.db.store_object(obj_json)
        self.assertTrue(success)

        contained = self.db.check_object_contained(obj_id)
        self.assertTrue(contained)

    def test_store_object(self):
        obj_json = '{"test":"test"}'

        self.assertTrue(self.db.store_object(obj_json))
        self.assertFalse(self.db.store_object(obj_json))

    def test_get_object_with_id(self):
        expected = '{"test":"test"}'
        obj_id = ObjectHandler.to_object_id(expected)

        self.db.store_object(expected)
        actual = self.db.get_object_with_id(obj_id)
        self.assertEqual(expected, actual)

    def test_get_object_with_id_fail(self):
        obj_id = ObjectHandler.to_object_id('{"test":"test"}')

        actual = self.db.get_object_with_id(obj_id)
        self.assertEqual(actual, None)

    def test_delete_object_with_id(self):
        obj_json = '{"test":"test"}'
        obj_id = ObjectHandler.to_object_id(obj_json)

        self.assertIsNone(self.db.delete_object_with_id(obj_id))

        self.db.store_object(obj_json)
        self.assertIsNone(self.db.delete_object_with_id(obj_id))

        self.assertIsNone(self.db.get_object_with_id(obj_id))
