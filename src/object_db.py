import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
db_path = PROJECT_ROOT / "data" / "known_objects.db"

from src.object_handler import ObjectHandler
from src.canonical_json_handler import CanonicalJSONHandler


class ObjectDB:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DB_PATH = PROJECT_ROOT / "data" / "known_objects.db"

    def __init__(self):
        self.ensure_table_exists()

    def _execute(self, query, params=()):
        connection = sqlite3.connect(self.DB_PATH, timeout=10)

        try:
            cursor = connection.execute(query, params)
            result = cursor.fetchall()
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def ensure_table_exists(self):
        self._execute(
            "CREATE TABLE IF NOT EXISTS known_objects "
            "(obj_id TEXT PRIMARY KEY, "
            "obj_json TEXT NOT NULL, "
            "saved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )

    def store_object(self, obj: str | dict) -> bool:
        """Store a canonical JSON object if it does not already exist.
        obj: canonical JSON of object or the object (dict) itself
        Returns True if stored, False if already present or invalid."""

        if isinstance(obj, dict):
            obj_json = CanonicalJSONHandler.to_canonical_json(obj)
        elif CanonicalJSONHandler.is_canonical_json(obj):
            obj_json = obj
        else:
            return False

        o_id = ObjectHandler.to_object_id(obj_json)
        if self.check_object_contained(o_id):
            return False
        query = "INSERT INTO known_objects (obj_id, obj_json) VALUES (?,?)"
        self._execute(query, (o_id, obj_json))
        return True

    def check_object_contained(self, obj_id: str) -> bool:
        """Check whether an object ID exists in the database."""
        query = "SELECT 1 FROM known_objects WHERE obj_id = ?"
        return bool(self._execute(query, (obj_id,)))

    def get_object_with_id(self, obj_id: str, as_dict: bool = True) -> str | dict[str, Any] | None:
        """Return the stored object as a dict or canonical JSON string."""

        query = "SELECT obj_json FROM known_objects WHERE obj_id = ?"
        result = self._execute(query, (obj_id,))

        if not result:
            return None

        obj_json = result[0][0]

        if as_dict:
            return CanonicalJSONHandler.to_dict(obj_json)[0]

        return obj_json

    def delete_object_with_id(self, obj_id: str) -> None:
        """Delete an object from the database.
        Returns None if deleted or not present"""
        query = "DELETE FROM known_objects WHERE obj_id = ?"
        self._execute(query, (obj_id,))
