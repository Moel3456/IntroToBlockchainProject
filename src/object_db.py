import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
db_path = PROJECT_ROOT / "data" / "known_objects.db"

from src.object_handler import ObjectHandler


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

    def store_object(self, obj: str) -> bool:
        """Store a canonical JSON object if it does not already exist.

        Returns True if stored, False if already present."""
        o_id = ObjectHandler.to_object_id(obj)
        if self.check_object_contained(o_id):
            return False
        query = "INSERT INTO known_objects (obj_id, obj_json) VALUES (?,?)"
        self._execute(query, (o_id, obj))
        return True

    def check_object_contained(self, obj_id: str) -> bool:
        """Check whether an object ID exists in the database."""
        query = "SELECT 1 FROM known_objects WHERE obj_id = ?"
        return bool(self._execute(query, (obj_id,)))

    def get_object_with_id(self, obj_id: str) -> str | None:
        """Return the canonical JSON object or None if not found."""
        query = "SELECT obj_json FROM known_objects WHERE obj_id = ?"
        result = self._execute(query, (obj_id,))
        return result[0][0] if result else None

    def delete_object_with_id(self, obj_id: str) -> None:
        """Delete an object from the database.
        Returns None if deleted or not present"""
        query = "DELETE FROM known_objects WHERE obj_id = ?"
        self._execute(query, (obj_id,))
