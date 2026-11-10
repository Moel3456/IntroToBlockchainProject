import hashlib


class ObjectHandler:
    @staticmethod
    def get_get_object_message(obj_id: str) -> dict:
        return {"type": "getobject", "objectid": obj_id}

    @staticmethod
    def get_i_have_object_message(obj_id: str) -> dict:
        return {"type": "ihaveobject", "objectid": obj_id}

    @staticmethod
    def get_object_message(obj: dict) -> dict:
        """Return a ready-to-send object message."""
        return {"type": "object", "object": obj}

    @staticmethod
    def to_object_id(obj: str) -> str:
        """obj: Object represented as a canonical JSON string.

        Return the BLAKE2s hash as a hexadecimal string."""
        return hashlib.blake2s(obj.encode("utf-8")).hexdigest()
