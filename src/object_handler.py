import hashlib


class ObjectHandler:
    @staticmethod
    def prep_get_object():
        pass

    @staticmethod
    def prep_i_have_object():
        pass

    @staticmethod
    def prep_object_message():
        pass

    @staticmethod
    def to_object_id(obj: str) -> str:
        """
            Generate a BLAKE2s object ID.

            Args:
                obj: Object represented as a canonical JSON string.

            Returns:
                The BLAKE2s hash as a hexadecimal string.
            """
        return hashlib.blake2s(obj.encode("utf-8")).hexdigest()
