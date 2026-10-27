import json
from typing import Any


# canonical rules:
# Hashes, public keys, signatures, and nonces: lowercase hexadecimal strings.
# Hash objects using BLAKE2s of their canonical JSON representation.
# Messages over TCP: one compact JSON object per line, terminated by \n.
# Keys must be exactly as specified; no extra keys.
# No whitespace outside strings.
# Order keys alphabetically

class CanonicalJSONHandler():
    """Handle canonical JSON conversion and file operations."""

    @staticmethod
    def to_dict(json_str: str) -> list[dict[str, Any]]:
        """Parse one or more complete newline-delimited JSON messages."""
        dict_list = []

        for line in json_str.splitlines():
            if not line:
                continue

            data = json.loads(line)

            if not isinstance(data, dict):
                raise ValueError("Expected a JSON object")

            dict_list.append(data)

        return dict_list

    @staticmethod
    def to_canonical_json(data: dict[str, Any]) -> str:
        """Serialize a dictionary into canonical JSON."""
        if not isinstance(data, dict):
            raise ValueError("Expected a dictionary")

        return json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    @staticmethod
    def is_canonical_json(json_str: str) -> bool:
        """Determine if content is canonical."""
        try:
            data = CanonicalJSONHandler.to_dict(json_str)[0]
            return json_str == CanonicalJSONHandler.to_canonical_json(data)
        except (json.JSONDecodeError, TypeError, ValueError):
            return False

    @staticmethod
    def is_valid_json(json_str: str) -> bool:
        """Determine if content is valid JSON."""
        if not isinstance(json_str, str):
            return False
        try:
            json.loads(json_str)
            return True
        except json.JSONDecodeError:
            return False

    @staticmethod
    def is_valid_message(message_dict: dict[str, Any]) -> bool:
        """Determine if dict is message. extend as soon as all requirements are clear"""
        if "type" not in message_dict:
            return False
        return True
