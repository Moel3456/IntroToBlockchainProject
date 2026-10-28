"""Configuration for our Kerma node."""

from src.main import Settings


def load_settings() -> Settings:
    return Settings(
        bind_host="0.0.0.0",
        port=18018,
        agent="KermaNode 0.10.0",
        advertised_address=None,
        max_outgoing_connections=8,
    )