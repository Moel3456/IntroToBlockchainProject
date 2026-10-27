"""Entry point: wires settings, peer storage, the protocol handler and the networking layer together.

Run from the repository root:

    python -m src.main

This module only wires things up. Sockets, listening, connection limits and
reconnects belong to src/network.py, which must provide:

    create_adapter(settings: Settings) -> NetworkAdapter

Settings come from src/config.py, which must provide:

    load_settings() -> Settings

Without src/config.py the defaults below are used. Without src/network.py the
node refuses to start and says so.
"""

from __future__ import annotations

import importlib
import logging
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import FrameType
from typing import Protocol

from src.peers_storage import PeersStorage
from src.protocol import ProtocolHandler, Transport, is_valid_peer_address

logger = logging.getLogger(__name__)

BOOTSTRAP_PEERS = ("128.130.122.101:18018",)


@dataclass(frozen=True)
class Settings:
    # Interface to listen on. "0.0.0.0" means all interfaces; it is never advertised.
    bind_host: str = "0.0.0.0"
    # Listening port. The submission must listen on 18018.
    port: int = 18018
    # Name sent in our hello: printable ASCII, at most 128 characters.
    agent: str = "IntroToBlockchainProject 0.10.0"
    # Public "host:port" other nodes can reach us at, sent first in our peers
    # messages. None means we do not advertise ourselves.
    advertised_address: str | None = None
    # Connection policy for the networking layer.
    max_outgoing_connections: int = 8


class NetworkAdapter(Transport, Protocol):
    """What src/network.py must implement, in addition to Transport.send/close."""

    def run(self, handler: ProtocolHandler, initial_peers: list[str]) -> None:
        """Listen on settings.bind_host:settings.port, connect to some of initial_peers,
        drive the handler as described in src/protocol.py, and block until stop()."""
        ...

    def stop(self) -> None:
        """Close the listener and all connections. Safe to call more than once."""
        ...


class NetworkUnavailableError(RuntimeError):
    pass


def load_settings() -> Settings:
    try:
        config = importlib.import_module("src.config")
    except ModuleNotFoundError as error:
        if error.name != "src.config":
            raise  # config.py exists but one of its own imports is missing
        logger.warning("src/config.py not found; using default settings")
        return Settings()
    return config.load_settings()


def create_network_adapter(settings: Settings) -> NetworkAdapter:
    try:
        network = importlib.import_module("src.network")
    except ModuleNotFoundError as error:
        if error.name != "src.network":
            raise
        raise NetworkUnavailableError(
            "The networking layer is not implemented yet: src/network.py must define "
            "create_adapter(settings) -> NetworkAdapter (see src/main.py and src/protocol.py)."
        ) from None
    return network.create_adapter(settings)


def startup_candidates(saved_peers: list[str], advertised_address: str | None) -> list[str]:
    """Bootstrap peers first, then saved peers; no duplicates, no invalid entries, not ourselves."""
    candidates = dict.fromkeys([*BOOTSTRAP_PEERS, *saved_peers])
    return [peer for peer in candidates if peer != advertised_address and is_valid_peer_address(peer)]


def run_node(
    settings: Settings,
    network: NetworkAdapter,
    storage: type[PeersStorage] = PeersStorage,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Start the node and block until it is stopped or interrupted."""
    storage.init()
    candidates = startup_candidates(storage.read_all_peers(), settings.advertised_address)
    handler = ProtocolHandler(
        network,
        agent=settings.agent,
        advertised_address=settings.advertised_address,
        storage=storage,
        clock=clock,
    )

    logger.info(
        "Starting node on %s:%d with %d startup peer candidates",
        settings.bind_host, settings.port, len(candidates),
    )
    try:
        network.run(handler, candidates)
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    finally:
        network.stop()


def _raise_keyboard_interrupt(signum: int, frame: FrameType | None) -> None:
    raise KeyboardInterrupt


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # `docker compose down` sends SIGTERM; treat it like Ctrl+C so shutdown is clean.
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    settings = load_settings()
    try:
        network = create_network_adapter(settings)
    except NetworkUnavailableError as error:
        logger.error("%s", error)
        return 1

    run_node(settings, network)
    return 0


if __name__ == "__main__":
    sys.exit(main())
