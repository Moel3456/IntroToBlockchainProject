"""Kerma protocol layer for Task 1: handshake, peer discovery and protocol errors.

This module knows the protocol rules but nothing about sockets. The networking
layer (a separate module) owns TCP, receive buffering and threads/event loops,
and talks to this module through the contract below.

Networking layer -> ProtocolHandler
    on_connection_opened(conn_id, outgoing_peer)
        Once per connection, right after connecting or accepting. outgoing_peer
        is the "host:port" we dialled, or None for an incoming connection (an
        incoming connection's source port is not its listening port).
    on_line(conn_id, line)
        Once per complete message: the raw bytes of one line, without the
        trailing b"\\n". The networking layer splits the byte stream on b"\\n"
        and never decodes or parses it.
    on_connection_closed(conn_id)
        Once, when the socket is gone (closed by either side).
    check_timeouts()
        Periodically, e.g. once per second. Enforces the 20 s hello deadline.
    on_connect_failed(peer)
        When an outgoing connection attempt to "host:port" fails.

ProtocolHandler -> networking layer (the Transport given to the constructor)
    send(conn_id, data)
        data is one complete message: canonical JSON, UTF-8 encoded, already
        ending in b"\\n". Write it unchanged, in call order.
    close(conn_id)
        Write everything already passed to send() for this connection, then
        close it. Errors are always sent with send() before close() is called.

Both Transport methods are called while this module holds its lock, so they
must not block for long (queue the data or use non-blocking writes) and must
not wait for another thread that is calling into this handler.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any, Protocol

from src.canonical_json_handler import CanonicalJSONHandler
from src.peers_storage import PeersStorage

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = "0.10.0"
HANDSHAKE_TIMEOUT_SECONDS = 20.0
MAX_PEERS_PER_MESSAGE = 30
MAX_AGENT_LENGTH = 128

INVALID_FORMAT = "INVALID_FORMAT"
INVALID_HANDSHAKE = "INVALID_HANDSHAKE"

# "0.10.x with x a single decimal digit"
_VERSION_PATTERN = re.compile(r"0\.10\.[0-9]")

# Protocol message types that Task 1 does not process. The task says we are
# not required to check their validity and must not close the connection.
_LATER_TASK_TYPES = frozenset({
    "getobject", "ihaveobject", "object", "getchaintip", "chaintip",
    "getmempool", "mempool", "error",
})

ConnectionId = Hashable


class Transport(Protocol):
    """What the networking layer provides to the protocol layer."""

    def send(self, conn_id: ConnectionId, data: bytes) -> None: ...

    def close(self, conn_id: ConnectionId) -> None: ...


class ProtocolViolation(Exception):
    """The remote peer broke the protocol. It gets an error message and is disconnected."""

    def __init__(self, name: str, msg: str) -> None:
        super().__init__(f"{name}: {msg}")
        self.name = name
        self.msg = msg


@dataclass
class ConnectionState:
    conn_id: ConnectionId
    outgoing_peer: str | None
    handshake_deadline: float
    hello_received: bool = False
    closing: bool = False


def is_printable_ascii(text: str) -> bool:
    return all(0x20 <= ord(char) <= 0x7E for char in text)


def is_valid_peer_address(peer: object) -> bool:
    """Check a "host:port" string against the rules in the protocol's Peers section."""
    # PeersStorage.is_valid_peer uses str.isdecimal() and the regex \d, which also
    # accept non-ASCII digits such as "١". Valid addresses are pure ASCII.
    return isinstance(peer, str) and peer.isascii() and PeersStorage.is_valid_peer(peer)


def select_peers_to_share(known_peers: list[str], own_address: str | None) -> list[str]:
    """Choose at most 30 peers for a "peers" message. known_peers is oldest first.

    Our own address goes first, followed by the most recently learned peers.
    The one exception: the spec requires every valid peer we learned to appear
    in our replies while we know at most 30, so with exactly 30 known peers we
    send all of them and leave our own address out.
    """
    if own_address is None or len(known_peers) == MAX_PEERS_PER_MESSAGE:
        return known_peers[-MAX_PEERS_PER_MESSAGE:]
    return [own_address, *known_peers[-(MAX_PEERS_PER_MESSAGE - 1):]]


def parse_message(line: bytes) -> dict[str, Any]:
    """Turn one received line into a validated message, or raise ProtocolViolation.

    Incoming JSON does not have to be canonical; any valid JSON is accepted.
    """
    try:
        message = json.loads(line.decode("utf-8"), parse_constant=_reject_constant)
    except (ValueError, RecursionError) as error:
        # ValueError covers both invalid UTF-8 and invalid JSON.
        raise ProtocolViolation(INVALID_FORMAT, f"Message is not valid JSON: {error}") from None

    if not isinstance(message, dict):
        raise ProtocolViolation(INVALID_FORMAT, "Message is not a JSON object")

    msg_type = message.get("type")
    if not isinstance(msg_type, str):
        raise ProtocolViolation(INVALID_FORMAT, 'Message has no string "type" key')

    if msg_type in _VALIDATORS:
        _VALIDATORS[msg_type](message)
    elif msg_type not in _LATER_TASK_TYPES:
        raise ProtocolViolation(INVALID_FORMAT, f"Unknown message type {_shorten(msg_type)}")
    return message


def _reject_constant(name: str) -> None:
    # json.loads accepts NaN and Infinity by default, but they are not valid JSON.
    raise ValueError(f"{name} is not valid JSON")


def _shorten(value: str, limit: int = 40) -> str:
    """Quote peer-supplied text for an error message without echoing huge strings."""
    return repr(value if len(value) <= limit else value[:limit] + "...")


def _require_exact_keys(message: dict[str, Any], expected: set[str]) -> None:
    keys = set(message)
    problems = []
    if missing := sorted(expected - keys):
        problems.append(f"missing keys {missing}")
    if extra := sorted(keys - expected):
        problems.append(f"unexpected keys [{', '.join(_shorten(key) for key in extra)}]")
    if problems:
        raise ProtocolViolation(INVALID_FORMAT, f'"{message["type"]}" message has {" and ".join(problems)}')


def _validate_hello(message: dict[str, Any]) -> None:
    _require_exact_keys(message, {"type", "version", "agent"})

    version = message["version"]
    if not isinstance(version, str) or not _VERSION_PATTERN.fullmatch(version):
        raise ProtocolViolation(INVALID_FORMAT, 'Version must have the form "0.10.x" with x a single digit')

    agent = message["agent"]
    if not isinstance(agent, str) or len(agent) > MAX_AGENT_LENGTH or not is_printable_ascii(agent):
        raise ProtocolViolation(INVALID_FORMAT, "Agent must be printable ASCII of at most 128 characters")


def _validate_getpeers(message: dict[str, Any]) -> None:
    _require_exact_keys(message, {"type"})


def _validate_peers(message: dict[str, Any]) -> None:
    _require_exact_keys(message, {"type", "peers"})

    peers = message["peers"]
    if not isinstance(peers, list):
        raise ProtocolViolation(INVALID_FORMAT, "peers must be a list")
    if len(peers) > MAX_PEERS_PER_MESSAGE:
        raise ProtocolViolation(INVALID_FORMAT, "peers must not contain more than 30 entries")
    for peer in peers:
        if not is_valid_peer_address(peer):
            shown = _shorten(peer) if isinstance(peer, str) else type(peer).__name__
            raise ProtocolViolation(INVALID_FORMAT, f"Invalid peer address {shown}")


_VALIDATORS: dict[str, Callable[[dict[str, Any]], None]] = {
    "hello": _validate_hello,
    "getpeers": _validate_getpeers,
    "peers": _validate_peers,
}


class ProtocolHandler:
    """Per-connection protocol state and message handling for Task 1.

    Safe to call from several threads: every entry point holds one lock.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        agent: str,
        advertised_address: str | None = None,
        storage: type[PeersStorage] = PeersStorage,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """
        agent: name sent in our hello.
        advertised_address: "host:port" where other nodes can reach us, sent
            first in our peers messages. None means we do not advertise ourselves.
        storage: peer storage, already initialised (PeersStorage.init()).
        clock: monotonic time in seconds; replaceable in tests.
        """
        if len(agent) > MAX_AGENT_LENGTH or not is_printable_ascii(agent):
            raise ValueError("agent must be printable ASCII of at most 128 characters")
        if advertised_address is not None and not is_valid_peer_address(advertised_address):
            raise ValueError(f"advertised_address {advertised_address!r} is not a valid public peer address")

        self._transport = transport
        self._agent = agent
        self._advertised_address = advertised_address
        self._storage = storage
        self._clock = clock
        self._connections: dict[ConnectionId, ConnectionState] = {}
        self._lock = threading.RLock()

    def on_connection_opened(self, conn_id: ConnectionId, outgoing_peer: str | None = None) -> None:
        with self._lock:
            if conn_id in self._connections:
                raise ValueError(f"Connection {conn_id!r} is already open")

            state = ConnectionState(conn_id, outgoing_peer, self._clock() + HANDSHAKE_TIMEOUT_SECONDS)
            self._connections[conn_id] = state
            # Our hello must come first; getpeers follows immediately, without
            # waiting for the remote hello.
            self._send(state, {"type": "hello", "version": PROTOCOL_VERSION, "agent": self._agent})
            self._send(state, {"type": "getpeers"})

    def on_line(self, conn_id: ConnectionId, line: bytes) -> None:
        with self._lock:
            state = self._connections.get(conn_id)
            if state is None or state.closing:
                return
            if (
                not state.hello_received
                and self._clock() >= state.handshake_deadline
            ):
                self._reject(
                    state,
                    ProtocolViolation(
                        INVALID_HANDSHAKE,
                        "No hello received within 20 seconds",
                    ),
                )
                return
            if not line.strip():
                return  # tolerate blank lines, e.g. from "\r\n" or "\n\n"

            try:
                message = parse_message(line)
                self._handle(state, message)
            except ProtocolViolation as violation:
                self._reject(state, violation)
            except Exception:
                # A bug or local failure, not the peer's fault: log it, drop
                # this connection only, and keep the peer in storage.
                logger.exception("Internal error while handling a message on %r", conn_id)
                self._close(state)

    def on_connection_closed(self, conn_id: ConnectionId) -> None:
        with self._lock:
            self._connections.pop(conn_id, None)

    def check_timeouts(self) -> None:
        with self._lock:
            now = self._clock()
            expired = [
                state for state in self._connections.values()
                if not state.closing and not state.hello_received and now >= state.handshake_deadline
            ]
            for state in expired:
                self._reject(state, ProtocolViolation(INVALID_HANDSHAKE, "No hello received within 20 seconds"))

    def on_connect_failed(self, peer: str) -> None:
        """Forget a peer we could not reach ("if you discover that a node is offline, you should forget it")."""
        with self._lock:
            self._forget(peer)

    def _handle(self, state: ConnectionState, message: dict[str, Any]) -> None:
        msg_type = message["type"]

        if not state.hello_received:
            if msg_type != "hello":
                raise ProtocolViolation(INVALID_HANDSHAKE, f'Received "{msg_type}" before "hello"')
            state.hello_received = True
            logger.info("Handshake completed with %r (agent %r)", state.conn_id, message["agent"])
            return

        if msg_type == "hello":
            raise ProtocolViolation(INVALID_HANDSHAKE, 'Received a second "hello"')
        if msg_type == "getpeers":
            self._send(state, {"type": "peers", "peers": self._peers_to_share()})
        elif msg_type == "peers":
            self._store_peers(message["peers"])
        elif msg_type == "error":
            logger.warning("%r reported error %r: %r", state.conn_id, message.get("name"), message.get("msg"))
        else:
            logger.debug("Ignoring %r message from %r (not part of Task 1)", msg_type, state.conn_id)

    def _peers_to_share(self) -> list[str]:
        try:
            stored = self._storage.read_all_peers()
        except OSError:
            logger.exception("Could not read stored peers")
            stored = []
        # Never send an invalid entry: the receiver would consider us faulty.
        known = [
            peer for peer in dict.fromkeys(stored)
            if peer != self._advertised_address and is_valid_peer_address(peer)
        ]
        return select_peers_to_share(known, self._advertised_address)

    def _store_peers(self, peers: list[str]) -> None:
        new_peers = [peer for peer in dict.fromkeys(peers) if peer != self._advertised_address]
        if not self._storage.add_all_to_file(new_peers):
            logger.error("Could not save discovered peers")

    def _forget(self, peer: str) -> None:
        if not self._storage.remove_from_file(peer):
            logger.error("Could not remove peer %s from storage", peer)

    def _reject(self, state: ConnectionState, violation: ProtocolViolation) -> None:
        logger.warning("Disconnecting %r: %s", state.conn_id, violation)
        self._send(state, {"type": "error", "name": violation.name, "msg": violation.msg})
        # Only peers we dialled can be forgotten reliably (see "Edge cases when
        # forgetting nodes" in the protocol description).
        if state.outgoing_peer is not None:
            self._forget(state.outgoing_peer)
        self._close(state)

    def _close(self, state: ConnectionState) -> None:
        if state.closing:
            return
        state.closing = True
        self._transport.close(state.conn_id)

    def _send(self, state: ConnectionState, message: dict[str, Any]) -> None:
        if state.closing:
            return
        line = CanonicalJSONHandler.to_canonical_json(message) + "\n"
        self._transport.send(state.conn_id, line.encode("utf-8"))
