"""TCP networking for our Kerma node."""

from __future__ import annotations

import errno
import logging
import selectors
import socket
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from concurrent.futures import Future, ThreadPoolExecutor

from src.peers_storage import PeersStorage
from src.protocol import ProtocolHandler, is_valid_peer_address

if TYPE_CHECKING:
    from src.main import Settings

logger = logging.getLogger(__name__)


@dataclass
class Connection:
    """State belonging to one TCP connection."""

    sock: socket.socket
    outgoing_peer: str | None = None

    # Incoming data waiting for a complete newline-delimited message.
    receive_buffer: bytearray = field(default_factory=bytearray)

    # Outgoing data waiting to be written to the socket.
    send_buffer: bytearray = field(default_factory=bytearray)

    # True while an outgoing TCP connection is being established.
    connecting: bool = False

    # True when queued messages must be sent before disconnecting.
    closing: bool = False

    # Used to enforce connection and closing deadlines.
    connect_deadline: float = 0.0
    close_deadline: float = 0.0


class TCPNetworkAdapter:
    """Manage TCP connections using one selector loop."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.selector = selectors.DefaultSelector()
        self.connections: dict[socket.socket, Connection] = {}
        self.handler: ProtocolHandler | None = None
        self.listener: socket.socket | None = None
        self.running = False
        self.resolver = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="peer-dns",
        )
        self.pending_resolutions: dict[str, Future[str]] = {}
        self.startup_peers: list[str] = []
        self.retry_after: dict[str, float] = {}

    def send(self, conn_id: socket.socket, data: bytes) -> None:
        """Queue already-encoded bytes without changing them."""
        connection = self.connections.get(conn_id)

        if connection is None or connection.closing:
            return

        connection.send_buffer.extend(data)
        self._update_interest(connection)

    def close(self, conn_id: socket.socket) -> None:
        """Send queued bytes before closing this connection."""
        connection = self.connections.get(conn_id)

        if connection is None or connection.closing:
            return

        connection.closing = True
        connection.close_deadline = time.monotonic() + 5.0
        self._update_interest(connection)

    def _update_interest(self, connection: Connection) -> None:
        """Tell the selector which socket events we need."""
        if connection.closing and not connection.send_buffer:
            self._disconnect(connection)
            return

        if connection.connecting:
            events = selectors.EVENT_WRITE
        else:
            events = 0

            if not connection.closing:
                events |= selectors.EVENT_READ

            if connection.send_buffer:
                events |= selectors.EVENT_WRITE

        self.selector.modify(connection.sock, events, connection)

    def _disconnect(self, connection: Connection) -> None:
        """Release one socket and notify the protocol handler."""
        sock = connection.sock

        if self.connections.pop(sock, None) is None:
            return

        try:
            self.selector.unregister(sock)
        except KeyError:
            pass

        sock.close()
        if connection.outgoing_peer is not None:
            self.retry_after[connection.outgoing_peer] = (
                time.monotonic() + 30.0
            )

        if self.handler is not None:
            self.handler.on_connection_closed(sock)

    def stop(self) -> None:
        """Release the listener and all connections."""
        self.running = False

        for future in self.pending_resolutions.values():
            future.cancel()

        self.pending_resolutions.clear()
        self.resolver.shutdown(wait=False, cancel_futures=True)

        for connection in list(self.connections.values()):
            self._disconnect(connection)

        if self.listener is not None:
            try:
                self.selector.unregister(self.listener)
            except KeyError:
                pass

            self.listener.close()
            self.listener = None

        self.selector.close()


    def _start_listener(self) -> None:
        """Start accepting TCP connections on our configured port."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.setblocking(False)
            listener.bind((self.settings.bind_host, self.settings.port))
            listener.listen(128)

            # None identifies the listener in selector events.
            self.selector.register(
                listener,
                selectors.EVENT_READ,
                data=None,
            )
        except Exception:
            listener.close()
            raise

        self.listener = listener

        logger.info(
            "Listening on %s:%d",
            self.settings.bind_host,
            self.settings.port,
        )

    def _accept_connections(self) -> None:
        """Accept waiting connections and start their handshakes."""
        if self.listener is None or self.handler is None:
            return

        # Limit each batch so existing connections also get processed.
        for _ in range(32):
            try:
                sock, remote_address = self.listener.accept()
            except BlockingIOError:
                return
            except OSError:
                logger.exception("Could not accept an incoming connection")
                return

            connection = Connection(sock=sock)

            try:
                sock.setblocking(False)
                self.selector.register(
                    sock,
                    selectors.EVENT_READ,
                    connection,
                )
                self.connections[sock] = connection

                logger.info("Incoming connection from %s", remote_address)

                # The protocol handler queues hello, then getpeers.
                self.handler.on_connection_opened(
                    sock,
                    outgoing_peer=None,
                )
            except Exception:
                logger.exception("Could not initialize incoming connection")

                if sock in self.connections:
                    self._disconnect(connection)
                else:
                    try:
                        self.selector.unregister(sock)
                    except KeyError:
                        pass
                    sock.close()


    def _read(self, connection: Connection) -> None:
        """Receive bytes and pass complete lines to the protocol handler."""
        if connection.closing or self.handler is None:
            return

        try:
            data = connection.sock.recv(65536)
        except BlockingIOError:
            return
        except OSError:
            logger.debug("Socket read failed", exc_info=True)
            self._disconnect(connection)
            return

        if not data:
            # The peer finished sending. Flush any queued reply, then close.
            self.close(connection.sock)
            return

        connection.receive_buffer.extend(data)

        while not connection.closing:
            newline_position = connection.receive_buffer.find(b"\n")

            if newline_position == -1:
                # Keep this partial message until more bytes arrive.
                break

            line = bytes(connection.receive_buffer[:newline_position])
            del connection.receive_buffer[:newline_position + 1]

            self.handler.on_line(connection.sock, line)

            # Handling the message may have closed this connection.
            if connection.sock not in self.connections:
                return

    def _write(self, connection: Connection) -> None:
        """Write queued bytes, preserving any bytes not yet sent."""
        if connection.connecting:
            return

        if connection.send_buffer:
            try:
                sent = connection.sock.send(connection.send_buffer)
            except BlockingIOError:
                return
            except OSError:
                logger.debug("Socket write failed", exc_info=True)
                self._disconnect(connection)
                return

            if sent == 0:
                self._disconnect(connection)
                return

            del connection.send_buffer[:sent]

        self._update_interest(connection)


    def _connect(self, peer: str, ipv4_address: str) -> None:
        """Start connecting to a peer using its already-resolved IPv4 address."""
        port = int(peer.rsplit(":", 1)[1])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        connection = Connection(
            sock=sock,
            outgoing_peer=peer,
            connecting=True,
            connect_deadline=time.monotonic() + 10.0,
        )

        try:
            sock.setblocking(False)
            self.selector.register(
                sock,
                selectors.EVENT_WRITE,
                connection,
            )
            self.connections[sock] = connection

            result = sock.connect_ex((ipv4_address, port))
        except OSError:
            logger.debug("Connection attempt failed for %s", peer, exc_info=True)

            if sock not in self.connections:
                sock.close()

            self._connection_failed(connection)
            return

        pending_errors = {
            errno.EINPROGRESS,
            errno.EWOULDBLOCK,
            errno.EALREADY,
            errno.EINTR,
            # Windows socket error codes.
            10035,
            10036,
            10037,
        }

        if result == 0:
            self._connection_established(connection)
        elif result not in pending_errors:
            self._connection_failed(connection)

    def _finish_connect(self, connection: Connection) -> None:
        """Check whether a pending TCP connection succeeded."""
        try:
            error = connection.sock.getsockopt(
                socket.SOL_SOCKET,
                socket.SO_ERROR,
            )
        except OSError:
            self._connection_failed(connection)
            return

        if error:
            self._connection_failed(connection)
            return

        self._connection_established(connection)

    def _connection_established(self, connection: Connection) -> None:
        """Begin the protocol handshake after TCP connects."""
        connection.connecting = False
        self._update_interest(connection)

        logger.info("Connected to %s", connection.outgoing_peer)

        if self.handler is not None:
            try:
                self.handler.on_connection_opened(
                    connection.sock,
                    outgoing_peer=connection.outgoing_peer,
                )
            except Exception:
                logger.exception("Could not initialize outgoing connection")
                self._disconnect(connection)

    def _connection_failed(self, connection: Connection) -> None:
        """Release a failed connection and report its peer."""
        peer = connection.outgoing_peer
        self._disconnect(connection)

        if peer is not None and self.handler is not None:
            logger.info("Could not connect to %s", peer)

            try:
                self.handler.on_connect_failed(peer)
            except Exception:
                logger.exception("Could not process failed peer %s", peer)

    @staticmethod
    def _resolve_ipv4(peer: str) -> str:
        """Resolve a peer address in a background worker."""
        host, port_text = peer.rsplit(":", 1)

        addresses = socket.getaddrinfo(
            host,
            int(port_text),
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )

        if not addresses:
            raise OSError(f"No IPv4 address found for {host}")

        return addresses[0][4][0]

    def _start_resolution(self, peer: str) -> bool:
        """Schedule a lookup if this peer is valid and a slot is available."""
        if not is_valid_peer_address(peer):
            return False

        if peer == self.settings.advertised_address:
            return False

        if peer in self.pending_resolutions:
            return False

        outgoing_peers = {
            connection.outgoing_peer
            for connection in self.connections.values()
            if connection.outgoing_peer is not None
        }

        if peer in outgoing_peers:
            return False

        occupied_slots = len(outgoing_peers) + len(self.pending_resolutions)

        if occupied_slots >= self.settings.max_outgoing_connections:
            return False

        self.pending_resolutions[peer] = self.resolver.submit(
            self._resolve_ipv4,
            peer,
        )
        return True

    def _process_resolutions(self) -> None:
        """Handle finished lookups from the networking thread."""
        for peer, future in list(self.pending_resolutions.items()):
            if not future.done():
                continue

            del self.pending_resolutions[peer]

            try:
                ipv4_address = future.result()
            except Exception:
                logger.warning("Could not resolve peer %s", peer)

                if self.handler is not None:
                    try:
                        self.handler.on_connect_failed(peer)
                    except Exception:
                        logger.exception(
                            "Could not process DNS failure for %s",
                            peer,
                        )
                continue

            self._connect(peer, ipv4_address)


    def _maintain_connections(self) -> None:
        """Consider startup peers and peers discovered while running."""
        try:
            saved_peers = PeersStorage.read_all_peers()
        except OSError:
            logger.exception("Could not read peers for connection discovery")
            saved_peers = []

        candidates = dict.fromkeys([*self.startup_peers, *saved_peers])
        now = time.monotonic()

        for peer in candidates:
            if not is_valid_peer_address(peer):
                if peer in self.startup_peers:
                    self.startup_peers.remove(peer)
                continue

            if peer == self.settings.advertised_address:
                if peer in self.startup_peers:
                    self.startup_peers.remove(peer)
                continue

            if now < self.retry_after.get(peer, 0.0):
                continue

            if self._start_resolution(peer):
                self.retry_after[peer] = now + 30.0

                # Startup candidates are attempted once. Further attempts
                # depend on whether the peer remains in persistent storage.
                if peer in self.startup_peers:
                    self.startup_peers.remove(peer)

    def _check_connection_deadlines(self) -> None:
        """Expire unfinished TCP connections and stalled closing queues."""
        now = time.monotonic()

        for connection in list(self.connections.values()):
            if connection.connecting:
                if now >= connection.connect_deadline:
                    self._connection_failed(connection)
            elif connection.closing:
                if now >= connection.close_deadline:
                    self._disconnect(connection)

    def run(
        self,
        handler: ProtocolHandler,
        initial_peers: list[str],
    ) -> None:
        """Listen, connect to peers, and process socket events."""
        self.handler = handler
        self.startup_peers = list(dict.fromkeys(initial_peers))
        self.running = True
        next_discovery = 0.0

        try:
            self._start_listener()

            while self.running:
                self._process_resolutions()

                now = time.monotonic()

                if now >= next_discovery:
                    self._maintain_connections()
                    next_discovery = now + 1.0

                # Check handshake deadlines before processing new messages.
                handler.check_timeouts()
                self._check_connection_deadlines()

                events = self.selector.select(timeout=0.1)

                for key, mask in events:
                    if not self.running:
                        break

                    if key.data is None:
                        self._accept_connections()
                        continue

                    connection = key.data

                    # An earlier callback may have removed this connection.
                    if connection.sock not in self.connections:
                        continue

                    if connection.connecting:
                        self._finish_connect(connection)
                        continue

                    if mask & selectors.EVENT_READ:
                        self._read(connection)

                    if connection.sock not in self.connections:
                        continue

                    if mask & selectors.EVENT_WRITE:
                        self._write(connection)
        finally:
            self.stop()


def create_adapter(settings: Settings) -> TCPNetworkAdapter:
    """Factory used by main.py."""
    return TCPNetworkAdapter(settings)