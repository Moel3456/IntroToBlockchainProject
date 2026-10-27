import threading
import ipaddress
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class PeersStorage:
    """Handle peers storage. Make sure to call PeersStorage.init() once first."""
    _lock = threading.Lock()
    _file_path = PROJECT_ROOT / "data" / "peers.txt"

    @staticmethod
    def init() -> None:
        """Ensure the peers file exists."""
        PeersStorage._file_path.parent.mkdir(parents=True, exist_ok=True)
        PeersStorage._file_path.touch(exist_ok=True)

    @staticmethod
    def add_to_file(peer: str) -> bool:
        """Add peer to file."""
        try:
            with PeersStorage._lock:
                if PeersStorage._is_present_in_file_thread_unsafe(peer):
                    return True

                with open(PeersStorage._file_path, "a", encoding="utf-8") as file:
                    file.write(peer.strip() + "\n")
                    return True
        except OSError:
            return False

    @staticmethod
    def add_all_to_file(peers: list[str]) -> bool:
        """Add all peers to file."""
        for peer in peers:
            if not PeersStorage.add_to_file(peer):
                return False
        return True

    @staticmethod
    def remove_from_file(peer: str) -> bool:
        """Remove a peer from the file."""
        with PeersStorage._lock:
            try:
                peers = PeersStorage._read_all_peers_thread_unsafe()
            except OSError:
                return False

            if peer not in peers:
                return True

            peers = [p for p in peers if p != peer]

            # Overwrite old entries with updated list
            return PeersStorage._overwrite_peers_thread_unsafe(peers)

    @staticmethod
    def remove_list_from_file(peers_to_remove: list[str]) -> bool:
        """Remove all peers from file."""
        try:
            with PeersStorage._lock:
                peers = PeersStorage._read_all_peers_thread_unsafe()
                peers_to_remove = set(peers_to_remove)
                peers = [p for p in peers if p not in peers_to_remove]

                return PeersStorage._overwrite_peers_thread_unsafe(peers)
        except OSError:
            return False

    @staticmethod
    def read_all_peers() -> list[str]:
        """Read all peers from file. """
        with PeersStorage._lock:
            return PeersStorage._read_all_peers_thread_unsafe()

    @staticmethod
    def get_30_peers() -> list[str]:
        """Return 30 peers."""
        return PeersStorage.read_all_peers()[-30:]

    @staticmethod
    def get_30_peers_with_own_peer() -> list[str]:
        """Return own peer and the 29 most recently added peers."""
        return [PeersStorage.own_peer_str(), *PeersStorage.get_30_peers()[-29:]]

    @staticmethod
    def is_valid_peer(peer: str) -> bool:
        """Check if peer is valid."""
        if not isinstance(peer, str) or peer.count(":") != 1:
            return False

        host, port_str = peer.rsplit(":", 1)

        # Validate port
        if not port_str.isdecimal() or not 1 <= int(port_str) <= 65535:
            return False

        # Validate IPv4 syntax and global reachability
        parts = host.split(".")
        if len(parts) == 4 and all(part.isdecimal() for part in parts):
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                return False

            if address.version != 4 or not address.is_global:
                return False

            return True

        # Validate DNS entry
        if not re.fullmatch(r"[a-zA-Z\d._-]{3,50}", host):
            return False

        if "." not in host[1:-1]:
            return False

        if not re.search(r"[a-zA-Z]", host):
            return False

        return True

    @staticmethod
    def own_peer_str() -> str:
        """Return self peer string."""
        return "myDns.host:18018"

    @staticmethod
    def _is_present_in_file_thread_unsafe(peer: str) -> bool:
        """Check whether peer exists. Caller must hold the lock."""
        return peer in PeersStorage._read_all_peers_thread_unsafe()

    @staticmethod
    def _read_all_peers_thread_unsafe() -> list[str]:
        """Read all peers from file. Caller must hold the lock."""
        with open(PeersStorage._file_path, "r", encoding="utf-8") as file:
            return file.read().splitlines()

    @staticmethod
    def _overwrite_peers_thread_unsafe(peers: list[str]) -> bool:
        """Overwrite the peers file safely. Caller must hold the lock."""
        temp_path = PeersStorage._file_path.with_suffix(".tmp")

        try:
            with open(temp_path, "w", encoding="utf-8") as file:
                file.writelines(peer + "\n" for peer in peers)
                file.flush()

            temp_path.replace(PeersStorage._file_path)
            return True

        except OSError:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            return False
