import tempfile
import unittest
from pathlib import Path

from src.peers_storage import PeersStorage


class TestPeersStorage(unittest.TestCase):
    def _read_actual_file_content(self):
        with open(Path(self.temp_dir.name) / "peers.txt", "r", encoding="utf-8") as file:
            actual = file.read()
        return actual

    def _write_actual_file_content(self, content):
        with open(Path(self.temp_dir.name) / "peers.txt", "w", encoding="utf-8") as file:
            file.write(content)

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_file = Path(self.temp_dir.name) / "peers.txt"
        self.test_file.touch()

        PeersStorage._file_path = self.test_file

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_add_to_file(self):
        peer = "host:port"
        success = PeersStorage.add_to_file(peer)
        self.assertTrue(success)

        actual = TestPeersStorage._read_actual_file_content(self)
        self.assertEqual(peer, actual.splitlines()[0])

    def test_add_all_to_file(self):
        peers = ["host:port", "host2:port2", "host3:port3", ]
        success = PeersStorage.add_all_to_file(peers)
        self.assertTrue(success)

        actual = TestPeersStorage._read_actual_file_content(self)
        self.assertEqual(peers[0], actual.splitlines()[0])
        self.assertEqual(peers[1], actual.splitlines()[1])
        self.assertEqual(peers[2], actual.splitlines()[2])

    def test_remove_from_file(self):
        TestPeersStorage._write_actual_file_content(self, "host:port\nhost2:port2\nhost3:port3\n")
        peer = "host:port"
        success = PeersStorage.remove_from_file(peer)
        self.assertTrue(success)

        actual = TestPeersStorage._read_actual_file_content(self)
        self.assertNotEqual(peer, actual.splitlines()[0])
        self.assertEqual("host2:port2", actual.splitlines()[0])
        self.assertEqual("host3:port3", actual.splitlines()[1])

    def test_remove_all_from_file(self):
        TestPeersStorage._write_actual_file_content(self, "host:port\nhost2:port2\nhost3:port3\n")
        peers = ["host3:port3", "host:port"]
        success = PeersStorage.remove_list_from_file(peers)
        self.assertTrue(success)

        actual = TestPeersStorage._read_actual_file_content(self)
        self.assertNotEqual(peers[0], actual.splitlines()[0])
        self.assertEqual("host2:port2", actual.splitlines()[0])

    def test_read_all_peers(self):
        TestPeersStorage._write_actual_file_content(self, "host:port\nhost2:port2\nhost3:port3\n")
        actual = PeersStorage.read_all_peers()
        self.assertEqual(["host:port", "host2:port2", "host3:port3"], actual)

    def test_get_30_peers(self):
        # Test with fewer than 30 peers
        peers = [
            "host:port",
            "host2:port2",
            "host3:port3",
            "host4:port4",
            "host5:port5",
        ]
        TestPeersStorage._write_actual_file_content(self, "\n".join(peers) + "\n")

        actual = PeersStorage.get_30_peers()

        self.assertEqual(5, len(actual))
        self.assertEqual(peers, actual)

        # Test with 35 peers
        peers = [f"host{i}.example.com:{8000 + i}" for i in range(1, 36)]
        TestPeersStorage._write_actual_file_content(self, "\n".join(peers) + "\n")

        actual = PeersStorage.get_30_peers()

        self.assertEqual(30, len(actual))
        self.assertEqual(peers[-30:], actual)
        self.assertIn(peers[-1], actual)  # Newest peer is included

    def test_get_30_peers_with_own_peer(self):
        peers = [
            "host:port",
            "host2:port2",
            "host3:port3",
            "host4:port4",
            "host5:port5",
        ]
        TestPeersStorage._write_actual_file_content(self, "\n".join(peers) + "\n")
        actual = PeersStorage.get_30_peers_with_own_peer()
        self.assertEqual(6, len(actual))
        self.assertEqual(["myDns.host:18018", *peers], actual)

    def test_is_valid_peer(self):
        _input = "256.2.3.4:18018"
        self.assertFalse(PeersStorage.is_valid_peer(_input))

        _input = "1.2.3.4.5:678"
        self.assertFalse(PeersStorage.is_valid_peer(_input))

        _input = "1.2.3.4:2000000"
        self.assertFalse(PeersStorage.is_valid_peer(_input))

        _input = "192.168.0.2:18018"
        self.assertFalse(PeersStorage.is_valid_peer(_input))

        _input = "kermanode.net"
        self.assertFalse(PeersStorage.is_valid_peer(_input))

        _input = "kermanode.net:18017"
        self.assertTrue(PeersStorage.is_valid_peer(_input))

        _input = "138.197.191.170:18018"
        self.assertTrue(PeersStorage.is_valid_peer(_input))

    def test_own_peer_str(self):
        expected = "myDns.host:18018"

        actual = PeersStorage.own_peer_str()

        self.assertEqual(expected, actual)
        pass
