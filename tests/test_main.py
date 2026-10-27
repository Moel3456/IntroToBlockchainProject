import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src import main as main_module
from src.main import (
    BOOTSTRAP_PEERS,
    NetworkUnavailableError,
    Settings,
    create_network_adapter,
    run_node,
    startup_candidates,
)
from src.peers_storage import PeersStorage
from src.protocol import ProtocolHandler

REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeNetwork:
    def __init__(self, run_error=None):
        self.run_error = run_error
        self.run_calls = []
        self.stop_calls = 0

    def send(self, conn_id, data):
        pass

    def close(self, conn_id):
        pass

    def run(self, handler, initial_peers):
        self.run_calls.append((handler, initial_peers))
        if self.run_error is not None:
            raise self.run_error

    def stop(self):
        self.stop_calls += 1


class TestStartupCandidates(unittest.TestCase):
    def test_bootstrap_first_and_no_duplicates(self):
        saved = ["a.example.com:18018", "128.130.122.101:18018", "a.example.com:18018"]

        self.assertEqual(
            ["128.130.122.101:18018", "a.example.com:18018"],
            startup_candidates(saved, None),
        )

    def test_skips_own_address_and_invalid_entries(self):
        saved = ["me.example.com:18018", "host:port", "10.0.0.1:18018", "b.example.com:1"]

        self.assertEqual(
            [*BOOTSTRAP_PEERS, "b.example.com:1"],
            startup_candidates(saved, "me.example.com:18018"),
        )


class TestRunNode(unittest.TestCase):
    def setUp(self):
        self._original_path = PeersStorage._file_path
        self.temp_dir = tempfile.TemporaryDirectory()
        # A path whose folder does not exist yet, to check that run_node initialises storage.
        PeersStorage._file_path = Path(self.temp_dir.name) / "data" / "peers.txt"

    def tearDown(self):
        PeersStorage._file_path = self._original_path
        self.temp_dir.cleanup()

    def test_loads_saved_peers_and_starts_network(self):
        PeersStorage.init()
        PeersStorage.add_to_file("saved.example.com:18018")
        network = FakeNetwork()

        run_node(Settings(), network)

        handler, candidates = network.run_calls[0]
        self.assertIsInstance(handler, ProtocolHandler)
        self.assertEqual([*BOOTSTRAP_PEERS, "saved.example.com:18018"], candidates)
        self.assertEqual(1, network.stop_calls)

    def test_initialises_storage(self):
        run_node(Settings(), FakeNetwork())

        self.assertTrue(PeersStorage._file_path.exists())

    def test_interrupt_stops_network(self):
        network = FakeNetwork(run_error=KeyboardInterrupt())

        run_node(Settings(), network)

        self.assertEqual(1, network.stop_calls)

    def test_network_failure_still_stops_network(self):
        network = FakeNetwork(run_error=OSError("port in use"))

        with self.assertRaises(OSError):
            run_node(Settings(), network)
        self.assertEqual(1, network.stop_calls)

    def test_invalid_advertised_address_is_refused(self):
        with self.assertRaises(ValueError):
            run_node(Settings(advertised_address="0.0.0.0:18018"), FakeNetwork())


class TestMissingModules(unittest.TestCase):
    def test_network_adapter_unavailable(self):
        with self.assertRaises(NetworkUnavailableError):
            create_network_adapter(Settings())

    def test_main_fails_clearly_without_network(self):
        with mock.patch("logging.basicConfig"), mock.patch("signal.signal"), \
                mock.patch.object(main_module, "run_node") as run_node_mock, \
                self.assertLogs("src.main", level="ERROR") as logs:
            exit_code = main_module.main()

        self.assertEqual(1, exit_code)
        run_node_mock.assert_not_called()
        self.assertIn("src/network.py", "\n".join(logs.output))

    def test_default_settings_without_config(self):
        with self.assertLogs("src.main", level="WARNING"):
            settings = main_module.load_settings()

        self.assertEqual(Settings(), settings)
        self.assertEqual(18018, settings.port)


class TestImport(unittest.TestCase):
    def test_import_has_no_side_effects(self):
        code = (
            "import sys\n"
            "from src.peers_storage import PeersStorage\n"
            "def fail(*args): raise AssertionError('storage touched on import')\n"
            "PeersStorage.init = staticmethod(fail)\n"
            "PeersStorage.read_all_peers = staticmethod(fail)\n"
            "import src.main\n"
            "assert 'src.network' not in sys.modules\n"
            "assert 'src.config' not in sys.modules\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("", result.stdout)


if __name__ == "__main__":
    unittest.main()
