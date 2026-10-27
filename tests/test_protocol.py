import json
import tempfile
import unittest
from pathlib import Path

from src.canonical_json_handler import CanonicalJSONHandler
from src.peers_storage import PeersStorage
from src.protocol import (
    HANDSHAKE_TIMEOUT_SECONDS,
    ProtocolHandler,
    select_peers_to_share,
)

OWN_ADDRESS = "mynode.example.com:18018"
HELLO = b'{"type":"hello","version":"0.10.0","agent":"Grader"}'


class FakeTransport:
    """Records send/close calls in order, decoding sent messages."""

    def __init__(self):
        self.events = []
        self.handler = None

    def send(self, conn_id, data):
        self.events.append(("send", conn_id, data))

    def close(self, conn_id):
        self.events.append(("close", conn_id))

    def messages(self, conn_id):
        return [json.loads(e[2]) for e in self.events if e[0] == "send" and e[1] == conn_id]

    def closed(self, conn_id):
        return ("close", conn_id) in self.events

    def clear(self):
        self.events.clear()


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class BrokenStorage(PeersStorage):
    @staticmethod
    def add_all_to_file(peers):
        raise RuntimeError("simulated bug")


class FailingStorage(PeersStorage):
    @staticmethod
    def add_all_to_file(peers):
        return False


class ProtocolTestCase(unittest.TestCase):
    def setUp(self):
        self._original_path = PeersStorage._file_path
        self.temp_dir = tempfile.TemporaryDirectory()
        PeersStorage._file_path = Path(self.temp_dir.name) / "peers.txt"
        PeersStorage.init()

        self.transport = FakeTransport()
        self.clock = FakeClock()
        self.handler = self._make_handler()

    def tearDown(self):
        PeersStorage._file_path = self._original_path
        self.temp_dir.cleanup()

    def _make_handler(self, storage=PeersStorage, advertised_address=OWN_ADDRESS):
        return ProtocolHandler(
            self.transport,
            agent="TestNode 0.10.0",
            advertised_address=advertised_address,
            storage=storage,
            clock=self.clock,
        )

    def _open_after_handshake(self, conn_id="a", outgoing_peer=None):
        self.handler.on_connection_opened(conn_id, outgoing_peer)
        self.handler.on_line(conn_id, HELLO)
        self.transport.clear()

    def _send_json(self, conn_id, message):
        self.handler.on_line(conn_id, json.dumps(message).encode())

    def assertRejected(self, conn_id, name):
        sends = [e for e in self.transport.events if e[1] == conn_id]
        self.assertEqual("send", sends[-2][0], "an error must be sent before closing")
        error = json.loads(sends[-2][2])
        self.assertEqual("error", error["type"])
        self.assertEqual(name, error["name"])
        self.assertIsInstance(error["msg"], str)
        self.assertEqual(("close", conn_id), sends[-1])
        self.assertEqual(1, self.transport.events.count(("close", conn_id)))

    def assertStillOpen(self, conn_id):
        self.assertFalse(self.transport.closed(conn_id))
        self.assertNotIn("error", [m["type"] for m in self.transport.messages(conn_id)])


class TestConnectionOpening(ProtocolTestCase):
    def test_sends_hello_then_getpeers_without_waiting_for_remote_hello(self):
        self.handler.on_connection_opened("a")

        self.assertEqual(
            [
                {"type": "hello", "version": "0.10.0", "agent": "TestNode 0.10.0"},
                {"type": "getpeers"},
            ],
            self.transport.messages("a"),
        )

    def test_sent_data_is_canonical_json_with_one_newline(self):
        self.handler.on_connection_opened("a")

        for _, _, data in self.transport.events:
            text = data.decode("utf-8")
            self.assertTrue(text.endswith("\n"))
            self.assertNotIn("\n", text[:-1])
            self.assertTrue(CanonicalJSONHandler.is_canonical_json(text[:-1]))

    def test_same_behaviour_for_outgoing_connections(self):
        self.handler.on_connection_opened("out", outgoing_peer="peer.example.com:18018")

        self.assertEqual(["hello", "getpeers"], [m["type"] for m in self.transport.messages("out")])

    def test_opening_same_connection_twice_is_a_programming_error(self):
        self.handler.on_connection_opened("a")
        with self.assertRaises(ValueError):
            self.handler.on_connection_opened("a")


class TestHandshake(ProtocolTestCase):
    def test_valid_hello_completes_handshake(self):
        self.handler.on_connection_opened("a")
        self.transport.clear()

        self.handler.on_line("a", HELLO)

        self.assertEqual([], self.transport.events)

    def test_non_canonical_hello_is_accepted(self):
        self.handler.on_connection_opened("a")
        self.handler.on_line("a", b'{ "agent" : "x",  "version":"0.10.9", "type":"hello" }\r')

        self.assertStillOpen("a")

    def test_message_before_hello(self):
        self.handler.on_connection_opened("a")
        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertRejected("a", "INVALID_HANDSHAKE")

    def test_later_task_message_before_hello(self):
        self.handler.on_connection_opened("a")
        self.handler.on_line("a", b'{"type":"getchaintip"}')

        self.assertRejected("a", "INVALID_HANDSHAKE")

    def test_second_hello(self):
        self._open_after_handshake("a")
        self.handler.on_line("a", HELLO)

        self.assertRejected("a", "INVALID_HANDSHAKE")

    def test_timeout_without_hello(self):
        self.handler.on_connection_opened("a")
        self.transport.clear()

        self.clock.now += HANDSHAKE_TIMEOUT_SECONDS - 0.1
        self.handler.check_timeouts()
        self.assertEqual([], self.transport.events)

        self.clock.now += 0.1
        self.handler.check_timeouts()
        self.assertRejected("a", "INVALID_HANDSHAKE")

        self.handler.check_timeouts()
        self.assertEqual(1, len([e for e in self.transport.events if e[0] == "send"]))

    def test_no_timeout_after_hello(self):
        self._open_after_handshake("a")

        self.clock.now += 1000
        self.handler.check_timeouts()

        self.assertEqual([], self.transport.events)

    def test_connections_have_independent_state(self):
        self._open_after_handshake("a")
        self.handler.on_connection_opened("b")
        self.transport.clear()

        self.handler.on_line("b", b'{"type":"getpeers"}')
        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertRejected("b", "INVALID_HANDSHAKE")
        self.assertEqual("peers", self.transport.messages("a")[0]["type"])
        self.assertStillOpen("a")


class TestInvalidFormat(ProtocolTestCase):
    # Examples from the Task 1 grader list, plus other malformed input.
    INVALID_LINES = [
        b"Wbgygvf7rgtyv7tfbgy{{{",
        b'{"type":"diufygeuybhv"}',
        b'{"type":"hello"}',
        b'{"type":"hello","version":"jd3.x","agent":"x"}',
        b'{"type":"hello","version":"5.8.2","agent":"x"}',
        b'{"type":"hello","version":"0.10.10","agent":"x"}',
        b'{"type":"hello","version":"0.10.0","agent":"x","extra":1}',
        b'{"type":"hello","version":"0.10.0","agent":"' + b"a" * 129 + b'"}',
        b'{"type":"hello","version":"0.10.0","agent":"tab\\there"}',
        b'{"type":"hello","version":"0.10.0","agent":5}',
        b'{"type":"hello","version":0.1,"agent":"x"}',
        b'"type":"hello"',
        b"[1, 2, 3]",
        b'{"kind":"hello"}',
        b'{"type":5}',
        b'{"type":"hello","version":"0.10.0","agent":NaN}',
        b"\xff\xfe",
        b"[" * 100000,
    ]

    def test_invalid_messages_before_handshake(self):
        for i, line in enumerate(self.INVALID_LINES):
            with self.subTest(line=line[:60]):
                self.handler.on_connection_opened(i)
                self.handler.on_line(i, line)
                self.assertRejected(i, "INVALID_FORMAT")

    def test_invalid_messages_after_handshake(self):
        for i, line in enumerate(self.INVALID_LINES):
            with self.subTest(line=line[:60]):
                self._open_after_handshake(i)
                self.handler.on_line(i, line)
                self.assertRejected(i, "INVALID_FORMAT")

    def test_getpeers_with_extra_key(self):
        self._open_after_handshake("a")
        self.handler.on_line("a", b'{"type":"getpeers","peers":[]}')

        self.assertRejected("a", "INVALID_FORMAT")

    def test_blank_lines_are_ignored(self):
        self._open_after_handshake("a")
        self.handler.on_line("a", b"")
        self.handler.on_line("a", b"\r")

        self.assertEqual([], self.transport.events)


class TestPeers(ProtocolTestCase):
    def test_getpeers_reply_starts_with_own_address(self):
        PeersStorage.add_all_to_file(["a.example.com:18018", "1.1.1.1:18018"])
        self._open_after_handshake("a")

        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertEqual(
            [{"type": "peers", "peers": [OWN_ADDRESS, "a.example.com:18018", "1.1.1.1:18018"]}],
            self.transport.messages("a"),
        )

    def test_getpeers_reply_without_advertised_address(self):
        self.handler = self._make_handler(advertised_address=None)
        PeersStorage.add_to_file("a.example.com:18018")
        self._open_after_handshake("a")

        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertEqual(["a.example.com:18018"], self.transport.messages("a")[0]["peers"])

    def test_invalid_stored_entries_are_not_shared(self):
        PeersStorage.add_all_to_file(["host:port", "192.168.0.2:18018", "ok.example.com:1"])
        self._open_after_handshake("a")

        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertEqual([OWN_ADDRESS, "ok.example.com:1"], self.transport.messages("a")[0]["peers"])

    def test_received_peers_are_returned_after_reconnect(self):
        peers = ["kermanode.net:18017", "138.197.191.170:18018"]
        self._open_after_handshake("a")
        self._send_json("a", {"type": "peers", "peers": peers})
        self.handler.on_connection_closed("a")

        self._open_after_handshake("b")
        self.handler.on_line("b", b'{"type":"getpeers"}')

        reply = self.transport.messages("b")[0]["peers"]
        for peer in peers:
            self.assertIn(peer, reply)
        self.assertStillOpen("a")

    def test_duplicates_and_own_address_are_not_stored_twice(self):
        self._open_after_handshake("a")
        self._send_json("a", {"type": "peers", "peers": ["x.example.com:1", "x.example.com:1", OWN_ADDRESS]})
        self._send_json("a", {"type": "peers", "peers": ["x.example.com:1"]})

        self.assertEqual(["x.example.com:1"], PeersStorage.read_all_peers())

    def test_empty_peers_list_is_valid(self):
        self._open_after_handshake("a")
        self._send_json("a", {"type": "peers", "peers": []})

        self.assertStillOpen("a")

    def test_invalid_peers_messages(self):
        invalid = [
            {"type": "peers", "peers": ["ok.example.com:18018", "256.2.3.4:18018"]},
            {"type": "peers", "peers": ["1.2.3.4.5:678"]},
            {"type": "peers", "peers": ["1.2.3.4:2000000"]},
            {"type": "peers", "peers": ["192.168.0.2:18018"]},
            {"type": "peers", "peers": ["nodotindomain:1234"]},
            {"type": "peers", "peers": ["kermanode.net"]},
            {"type": "peers", "peers": [".kermanode:1"]},
            {"type": "peers", "peers": ["kermanode.net:١٨"]},
            {"type": "peers", "peers": ["ab.c١:18018"]},
            {"type": "peers", "peers": [f"h{i}.example.com:1" for i in range(31)]},
            {"type": "peers", "peers": [5]},
            {"type": "peers", "peers": "kermanode.net:18017"},
            {"type": "peers"},
            {"type": "peers", "peers": [], "extra": True},
        ]
        for i, message in enumerate(invalid):
            with self.subTest(message=str(message)[:80]):
                self._open_after_handshake(i)
                self._send_json(i, message)
                self.assertRejected(i, "INVALID_FORMAT")

        # Validation happens before any effect: nothing from these messages is stored.
        self.assertEqual([], PeersStorage.read_all_peers())

    def test_thirty_peers_are_accepted(self):
        self._open_after_handshake("a")
        self._send_json("a", {"type": "peers", "peers": [f"h{i}.example.com:1" for i in range(30)]})

        self.assertStillOpen("a")
        self.assertEqual(30, len(PeersStorage.read_all_peers()))

    def test_storage_failure_does_not_blame_the_peer(self):
        self.handler = self._make_handler(storage=FailingStorage)
        self._open_after_handshake("a")

        with self.assertLogs("src.protocol", level="ERROR"):
            self._send_json("a", {"type": "peers", "peers": ["x.example.com:1"]})

        self.assertStillOpen("a")


class TestSelectPeersToShare(unittest.TestCase):
    def test_own_address_first(self):
        self.assertEqual([OWN_ADDRESS, "a.b.c:1"], select_peers_to_share(["a.b.c:1"], OWN_ADDRESS))

    def test_exactly_thirty_known_peers_are_all_sent(self):
        known = [f"h{i}.example.com:1" for i in range(30)]
        self.assertEqual(known, select_peers_to_share(known, OWN_ADDRESS))

    def test_more_than_thirty_sends_own_and_29_newest(self):
        known = [f"h{i}.example.com:1" for i in range(35)]
        self.assertEqual([OWN_ADDRESS, *known[-29:]], select_peers_to_share(known, OWN_ADDRESS))
        self.assertEqual(known[-30:], select_peers_to_share(known, None))


class TestOtherMessagesAndClosing(ProtocolTestCase):
    def test_later_task_messages_are_ignored_after_handshake(self):
        self._open_after_handshake("a")
        for line in [
            b'{"type":"getchaintip"}',
            b'{"type":"getmempool"}',
            b'{"type":"getobject","objectid":"00"}',
            b'{"type":"error","name":"INVALID_FORMAT","msg":"x"}',
        ]:
            self.handler.on_line("a", line)

        self.assertEqual([], self.transport.events)

    def test_nothing_is_handled_after_closing(self):
        self.handler.on_connection_opened("a")
        self.handler.on_line("a", b"garbage")
        events_after_reject = list(self.transport.events)

        self.handler.on_line("a", HELLO)
        self.handler.on_line("a", b"more garbage")
        self.clock.now += 100
        self.handler.check_timeouts()
        self.handler.on_connection_closed("a")
        self.handler.on_line("a", b'{"type":"getpeers"}')

        self.assertEqual(events_after_reject, self.transport.events)

    def test_unknown_connection_is_ignored(self):
        self.handler.on_line("nobody", b'{"type":"getpeers"}')
        self.assertEqual([], self.transport.events)

    def test_close_that_reports_back_synchronously(self):
        # A transport may call on_connection_closed from inside close().
        self.transport.close = lambda conn_id: self.handler.on_connection_closed(conn_id)
        self.handler.on_connection_opened("a")

        self.handler.on_line("a", b"garbage")
        self.handler.on_line("a", HELLO)

    def test_faulty_outgoing_peer_is_forgotten(self):
        PeersStorage.add_all_to_file(["bad.example.com:18018", "good.example.com:18018"])
        self._open_after_handshake("a", outgoing_peer="bad.example.com:18018")

        self.handler.on_line("a", b"garbage")

        self.assertEqual(["good.example.com:18018"], PeersStorage.read_all_peers())

    def test_faulty_incoming_peer_does_not_change_storage(self):
        PeersStorage.add_to_file("good.example.com:18018")
        self._open_after_handshake("a")

        self.handler.on_line("a", b"garbage")

        self.assertEqual(["good.example.com:18018"], PeersStorage.read_all_peers())

    def test_unreachable_peer_is_forgotten(self):
        PeersStorage.add_to_file("down.example.com:18018")

        self.handler.on_connect_failed("down.example.com:18018")

        self.assertEqual([], PeersStorage.read_all_peers())

    def test_internal_error_closes_connection_without_blaming_peer(self):
        self.handler = self._make_handler(storage=BrokenStorage)
        self._open_after_handshake("a")

        with self.assertLogs("src.protocol", level="ERROR"):
            self._send_json("a", {"type": "peers", "peers": ["x.example.com:1"]})

        self.assertEqual([("close", "a")], self.transport.events)


class TestConstructor(unittest.TestCase):
    def test_rejects_invalid_own_values(self):
        for kwargs in [
            {"agent": "x" * 129},
            {"agent": "bad\nagent"},
            {"agent": "ok", "advertised_address": "0.0.0.0:18018"},
            {"agent": "ok", "advertised_address": "myDns:18018"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                ProtocolHandler(FakeTransport(), **kwargs)


if __name__ == "__main__":
    unittest.main()
