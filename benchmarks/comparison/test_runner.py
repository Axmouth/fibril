import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import run
import table


class SettlementTests(unittest.TestCase):
    def test_rabbit_stale_zero_is_not_completion(self):
        result = {"counts": {"issued": 100}}
        state = {"type": "quorum", "durable": True, "members": ["one"],
                 "messages_ready": 0, "messages_unacknowledged": 0}
        with patch.object(run, "http", return_value=state):
            self.assertIsNone(run.verify_server("rabbitmq", "", "bench", result))
            state["message_stats"] = {"ack": 100, "deliver": 100}
            self.assertEqual(run.verify_server("rabbitmq", "", "bench", result), state)
            state["messages_unacknowledged"] = 1
            self.assertIsNone(run.verify_server("rabbitmq", "", "bench", result))
            state["members"] = ["one", "two"]
            with self.assertRaises(RuntimeError):
                run.verify_server("rabbitmq", "", "bench", result)

    def test_fibril_frontier_must_cover_workload(self):
        state = {"ready_count": 0, "inflight_count": 0, "settled_until": 99}
        with patch.object(run, "http", return_value={"queues": [{"topic": "bench", "state": state}]}):
            self.assertIsNone(run.verify_server("fibril", "", "bench", {"counts": {"issued": 100}}))
            state["settled_until"] = 100
            self.assertIsNotNone(run.verify_server("fibril", "", "bench", {"counts": {"issued": 100}}))

    def test_empty_nats_stream_must_have_final_sequence(self):
        state = {"messages": 0, "last_seq": 0}
        response = {"account_details": [{"stream_detail": [{"name": "bench", "state": state}]}]}
        with patch.object(run, "http", return_value=response):
            self.assertIsNone(run.verify_server("nats", "", "bench", {"counts": {"issued": 100}}))
            state["last_seq"] = 100
            self.assertIsNotNone(run.verify_server("nats", "", "bench", {"counts": {"issued": 100}}))

    def test_missing_resources_stay_unavailable(self):
        self.assertEqual(run.summarize_resources([{"broker": {"unavailable": "no cgroup"}}]), {"available": False})

    def test_failure_cannot_become_result_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/"provenance.json").write_text(json.dumps({"clock_ticks_per_second": 100}))
            case = root/"00-nats"; case.mkdir()
            (case/"result.json").write_text(json.dumps({"status":"failed"}))
            (case/"failure.json").write_text(json.dumps({"error":"drain timed out"}))
            self.assertEqual(table.render(root), 0)
            self.assertIn("drain timed out", (root/"TABLE.md").read_text())


if __name__ == "__main__":
    unittest.main()
