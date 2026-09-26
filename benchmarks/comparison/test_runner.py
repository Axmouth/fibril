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

    def test_table_distinguishes_bursts_from_equal_average_smooth_traffic(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/"provenance.json").write_text(json.dumps({"clock_ticks_per_second": 100}))
            for name, pattern in [("00-smooth", []), ("01-bursts", [200, 100])]:
                case = root/name
                case.mkdir()
                (case/"placement.json").write_text(json.dumps({"mount": {"filesystems": [{"fstype": "ext4", "target": "/bench"}]}}))
                result = {"status": "validated", "config": {"warmup_secs": 6, "duration_secs": 60,
                    "payload_bytes": 1024, "rate": 100, "burst_pattern": pattern},
                    "contract": {"sync_policy": "durable"}, "cohort_completed_per_sec": 100,
                    "confirm": {"from_admission": {"p99_ms": 1}},
                    "delivery": {"from_admission": {"p99_ms": 2}, "from_schedule": {"p99_ms": 3},
                        "burst_delivery_complete_from_schedule": {"samples": 40, "p50_ms": 4, "p95_ms": 5, "p99_ms": 6}},
                    "broker_resources": {"available": False},
                    "timeline": [{"client": {"rss_kib": None, "cpu_ticks": None}}]}
                (case/"result.json").write_text(json.dumps(result))
            self.assertEqual(table.render(root), 2)
            report = (root/"TABLE.md").read_text()
            self.assertIn("| evenly spaced | 100 |", report)
            self.assertIn("| bursts 200,100 | 100 |", report)
            self.assertIn("| 01-bursts | 40 | 4.000 | 5.000 | 6.000 |", report)
            self.assertNotIn("| 00-smooth | 40 |", report)


if __name__ == "__main__":
    unittest.main()
