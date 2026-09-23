# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

import csv
import json

import pytest
from pyathena_bench.report import report, summarize


def test_summary_excludes_failures_and_warmups_but_reports_them():
    base = {
        "case_id": "test",
        "scale": "small",
        "status": "ok",
        "warmup": False,
        "queries": [{"total_seconds": 2}],
        "successful_queries_per_second": 0.5,
        "rss_peak_bytes": 100,
        "max_threads": 3,
        "loop_lag_seconds": [0.01],
    }
    trials = [
        base,
        {**base, "warmup": True, "queries": [{"total_seconds": 999}]},
        {**base, "status": "error", "queries": [{"status": "error", "error": "Access denied"}]},
        {
            "case_id": "unsupported",
            "scale": "small",
            "status": "unsupported",
            "reason": "Nested CSV",
        },
    ]
    rows = summarize(trials)
    assert rows[0]["median_seconds"] == 2
    assert rows[0]["successful_trials"] == rows[0]["failed_trials"] == 1
    assert rows[0]["notes"] == "Access denied"
    assert rows[1]["unsupported"]
    assert rows[1]["median_seconds"] is None


def test_summary_preserves_failed_warmup_and_cancellation_reasons():
    rows = summarize(
        [
            {
                "case_id": "case",
                "scale": "small",
                "status": "error",
                "warmup": True,
                "queries": [{"status": "row_count_mismatch", "rows": 2, "expected_rows": 10}],
                "cancellation_errors": ["q: StopQueryExecution denied"],
            }
        ]
    )
    assert rows[0]["failed_warmups"] == 1
    assert rows[0]["median_seconds"] is None
    assert "expected 10, got 2" in rows[0]["notes"]
    assert "Warmup: q: StopQueryExecution denied" in rows[0]["notes"]


class TestReport:
    def test_report_recovers_an_interrupted_trial_without_claiming_success(self, tmp_path):
        event = {
            "event": "trial_start",
            "trial": "interrupted",
            "case_id": "case",
            "scale": "small",
            "warmup": False,
        }
        (tmp_path / "events.jsonl").write_text(json.dumps(event) + "\n")
        (tmp_path / "trials.jsonl").write_text('{"trial":')
        with pytest.warns(UserWarning, match="incomplete final record"):
            report(tmp_path)
        with (tmp_path / "summary.csv").open() as stream:
            row = next(csv.DictReader(stream))
        assert row["failed_trials"] == "1"
        assert row["successful_trials"] == "0"
        assert row["median_seconds"] == ""

    def test_report_exposes_client_init_and_memory_increase(self, tmp_path):
        trial = {
            "case_id": "case",
            "scale": "small",
            "status": "ok",
            "queries": [
                {
                    "total_seconds": 20,
                    "post_completion_setup_seconds": 2,
                    "consume_seconds": 3,
                    "init_seconds": 1,
                }
            ],
            "successful_queries_per_second": 0.05,
            "rss_peak_bytes": 100,
            "rss_increase_bytes": 30,
            "max_threads": 3,
        }
        (tmp_path / "trials.jsonl").write_text(json.dumps(trial) + "\n")
        report(tmp_path)
        row = summarize([trial])[0]
        assert row["median_client_result_seconds"] == 5
        assert row["median_init_seconds"] == 1
        assert row["median_rss_increase_bytes"] == 30
        markdown = (tmp_path / "summary.md").read_text()
        assert "Client result (s)" in markdown
        assert "Init (s)" in markdown
        assert "RSS increase (B)" in markdown
