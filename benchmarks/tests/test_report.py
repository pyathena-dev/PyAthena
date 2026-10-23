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


def test_summary_notes_the_sizes_of_a_size_mismatch():
    query = {"status": "size_mismatch", "bytes": 9, "expected_bytes": 10, "total_seconds": 1}
    rows = summarize([{"case_id": "case", "scale": "small", "status": "error", "queries": [query]}])
    assert rows[0]["notes"] == "size_mismatch: expected 10 B, got 9 B"


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

    def test_report_shows_the_throughput_of_filesystem_transfers(self, tmp_path):
        base = {
            "case_id": "filesystem-s3-sync-read-w8-p10",
            "scale": "small",
            "status": "ok",
            "successful_queries_per_second": 0.5,
            "rss_peak_bytes": 100,
            "max_threads": 12,
        }
        trials = [
            {**base, "repetition": r, "queries": [{"bytes": 2**30, "total_seconds": s}]}
            for r, s in enumerate((2, 4, 8))
        ]
        (tmp_path / "trials.jsonl").write_text("".join(json.dumps(t) + "\n" for t in trials))
        report(tmp_path)
        row = summarize(trials)[0]
        assert row["median_mib_per_second"] == 256
        assert row["peak_threads"] == 12
        markdown = (tmp_path / "summary.md").read_text()
        assert "Throughput (MiB/s)" in markdown
        assert "| 256.0 |" in markdown

    def test_report_shows_objects_per_second_and_cpu_time(self, tmp_path):
        base = {
            "case_id": "objects-s3-aio-cat-n8",
            "scale": "small",
            "status": "ok",
            "successful_queries_per_second": 0.5,
            "rss_peak_bytes": 100,
            "max_threads": 12,
        }
        query = {"objects": 100, "bytes": 2**20, "total_seconds": 2, "cpu_seconds": 0.5}
        trials = [{**base, "repetition": r, "queries": [query]} for r in range(2)]
        mismatch = {
            **base,
            "repetition": 2,
            "status": "error",
            "queries": [
                {**query, "status": "count_mismatch", "objects": 99, "expected_objects": 100}
            ],
        }
        (tmp_path / "trials.jsonl").write_text(
            "".join(json.dumps(t) + "\n" for t in [*trials, mismatch])
        )
        report(tmp_path)
        row = summarize([*trials, mismatch])[0]
        assert row["median_objects_per_second"] == 50
        assert row["median_cpu_seconds"] == 0.5
        assert row["median_mib_per_second"] == 0.5
        assert row["notes"] == "count_mismatch: expected 100, got 99"
        markdown = (tmp_path / "summary.md").read_text()
        assert "| Objects/s | CPU (s) |" in markdown
        assert "| 50.0 | 0.5 |" in markdown

    def test_report_shows_the_most_output_files_and_the_largest_file(self, tmp_path):
        base = {
            "case_id": "case",
            "scale": "small",
            "status": "ok",
            "queries": [{"total_seconds": 1}],
            "successful_queries_per_second": 1,
            "rss_peak_bytes": 1,
            "max_threads": 1,
        }
        trials = [
            {**base, "output_files": {"count": 30, "bytes": 60, "max_bytes": 4, "min_bytes": 1}},
            {**base, "output_files": {"count": 31, "bytes": 60, "max_bytes": 3, "min_bytes": 1}},
        ]
        (tmp_path / "trials.jsonl").write_text("".join(json.dumps(t) + "\n" for t in trials))
        report(tmp_path)
        row = summarize(trials)[0]
        assert (row["max_output_files"], row["max_output_file_bytes"]) == (31, 4)
        assert "| 31 | 4 |" in (tmp_path / "summary.md").read_text()
        assert summarize([base])[0]["max_output_files"] is None
        failed_listing = {**base, "output_files_error": "ClientError: denied"}
        row = summarize([trials[0], failed_listing])[0]
        assert (row["successful_trials"], row["max_output_files"]) == (2, 30)
        assert row["notes"] == "Output files not listed: ClientError: denied"
        # A warmup is not in the columns, so its listing failure is not noted either.
        row = summarize([trials[0], {**failed_listing, "warmup": True}])[0]
        assert row["notes"] == ""

    def test_report_merges_attempts_and_notes_superseded_failures(self, tmp_path):
        def trial(repetition, status="ok"):
            return {
                "trial": f"t{repetition}-{status}",
                "case_id": "case",
                "scale": "small",
                "repetition": repetition,
                "warmup": False,
                "status": status,
                "queries": [{"total_seconds": 1}],
                "successful_queries_per_second": 1,
                "rss_peak_bytes": 1,
                "max_threads": 1,
            }

        first = tmp_path / "attempt-1"
        second = tmp_path / "attempt-2"
        first.mkdir()
        second.mkdir()
        (first / "trials.jsonl").write_text(
            json.dumps(trial(0))
            + "\n"
            + json.dumps({**trial(1, "interrupted"), "queries": []})
            + "\n"
        )
        # The interrupted attempt also started a trial that it never recorded.
        start = {"event": "trial_start", "trial": "lost", "case_id": "case", "scale": "small"}
        (first / "events.jsonl").write_text(json.dumps({**start, "repetition": 2}) + "\n")
        (second / "trials.jsonl").write_text(
            json.dumps(trial(1)) + "\n" + json.dumps(trial(2)) + "\n"
        )
        report(tmp_path)
        with (tmp_path / "summary.csv").open() as stream:
            row = next(csv.DictReader(stream))
        assert (row["successful_trials"], row["failed_trials"]) == ("3", "0")
        # The superseded failures stay visible in the notes: the interrupted and the
        # unrecorded trial.
        assert "Superseded: interrupted" in row["notes"]
        assert "Superseded: Trial started without a final record" in row["notes"]

    def test_report_counts_a_repetition_recorded_by_two_attempts_once(self, tmp_path):
        record = {
            "case_id": "case",
            "scale": "small",
            "repetition": 0,
            "warmup": False,
            "status": "ok",
            "queries": [{"total_seconds": 1}],
            "successful_queries_per_second": 1,
            "rss_peak_bytes": 1,
            "max_threads": 1,
        }
        for attempt in (1, 2):
            directory = tmp_path / f"attempt-{attempt}"
            directory.mkdir()
            (directory / "trials.jsonl").write_text(
                json.dumps({**record, "trial": f"t{attempt}"}) + "\n"
            )
        report(tmp_path)
        with (tmp_path / "summary.csv").open() as stream:
            assert next(csv.DictReader(stream))["successful_trials"] == "1"

    def test_report_reads_the_checkpoint_of_an_attempt(self, tmp_path):
        record = {
            "trial": "t0",
            "case_id": "case",
            "scale": "small",
            "repetition": 0,
            "warmup": False,
            "status": "ok",
            "queries": [{"total_seconds": 1}],
            "successful_queries_per_second": 1,
            "rss_peak_bytes": 1,
            "max_threads": 1,
        }
        checkpoint = tmp_path / "attempt-1" / "checkpoint"
        checkpoint.mkdir(parents=True)
        # The host died after recording one trial and starting another.
        (checkpoint / "t0.json").write_text(json.dumps(record))
        start = {"event": "trial_start", "trial": "t1", "case_id": "case", "scale": "small"}
        (checkpoint / "t1-start.json").write_text(json.dumps({**start, "repetition": 1}))
        (checkpoint / "t0-start.json").write_text(json.dumps({**start, "trial": "t0"}))
        report(tmp_path)
        with (tmp_path / "summary.csv").open() as stream:
            row = next(csv.DictReader(stream))
        assert (row["successful_trials"], row["failed_trials"]) == ("1", "1")
