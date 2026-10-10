# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
import os
import sys
from unittest.mock import patch

import pytest

from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.connection import Connection


class TestAthenaArrowResultSet:
    @pytest.mark.parametrize(
        ("connection_kwargs", "expected_access_key"),
        [
            ({}, "DEFAULTKEY"),
            ({"profile_name": "static"}, "STATICKEY"),
            ({"profile_name": "process"}, "PROCESSKEY"),
            (
                {"profile_name": "process", "role_arn": "arn:aws:iam::123456789012:role/r"},
                "ROLEKEY",
            ),
        ],
        ids=["default", "static", "credential_process", "role_arn"],
    )
    def test_create_s3_file_system_uses_session_credentials(
        self, monkeypatch, tmp_path, connection_kwargs, expected_access_key
    ):
        """The pyarrow filesystem reads with the credentials of the connection's session.

        No AWS calls; the profiles come from temporary config files, and the role
        assumption is mocked.
        """
        for key in list(os.environ):
            if key.startswith("AWS_"):
                monkeypatch.delenv(key)
        process = tmp_path / "credential_process.py"
        process.write_text(
            'print(\'{"Version": 1, "AccessKeyId": "PROCESSKEY", "SecretAccessKey": "secret"}\')\n'
        )
        config = tmp_path / "config"
        config.write_text(
            "[default]\naws_access_key_id = DEFAULTKEY\naws_secret_access_key = secret\n"
            "[profile static]\naws_access_key_id = STATICKEY\naws_secret_access_key = secret\n"
            f'[profile process]\ncredential_process = "{sys.executable}" "{process}"\n'
        )
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
        role_credentials = {
            "AccessKeyId": "ROLEKEY",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
        }
        with patch.object(Connection, "_assume_role", return_value=role_credentials):
            connection = Connection(
                region_name="us-east-1", s3_staging_dir="s3://bucket/path/", **connection_kwargs
            )
        with connection:
            result_set = AthenaArrowResultSet.__new__(AthenaArrowResultSet)  # bypass __init__
            result_set._connection = connection
            result_set._connect_timeout = None
            result_set._request_timeout = None
            fs = result_set._AthenaArrowResultSet__s3_file_system()

        # pyarrow exposes the filesystem options only through its pickle support.
        options = fs.__reduce__()[1][0]
        assert options["access_key"] == expected_access_key
        assert options["role_arn"] == ""
