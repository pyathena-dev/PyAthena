# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT
from unittest.mock import MagicMock, patch

from pyathena.arrow.converter import DefaultArrowTypeConverter
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig


class TestAthenaArrowResultSet:
    def test_fetch_after_close(self):
        """No AWS calls; the query execution and the filesystem are mocked."""
        with patch.object(AthenaArrowResultSet, "_create_s3_file_system"):
            result_set = AthenaArrowResultSet(
                connection=MagicMock(),
                converter=DefaultArrowTypeConverter(),
                query_execution=MagicMock(state=AthenaQueryExecution.STATE_FAILED),
                arraysize=1,
                retry_config=RetryConfig(),
            )
        result_set.close()
        assert result_set.fetchone() is None
        assert result_set.fetchmany() == []
        assert result_set.fetchall() == []
