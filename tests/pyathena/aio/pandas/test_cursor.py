# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pyathena.aio.pandas.cursor import AioPandasCursor
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig

_FIVE_ROWS = "SELECT number FROM (VALUES (1), (2), (3), (4), (5)) AS t(number)"


class TestAioPandasCursor:
    async def test_binary_null_vs_empty(self, aio_pandas_cursor):
        query = """SELECT * FROM (VALUES
                    (1, CAST(NULL AS VARBINARY), 'null', CAST(NULL AS VARCHAR)),
                    (2, X'', 'empty', ''),
                    (3, X'00ff275c25', 'comma, quote" and' || chr(10) || 'newline', 'NULL')
                ) AS t(id, value, label, text_value) ORDER BY id"""
        await aio_pandas_cursor.execute(query, chunksize=2)
        result = aio_pandas_cursor
        rows = await result.fetchall()
        assert [row[:3] for row in rows] == [
            (1, None, "null"),
            (2, b"", "empty"),
            (3, b"\x00\xff'\\%", 'comma, quote" and\nnewline'),
        ]

    async def test_fetchone(self, aio_pandas_cursor):
        assert await aio_pandas_cursor.execute("SELECT * FROM one_row") is aio_pandas_cursor
        assert aio_pandas_cursor.description == [
            ("number_of_rows", "integer", None, None, 10, 0, "UNKNOWN")
        ]
        assert aio_pandas_cursor.rownumber == 0
        assert await aio_pandas_cursor.fetchone() == (1,)
        assert aio_pandas_cursor.rownumber == 1
        assert await aio_pandas_cursor.fetchone() is None

    async def test_as_pandas(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        df = aio_pandas_cursor.as_pandas()
        assert len(df) == 1
        assert df.columns.tolist() == ["number_of_rows"]
        assert df["number_of_rows"].iloc[0] == 1

    async def test_no_result_set_raises(self, aio_pandas_cursor):
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchone()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchmany()
        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.fetchall()
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.as_pandas()

    @pytest.mark.parametrize(
        "aio_pandas_cursor",
        [{"cursor_kwargs": {"unload": True}}],
        indirect=["aio_pandas_cursor"],
    )
    async def test_fetchone_unload(self, aio_pandas_cursor):
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        assert aio_pandas_cursor.as_pandas()["number_of_rows"].tolist() == [1]
        assert await aio_pandas_cursor.fetchone() == (1,)
        assert await aio_pandas_cursor.fetchone() is None

    @pytest.mark.parametrize(
        "execute_kwargs",
        [
            {},
            {
                "block_size": 2048,
                "cache_type": "none",
                "s3_max_workers": 3,
                "auto_optimize_chunksize": False,
            },
        ],
    )
    async def test_read_options(self, execute_kwargs):
        """The cursor's read options reach the result set, and execute() overrides them.

        No AWS calls; the query and its result set are mocked.
        """
        cursor_kwargs = {
            "block_size": 1024,
            "cache_type": "bytes",
            "s3_max_workers": 2,
            "auto_optimize_chunksize": True,
        }
        cursor = AioPandasCursor(
            connection=MagicMock(),
            converter=MagicMock(),
            formatter=MagicMock(),
            retry_config=RetryConfig(),
            **cursor_kwargs,
        )
        query_execution = MagicMock(state=AthenaQueryExecution.STATE_SUCCEEDED)
        with (
            patch.object(AioPandasCursor, "_execute", return_value="query_id"),
            patch.object(AioPandasCursor, "_poll", return_value=query_execution),
            patch("pyathena.aio.pandas.cursor.AthenaPandasResultSet") as result_set_class,
        ):
            await cursor.execute("SELECT 1", **execute_kwargs)
        kwargs = result_set_class.call_args.kwargs
        expected = {**cursor_kwargs, **execute_kwargs}
        expected["max_workers"] = expected.pop("s3_max_workers")
        assert {key: kwargs[key] for key in expected} == expected

    async def test_closed(self, aio_pandas_cursor):
        """GH-1207: a closed cursor rejects operations."""
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        aio_pandas_cursor.close()

        with pytest.raises(ProgrammingError):
            await aio_pandas_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            aio_pandas_cursor.as_pandas()

    @pytest.mark.parametrize(("chunksize", "sizes"), [(None, [5]), (2, [2, 2, 1])])
    async def test_iter_chunks(self, aio_pandas_cursor, chunksize, sizes):
        """GH-1209: iter_chunks() yields the chunks, and read_pandas() the whole result."""
        await aio_pandas_cursor.execute(_FIVE_ROWS, chunksize=chunksize)
        chunks = [chunk async for chunk in aio_pandas_cursor.iter_chunks()]
        assert [len(chunk) for chunk in chunks] == sizes
        assert pd.concat(chunks)["number"].tolist() == [1, 2, 3, 4, 5]
        await aio_pandas_cursor.execute(_FIVE_ROWS, chunksize=chunksize)
        assert (await aio_pandas_cursor.read_pandas())["number"].tolist() == [1, 2, 3, 4, 5]

    async def test_iter_chunks_after_execute(self, aio_pandas_cursor):
        """GH-1209: iter_chunks() reads the result set of its call, and raises once replaced."""
        await aio_pandas_cursor.execute(_FIVE_ROWS, chunksize=2)
        chunks = aio_pandas_cursor.iter_chunks()
        await aio_pandas_cursor.execute("SELECT * FROM one_row")
        with pytest.raises(ProgrammingError):
            await anext(chunks)
