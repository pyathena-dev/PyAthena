#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sys
import time

from pyathena.pandas_cursor import PandasCursor
from pyathenajdbc import connect as jdbc_connect

from pyathena import connect

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(logging.INFO)

S3_STAGING_DIR = "s3://YOUR_BUCKET/path/to/"
REGION_NAME = "us-west-2"
COUNT = 5

SMALL_RESULT_SET_QUERY = """
SELECT * FROM pypi_downloads_20180915
WHERE file.project = 'pyhive'
"""
MEDIUM_RESULT_SET_QUERY = """
SELECT * FROM pypi_downloads_20180915
WHERE file.project = 'requests'
"""
LARGE_RESULT_SET_QUERY = """
SELECT * FROM pypi_downloads_20180915
WHERE file.project = 'pip'
"""


def run_pyathen_pandas_cursor(query):
    LOGGER.info("PyAthena PandasCursor =========================")
    cursor = connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=REGION_NAME,
        cursor_class=PandasCursor,
    ).cursor()
    avgs = []
    for i in range(0, COUNT):
        start = time.time()
        df = cursor.execute(query).as_pandas()
        end = time.time()
        elapsed = end - start
        LOGGER.info("loop:{0}\tcount:{1}\telasped:{2}".format(i, df.shape[0], elapsed))
        avgs.append(elapsed)
    avg = sum(avgs) / COUNT
    LOGGER.info("Avg: {0}".format(avg))
    LOGGER.info("===============================================")


def run_pyathena_cursor(query):
    LOGGER.info("PyAthena Cursor ===============================")
    cursor = connect(s3_staging_dir=S3_STAGING_DIR, region_name=REGION_NAME).cursor()
    avgs = []
    for i in range(0, COUNT):
        start = time.time()
        result = cursor.execute(query).fetchall()
        end = time.time()
        elapsed = end - start
        LOGGER.info("loop:{0}\tcount:{1}\telasped:{2}".format(i, len(result), elapsed))
        avgs.append(elapsed)
    avg = sum(avgs) / COUNT
    LOGGER.info("Avg: {0}".format(avg))
    LOGGER.info("===============================================")


def run_pyathenajdbc_cursor(query):
    LOGGER.info("PyAthenaJDBC Cursor ===========================")
    cursor = jdbc_connect(s3_staging_dir=S3_STAGING_DIR, region_name=REGION_NAME).cursor()
    avgs = []
    for i in range(0, COUNT):
        start = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        end = time.time()
        elapsed = end - start
        LOGGER.info("loop:{0}\tcount:{1}\telasped:{2}".format(i, len(result), elapsed))
        avgs.append(elapsed)
    avg = sum(avgs) / COUNT
    LOGGER.info("Avg: {0}".format(avg))
    LOGGER.info("===============================================")


def main():
    for query in [
        SMALL_RESULT_SET_QUERY,
        MEDIUM_RESULT_SET_QUERY,
        LARGE_RESULT_SET_QUERY,
    ]:
        LOGGER.info(query)
        run_pyathenajdbc_cursor(query)
        LOGGER.info("")
        run_pyathena_cursor(query)
        LOGGER.info("")
        run_pyathen_pandas_cursor(query)
        LOGGER.info("")


if __name__ == "__main__":
    main()
