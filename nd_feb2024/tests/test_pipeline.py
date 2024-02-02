"""
Module for testing pipeline.py.
Tests do not currently run anywhere, if part of CICD then integrate it within the
CI process, such as when opening a pull request.
"""

import pytest
from nd_feb2024.pipeline import valid_user_id
from pyspark.sql import SparkSession
from pyspark.sql import types as t


@pytest.fixture(scope="session", name="spark", autouse=True)
def _spark():
    spark = (
        SparkSession.builder
        # One driver and one executor.
        .master("local[1]").getOrCreate()
    )
    return spark


@pytest.fixture(scope="session", name="test_df_schema")
def _test_df_schema():
    schema = t.StructType(
        [
            t.StructField("movie_id", t.IntegerType(), True),
            t.StructField("rating", t.IntegerType(), True),
            t.StructField("user_id", t.IntegerType(), True),
        ]
    )
    return schema


@pytest.fixture(scope="session", name="df_valid")
def _dummy_df(spark, test_df_schema):
    data = [(25, 123, 343), (1, 2, 3), (918, 33, 33)]
    return spark.createDataFrame(data, test_df_schema)


###############################
########### Testing ###########
###############################


def test_valid_id_pass(df_valid):
    filter_df = df_valid.filter(valid_user_id())
    assert df_valid == filter_df

# TODO: add more tests such as
