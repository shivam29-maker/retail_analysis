import pytest
from lib.Utils import get_spark_session

       #
       # thus we should use yield keyword and release the resource after use

@pytest.fixture
def spark():
    "creates spark session for local mode" # the docstring for the fixture
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "gives expected results"
    results_schema = "state string, count integer"
    return spark.read.format("csv").option("header",True).schema(results_schema) \
        .load("data/test_results/*")