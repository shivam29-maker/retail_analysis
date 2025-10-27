import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, join_orders_customers, count_orders_state , filter_order_generic    
from lib.ConfigReader import get_app_config

#setup for testing spark related functions would require a spark session thus it should be writtwn under a fixture and
# fixtures should be written in a seperate file "conftest.py"




def test_read_customers_df(spark):
    read_customers_count = read_customers(spark, "LOCAL").count()
    assert read_customers_count == 12435

@pytest.mark.slow()
def test_read_orders_df(spark):
    read_orders_count = read_orders(spark, "LOCAL").count()
    assert read_orders_count == 68884


# to run the test, python -m pytest 
@pytest.mark.transformation
def test_filter_closed_orders_df(spark):
    orders_df = read_orders(spark, "LOCAL")
    closed_orders_df = filter_closed_orders(orders_df)
    closed_orders_count = closed_orders_df.count()
    assert closed_orders_count == 7556

@pytest.mark.skip("work in progress")
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == 'E:\\demoproject\\data\\orders.csv'

    
@pytest.mark.transformation
def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark, "LOCAL")
    counted_df = count_orders_state(customers_df)
    assert counted_df.collect() == expected_results.collect()


@pytest.mark.skip
def test_check_closed_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filter_counted = filter_order_generic(orders_df,"CLOSED").count()
    assert filter_counted == 7556

@pytest.mark.skip
def test_check_pending_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filter_counted = filter_order_generic(orders_df,"PENDING_PAYMENT").count()
    assert filter_counted == 15030


@pytest.mark.skip
def test_check_complete_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filter_counted = filter_order_generic(orders_df,"COMPLETE").count()
    assert filter_counted == 22900

# the above test cases are redundent, here we are going to use parametrized fucntion 


@pytest.mark.parametrize("status,count",[("CLOSED", 7556),("PENDING_PAYMENT", 15030),("COMPLETE", 22900)])

@pytest.mark.latest
def test_check_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filter_counted = filter_order_generic(orders_df,status).count()
    assert filter_counted == count
