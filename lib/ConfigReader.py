# cofig reader file is there to read all the configuration settings from a config file and provide easy access to these settings throughout the application.

import configparser

from pyspark import SparkConf

def get_app_config(env):
    config = configparser.ConfigParser()
    path = 'E:\demoproject\configs\\application.conf'
    config.read(path)
    app_config = {}
    
    for key, value in config.items(env):
        app_config[key] = value
    
    return app_config

def get_spark_config(env):
    config = configparser.ConfigParser()
    path = 'E:\demoproject\configs\\application.conf'        
    config.read(path)
    pyspark_conf = SparkConf()
    
    for key, value in config.items(env):
        pyspark_conf.set(key, value)
    
    return pyspark_conf