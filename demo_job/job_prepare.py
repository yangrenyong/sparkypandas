import __main__
from pyspark.sql import SparkSession


def start_spark(appName='sparkypandas-demo', master='local[*]', env='DEBUG'):
    is_repl = not (hasattr(__main__, '__file__'))
    is_debug = 'DEBUG' == env

    if not (is_repl or is_debug):
        session_builder = (
            SparkSession
                .builder
                .enableHiveSupport()
                .appName(appName))
    else:
        session_builder = (
            SparkSession
                .builder
                .master(master)
                .enableHiveSupport()
                .appName(appName))

    return session_builder.getOrCreate()