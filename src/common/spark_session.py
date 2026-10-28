from pyspark.sql import SparkSession


def get_spark(app_name):
    # Delta + S3A cấu hình trong configs/spark-defaults.conf (mount vào /opt/spark/conf)
    return SparkSession.builder.appName(app_name).getOrCreate()
