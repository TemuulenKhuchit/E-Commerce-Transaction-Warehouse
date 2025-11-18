from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder
        .appName("LocalETLProject")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
