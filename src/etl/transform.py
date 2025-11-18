from pyspark.sql.functions import col


def clean_data(df):
    return df.filter(col("salary") > 3500)


def add_bonus(df):
    return df.withColumn("bonus", col("salary") * 0.10)
