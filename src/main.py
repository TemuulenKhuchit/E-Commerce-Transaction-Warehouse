from config.spark_config import get_spark
from etl.transform import clean_data, add_bonus


def main():
    spark = get_spark()

    df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

    df_clean = clean_data(df)
    df_bonus = add_bonus(df_clean)

    df_bonus.show()

    spark.stop()


if __name__ == "__main__":
    main()
