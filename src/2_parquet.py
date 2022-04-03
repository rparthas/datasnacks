from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark import SparkConf
import time

base_path = "../data/"
conf = SparkConf()
spark = SparkSession.builder.appName("ParquetDemo").config(conf=conf).getOrCreate()


# CSV :  5 * 734MB = 3.68 GB
# Record count: 5,80,69,710 ~ 58 million
# Size : 5 * 150 -170 MB = 850 MB
def generate_parquet():
    csv_df = spark.read.csv(f"{base_path}/fhv_csv/", header=True)
    csv_df.coalesce(5).write.mode("overwrite").parquet(f"{base_path}/fhv_data.parquet")
    print(csv_df.count())


def compare_speed(df, format):
    df.createOrReplaceTempView("rides")
    start = time.time()
    spark.sql("select count(*) from rides").show(10)
    end = time.time()
    print(f"Time taken for count[{format}]: {round(end - start, 2)}")
    start = time.time()
    spark.sql("select PULOcationID,count(*) from rides group by 1 order by 2 desc").show(10)
    end = time.time()
    print(f"Time taken for column access[{format}]: {round(end - start, 2)}")


if __name__ == '__main__':
    # generate_parquet()
    compare_speed(spark.read.parquet(f"{base_path}/fhv_data.parquet"), "Parquet")
    compare_speed(spark.read.csv(f"{base_path}/fhv_csv", header=True), "CSV")
