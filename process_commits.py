from pyspark.sql import SparkSession
from pyspark.sql.types import *

def init_spark():
    return SparkSession.builder.master("local").appName("github commit processor").getOrCreate()


def process_commits(spark,raw_data):
    schema = StructType([
        StructField("sha",StringType(),True),
        StructField("author",StringType(),True),
        StructField("date",StringType(),True),
        StructField("message",StringType(),True),
        StructField("files",ArrayType(MapType(StringType(),StringType())),True),
    ])

    commit_df = spark.createDataFrame(raw_data,schema)

    return commit_df

