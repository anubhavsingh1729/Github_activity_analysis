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

    from pyspark.sql.functions import col, explode

    commit_df = commit_df.withColumn("file", explode(col("files"))) \
                         .select(
                             "sha", "author", "date", "message",
                             col("file.filename").alias("filename"),
                             col("file.additions").cast("int").alias("additions"),
                             col("file.deletions").cast("int").alias("deletions"),
                             col("file.changes").cast("int").alias("changes"),
                             col("file.patch").alias("patch")
                         )
    return commit_df