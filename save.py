# save pyspark dataframe

def save_to_parquet(df, path):
    df.write.format("parquet").mode("overwrite").save(path)

def save_to_json(df, path):
    df.write.format("json").mode("overwrite").save(path)