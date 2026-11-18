"""Bronze layer: raw files as received, all columns as strings, plus ingestion metadata."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV export without type inference: bronze keeps the data exactly as delivered."""
    return (
        spark.read.option("header", True).option("inferSchema", False).option("encoding", "UTF-8").csv(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
