from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
    .appName("Check_Gold_Data")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

gold_ohlc_path = "s3a://lakehouse/gold/OHLC_1Min"
gold_whale_path = "s3a://lakehouse/gold/Whale_Alert"
gold_maker_taker_path = "s3a://lakehouse/gold/maker_taker_flow_1min"
gold_vwap_path = "s3a://lakehouse/gold/VWAP_1Min"


def inspect_delta_table(table_name, table_path, order_column):
    print(f"\n=== KIEM TRA BANG {table_name} ===")
    print(f"Dang doc du lieu Delta tu: {table_path}\n")

    df = spark.read.format("delta").load(table_path)

    print("Schema:")
    df.printSchema()

    print("\n20 dong moi nhat:")
    df.orderBy(col(order_column).desc()).show(20, truncate=False)

    total_rows = df.count()
    print(f"\n=> Tong so dong hien co trong {table_name}: {total_rows}")


try:
    inspect_delta_table("OHLC_1Min", gold_ohlc_path, "candle_time")
    inspect_delta_table("Whale_Alert", gold_whale_path, "event_time")
    inspect_delta_table("maker_taker_flow_1min", gold_maker_taker_path, "window_start")
    inspect_delta_table("VWAP_1Min", gold_vwap_path, "window_start")
except Exception as e:
    print(f"Co loi xay ra khi kiem tra du lieu Gold: {e}")

spark.stop()
