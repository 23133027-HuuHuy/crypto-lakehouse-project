import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, from_unixtime, lit, lower, to_timestamp, when
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

# ================================================================
# SPARK STREAMING: Bronze → Silver (Làm sạch & chuẩn hóa)
# ================================================================

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)

bronze_path = "s3a://lakehouse/bronze/all_crypto_trades"
silver_path = "s3a://lakehouse/silver/btc_trades"
checkpoint_silver = "s3a://lakehouse/checkpoints/bronze_to_silver"


def wait_for_bronze_table(spark_session, path, max_retries=60, delay=10):
    """Đợi Bronze Delta table tồn tại trước khi đọc stream"""
    for attempt in range(1, max_retries + 1):
        try:
            hadoop_conf = spark_session._jsc.hadoopConfiguration()
            fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session._jvm.java.net.URI(path), hadoop_conf)
            delta_log = spark_session._jvm.org.apache.hadoop.fs.Path(path + "/_delta_log")
            if fs.exists(delta_log):
                print(f"✅ Bronze Delta table đã sẵn sàng tại: {path}")
                return True
        except Exception as e:
            print(f"⏳ [{attempt}/{max_retries}] Đợi Bronze table... ({e})")
        time.sleep(delay)
    raise FileNotFoundError(f"Bronze Delta table không tồn tại sau {max_retries * delay}s: {path}")


# 1. Khởi tạo Spark
print("🔧 Đang khởi tạo Spark Session cho pipeline Bronze → Silver...")
spark = (SparkSession.builder
    .appName("Lakehouse_Bronze_To_Silver")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 2. Đợi Bronze table tồn tại (stream-bronze cần ghi ít nhất 1 micro-batch trước)
wait_for_bronze_table(spark, bronze_path)

# 3. Định nghĩa Schema chuẩn của Binance để bóc tách JSON
json_schema = StructType([
    StructField("p", StringType(), True),  # Price
    StructField("q", StringType(), True),  # Quantity
    StructField("T", LongType(), True),    # Event time
    StructField("s", StringType(), True),  # Symbol (BTCUSDT)
    StructField("m", BooleanType(), True)  # is buyer maker
])

# 4. Đọc luồng dữ liệu từ lớp BRONZE
df_bronze = spark.readStream.format("delta").load(bronze_path)

# KHẮC PHỤC LỖI SCHEMA: Đảm bảo các cột luôn tồn tại dù chỉ có Stream hoặc chỉ có Batch
if "value" not in df_bronze.columns:
    df_bronze = df_bronze.withColumn("value", lit(None).cast("string"))
# Thêm cột Batch nếu chưa có (trường hợp Bronze chỉ có dữ liệu Stream)
for batch_col in ["Price", "Quantity", "Quote_Qty", "Timestamp", "is_Buyer_Maker"]:
    if batch_col not in df_bronze.columns:
        df_bronze = df_bronze.withColumn(batch_col, lit(None).cast("string"))

# 5. Xử lý làm sạch đa luồng (Handling both Batch & Stream struct)
df_silver = (df_bronze
    # Kiểm tra: nếu có cột 'value' (từ Stream) thì parse JSON, còn không thì None
    .withColumn("stream_data", when(col("value").isNotNull(), from_json(col("value"), json_schema)).otherwise(None))
    .withColumn("price", coalesce(col("stream_data.p"), col("Price")).cast(DoubleType()))
    .withColumn("quantity", coalesce(col("stream_data.q"), col("Quantity")).cast(DoubleType()))
    .withColumn(
        "quote_qty",
        coalesce(
            col("Quote_Qty").cast(DoubleType()),
            (col("price") * col("quantity")).cast(DoubleType())
        )
    )
    .withColumn(
        "event_time",
        coalesce(
            # Stream data: timestamp T là MILLISECONDS -> chia 1000
            to_timestamp(from_unixtime(col("stream_data.T").cast(DoubleType()) / 1000.0)),
            # Batch data: timestamp là MICROSECONDS -> chia 1000000
            to_timestamp(from_unixtime(col("Timestamp").cast(DoubleType()) / 1000000.0))
        )
    )
    .withColumn(
        "is_buyer_maker",
        when(col("stream_data.m").isNotNull(), col("stream_data.m"))
        .when(lower(col("is_Buyer_Maker")) == "true", lit(True))
        .when(lower(col("is_Buyer_Maker")) == "false", lit(False))
        .otherwise(None)
        .cast(BooleanType())
    )
    .select(
        coalesce(col("stream_data.s"), lit("BTCUSDT")).alias("symbol"),
        col("price"),
        col("quantity"),
        col("quote_qty"),
        col("event_time"),
        col("is_buyer_maker")
    )
    .filter(
        col("price").isNotNull() &
        (col("price") > 0) &
        col("quantity").isNotNull() &
        (col("quantity") > 0)
    )  # Loại bỏ dữ liệu rác/lỗi
)

# 6. Ghi xuống lớp SILVER
query = (df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_silver)
    .option("mergeSchema", "true")
    .start(silver_path))

print(f"✨ Lớp Silver đang được tinh chế tại: {silver_path}")
query.awaitTermination()
