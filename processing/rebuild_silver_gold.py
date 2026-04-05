from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp, coalesce, when, lower, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType

print("=" * 60)
print("REBUILD SILVER & GOLD LAYERS (FIX TIMESTAMP)")
print("=" * 60)

# 1. Khởi tạo Spark
spark = (SparkSession.builder
    .appName("Rebuild_Silver_Gold_Layers")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Paths
bronze_path = "s3a://lakehouse/bronze/batch_data"
silver_path = "s3a://lakehouse/silver/btc_trades"
gold_ohlc_path = "s3a://lakehouse/gold/OHLC_1Min"
gold_whale_path = "s3a://lakehouse/gold/Whale_Alert"
gold_maker_taker_path = "s3a://lakehouse/gold/maker_taker_flow_1min"
gold_vwap_path = "s3a://lakehouse/gold/VWAP_1Min"

whale_threshold_usdt = 50000.0


def delete_path(path_str):
    """Xóa thư mục trên S3/MinIO"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path_str), hadoop_conf)
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path_str)
        if fs.exists(path_obj):
            fs.delete(path_obj, True)
            print(f"  ✓ Đã xóa: {path_str}")
        else:
            print(f"  - Không tồn tại: {path_str}")
    except Exception as e:
        print(f"  ✗ Lỗi xóa {path_str}: {e}")


# 2. Xóa Silver và Gold layers
print("\n[BƯỚC 1] Xóa dữ liệu cũ Silver + Gold...")
delete_path(silver_path)
delete_path(gold_ohlc_path)
delete_path(gold_whale_path)
delete_path(gold_maker_taker_path)
delete_path(gold_vwap_path)

# Xóa checkpoints
print("\n[BƯỚC 2] Xóa checkpoints...")
delete_path("s3a://lakehouse/checkpoints/bronze_to_silver")
delete_path("s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min")
delete_path("s3a://lakehouse/checkpoints/silver_to_gold_whale_alert")
delete_path("s3a://lakehouse/checkpoints/silver_to_gold_maker_taker_flow_1min")
delete_path("s3a://lakehouse/checkpoints/silver_to_gold_vwap_1min")

# 3. Đọc Bronze và rebuild Silver
print("\n[BƯỚC 3] Đọc dữ liệu Bronze...")
df_bronze = spark.read.format("delta").load(bronze_path)
print(f"  → Tổng số dòng Bronze: {df_bronze.count():,}")

# Schema cho JSON từ stream
json_schema = StructType([
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("T", LongType(), True),
    StructField("s", StringType(), True),
    StructField("m", BooleanType(), True)
])

# Đảm bảo các cột tồn tại
if "value" not in df_bronze.columns:
    df_bronze = df_bronze.withColumn("value", lit(None).cast("string"))
for batch_col in ["Price", "Quantity", "Quote_Qty", "Timestamp", "is_Buyer_Maker"]:
    if batch_col not in df_bronze.columns:
        df_bronze = df_bronze.withColumn(batch_col, lit(None).cast("string"))

# 4. Transform Bronze → Silver với timestamp FIX
print("\n[BƯỚC 4] Transform Bronze → Silver (timestamp fix)...")
df_silver = (df_bronze
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
        (col("quantity") > 0) &
        col("event_time").isNotNull()
    )
)

# 5. Ghi Silver
print("\n[BƯỚC 5] Ghi dữ liệu Silver...")
df_silver.write.format("delta").mode("overwrite").save(silver_path)
silver_count = spark.read.format("delta").load(silver_path).count()
print(f"  ✓ Đã ghi {silver_count:,} dòng vào Silver")

# Kiểm tra timestamp
print("\n[KIỂM TRA] Sample Silver data:")
spark.read.format("delta").load(silver_path).orderBy(col("event_time").desc()).show(5, truncate=False)

# 6. Build Gold layers
print("\n[BƯỚC 6] Build Gold OHLC_1Min...")
df_silver_full = spark.read.format("delta").load(silver_path)
df_silver_full.createOrReplaceTempView("silver_trades")

df_ohlc = spark.sql("""
    SELECT
        symbol,
        window(event_time, '1 minute').start AS candle_time,
        min_by(price, event_time) AS open_price,
        max(price) AS high_price,
        min(price) AS low_price,
        max_by(price, event_time) AS close_price,
        sum(quantity) AS total_quantity,
        sum(quote_qty) AS total_quote_qty,
        count(*) AS total_trades
    FROM silver_trades
    GROUP BY symbol, window(event_time, '1 minute')
""")
df_ohlc.write.format("delta").mode("overwrite").save(gold_ohlc_path)
print(f"  ✓ OHLC_1Min: {df_ohlc.count():,} dòng")

print("\n[BƯỚC 7] Build Gold Whale_Alert...")
df_whale = spark.sql(f"""
    SELECT
        symbol,
        event_time,
        price,
        quantity,
        quote_qty,
        is_buyer_maker,
        CAST(quote_qty AS DOUBLE) AS trade_value_usdt
    FROM silver_trades
    WHERE quote_qty > {whale_threshold_usdt}
""")
df_whale.write.format("delta").mode("overwrite").save(gold_whale_path)
print(f"  ✓ Whale_Alert: {df_whale.count():,} dòng")

print("\n[BƯỚC 8] Build Gold maker_taker_flow_1min...")
df_flow = spark.sql("""
    SELECT
        symbol,
        window(event_time, '1 minute').start AS window_start,
        sum(CASE WHEN is_buyer_maker = false THEN quantity ELSE 0 END) AS buy_aggressive_qty,
        sum(CASE WHEN is_buyer_maker = true THEN quantity ELSE 0 END) AS sell_aggressive_qty,
        sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END) AS buy_aggressive_quote_qty,
        sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS sell_aggressive_quote_qty,
        sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END)
            - sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS net_flow
    FROM silver_trades
    WHERE is_buyer_maker IS NOT NULL
    GROUP BY symbol, window(event_time, '1 minute')
""")
df_flow.write.format("delta").mode("overwrite").save(gold_maker_taker_path)
print(f"  ✓ maker_taker_flow_1min: {df_flow.count():,} dòng")

print("\n[BƯỚC 9] Build Gold VWAP_1Min...")
df_vwap = spark.sql("""
    SELECT
        symbol,
        window(event_time, '1 minute').start AS window_start,
        sum(quantity) AS total_quantity,
        sum(quote_qty) AS total_quote_qty,
        count(*) AS trade_count,
        sum(quote_qty) / sum(quantity) AS vwap_price,
        avg(quantity) AS avg_trade_size,
        max_by(price, event_time) - (sum(quote_qty) / sum(quantity)) AS close_vs_vwap_diff,
        ((max_by(price, event_time) - (sum(quote_qty) / sum(quantity))) / (sum(quote_qty) / sum(quantity))) * 100
            AS close_vs_vwap_pct
    FROM silver_trades
    GROUP BY symbol, window(event_time, '1 minute')
""")
df_vwap.write.format("delta").mode("overwrite").save(gold_vwap_path)
print(f"  ✓ VWAP_1Min: {df_vwap.count():,} dòng")

print("\n" + "=" * 60)
print("✅ HOÀN TẤT REBUILD!")
print("=" * 60)

# Kiểm tra kết quả cuối
print("\n[KIỂM TRA] Sample Gold OHLC data:")
spark.read.format("delta").load(gold_ohlc_path).orderBy(col("candle_time").desc()).show(5, truncate=False)

spark.stop()
