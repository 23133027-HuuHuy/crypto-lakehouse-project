import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ================================================================
# SPARK STREAMING: Silver → Gold (Tổng hợp analytics)
# ================================================================

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)

silver_path = "s3a://lakehouse/silver/btc_trades"
gold_ohlc_path = "s3a://lakehouse/gold/OHLC_1Min"
gold_whale_path = "s3a://lakehouse/gold/Whale_Alert"
gold_maker_taker_path = "s3a://lakehouse/gold/maker_taker_flow_1min"
gold_vwap_path = "s3a://lakehouse/gold/VWAP_1Min"
checkpoint_ohlc = "s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min"
checkpoint_whale = "s3a://lakehouse/checkpoints/silver_to_gold_whale_alert"
checkpoint_maker_taker = "s3a://lakehouse/checkpoints/silver_to_gold_maker_taker_flow_1min"
checkpoint_vwap = "s3a://lakehouse/checkpoints/silver_to_gold_vwap_1min"
whale_threshold_usdt = 50000.0


def wait_for_silver_table(spark_session, path, max_retries=90, delay=10):
    """Đợi Silver Delta table tồn tại trước khi đọc stream"""
    for attempt in range(1, max_retries + 1):
        try:
            hadoop_conf = spark_session._jsc.hadoopConfiguration()
            fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session._jvm.java.net.URI(path), hadoop_conf)
            delta_log = spark_session._jvm.org.apache.hadoop.fs.Path(path + "/_delta_log")
            if fs.exists(delta_log):
                print(f"✅ Silver Delta table đã sẵn sàng tại: {path}")
                return True
        except Exception as e:
            print(f"⏳ [{attempt}/{max_retries}] Đợi Silver table... ({e})")
        time.sleep(delay)
    raise FileNotFoundError(f"Silver Delta table không tồn tại sau {max_retries * delay}s: {path}")


# 1. Khởi tạo Spark
print("1. Dang khoi tao Spark Session cho pipeline Silver -> Gold...")
spark = (SparkSession.builder
    .appName("Lakehouse_Silver_To_Gold")
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

# 2. Đợi Silver table tồn tại
wait_for_silver_table(spark, silver_path)

print(f"2. Dang doc du lieu streaming tu lop Silver: {silver_path}")

df_silver = (spark.readStream
    .format("delta")
    .load(silver_path)
    .filter(
        col("symbol").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("quote_qty").isNotNull() &
        col("event_time").isNotNull()
    ))


def load_silver_snapshot():
    return (spark.read
        .format("delta")
        .load(silver_path)
        .filter(
            col("symbol").isNotNull() &
            col("price").isNotNull() &
            col("quantity").isNotNull() &
            col("quote_qty").isNotNull() &
            col("event_time").isNotNull()
        ))


# ============================================================
# GOLD TABLE 1: OHLC_1Min (Nến giá 1 phút)
# ============================================================
def rebuild_ohlc_1min(_, batch_id):
    print(f"\n[Batch {batch_id}] Dang tinh lai bang OHLC_1Min tu toan bo lop Silver...")

    df_silver_full = load_silver_snapshot()
    df_silver_full.createOrReplaceTempView("silver_trades_full")

    df_ohlc_snapshot = spark.sql("""
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
        FROM silver_trades_full
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    (df_ohlc_snapshot.write
        .format("delta")
        .mode("overwrite")
        .save(gold_ohlc_path))


# ============================================================
# GOLD TABLE 2: Whale_Alert (Cảnh báo giao dịch lớn)
# ============================================================
def rebuild_whale_alert(micro_batch_df, batch_id):
    """Dùng foreachBatch để xử lý Whale Alert đúng cách"""
    print(f"\n[Batch {batch_id}] Dang quet Whale Alert tu micro-batch...")

    whale_df = micro_batch_df.filter(col("quote_qty") > whale_threshold_usdt)

    if whale_df.count() > 0:
        (whale_df
            .withColumn("trade_value_usdt", col("quote_qty").cast("double"))
            .write
            .format("delta")
            .mode("append")
            .save(gold_whale_path))
        print(f"  -> Them {whale_df.count()} Whale Alert moi")


# ============================================================
# GOLD TABLE 3: Maker/Taker Flow (Dòng tiền mua/bán)
# ============================================================
def rebuild_maker_taker_flow(_, batch_id):
    print(f"\n[Batch {batch_id}] Dang tinh lai bang maker_taker_flow_1min...")

    df_silver_full = load_silver_snapshot()
    df_silver_full.createOrReplaceTempView("silver_trades_full")

    df_maker_taker_snapshot = spark.sql("""
        SELECT
            symbol,
            window(event_time, '1 minute').start AS window_start,
            sum(CASE WHEN is_buyer_maker = false THEN quantity ELSE 0 END) AS buy_aggressive_qty,
            sum(CASE WHEN is_buyer_maker = true THEN quantity ELSE 0 END) AS sell_aggressive_qty,
            sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END) AS buy_aggressive_quote_qty,
            sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS sell_aggressive_quote_qty,
            sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END)
                - sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS net_flow
        FROM silver_trades_full
        WHERE is_buyer_maker IS NOT NULL
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    (df_maker_taker_snapshot.write
        .format("delta")
        .mode("overwrite")
        .save(gold_maker_taker_path))


# ============================================================
# GOLD TABLE 4: VWAP_1Min (Volume-Weighted Average Price)
# ============================================================
def rebuild_vwap_1min(_, batch_id):
    print(f"\n[Batch {batch_id}] Dang tinh lai bang VWAP_1Min...")

    df_silver_full = load_silver_snapshot()
    df_silver_full.createOrReplaceTempView("silver_trades_full")

    df_vwap_snapshot = spark.sql("""
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
        FROM silver_trades_full
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    (df_vwap_snapshot.write
        .format("delta")
        .mode("overwrite")
        .save(gold_vwap_path))


# ============================================================
# START ALL STREAMING QUERIES
# ============================================================

print("3. Dang tao bang Gold OHLC_1Min bang PySpark SQL...")
query_ohlc = (df_silver.writeStream
    .foreachBatch(rebuild_ohlc_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_ohlc)
    .start())

print("4. Dang tao bang Gold Whale_Alert bang PySpark SQL...")
query_whale = (df_silver.writeStream
    .foreachBatch(rebuild_whale_alert)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_whale)
    .start())

print("5. Dang tao bang Gold maker_taker_flow_1min bang PySpark SQL...")
query_maker_taker = (df_silver.writeStream
    .foreachBatch(rebuild_maker_taker_flow)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_maker_taker)
    .start())

print("6. Dang tao bang Gold VWAP_1Min bang PySpark SQL...")
query_vwap = (df_silver.writeStream
    .foreachBatch(rebuild_vwap_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_vwap)
    .start())

print(f"   -> Gold OHLC_1Min dang duoc cap nhat tai: {gold_ohlc_path}")
print(f"   -> Gold Whale_Alert dang duoc cap nhat tai: {gold_whale_path}")
print(f"   -> Gold maker_taker_flow_1min dang duoc cap nhat tai: {gold_maker_taker_path}")
print(f"   -> Gold VWAP_1Min dang duoc cap nhat tai: {gold_vwap_path}")
print(f"   -> Nguong canh bao giao dich lon: > {whale_threshold_usdt:,.0f} USDT/lenh")

spark.streams.awaitAnyTermination()
