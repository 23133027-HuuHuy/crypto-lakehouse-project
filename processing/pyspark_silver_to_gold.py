from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("1. Dang khoi tao Spark Session cho pipeline Silver -> Gold...")

spark = (SparkSession.builder
    .appName("Lakehouse_Silver_To_Gold")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.23.19")
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

silver_path = "s3a://lakehouse/silver/btc_trades"
gold_ohlc_path = "s3a://lakehouse/gold/OHLC_1Min"
gold_whale_path = "s3a://lakehouse/gold/Whale_Alert"
checkpoint_ohlc = "s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min"
checkpoint_whale = "s3a://lakehouse/checkpoints/silver_to_gold_whale_alert"
whale_threshold_usdt = 50000.0


def path_exists(path_str):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path_str), hadoop_conf)
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    return fs.exists(path_obj)


if not path_exists(silver_path):
    print(f"Khong tim thay du lieu Silver tai: {silver_path}")
    print("Vui long chay pipeline Bronze -> Silver truoc, hoac kiem tra lai duong dan Delta tren MinIO.")
    spark.stop()
    raise SystemExit(0)

print(f"2. Dang doc du lieu streaming tu lop Silver: {silver_path}")

df_silver = (spark.readStream
    .format("delta")
    .load(silver_path)
    .filter(
        col("symbol").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("event_time").isNotNull()
    ))


def rebuild_ohlc_1min(_, batch_id):
    print(f"\n[Batch {batch_id}] Dang tinh lai bang OHLC_1Min tu toan bo lop Silver...")

    df_silver_full = (spark.read
        .format("delta")
        .load(silver_path)
        .filter(
            col("symbol").isNotNull() &
            col("price").isNotNull() &
            col("quantity").isNotNull() &
            col("event_time").isNotNull()
        ))

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
            count(*) AS total_trades
        FROM silver_trades_full
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    (df_ohlc_snapshot.write
        .format("delta")
        .mode("overwrite")
        .save(gold_ohlc_path))

print("3. Dang tao bang Gold OHLC_1Min bang PySpark SQL...")
query_ohlc = (df_silver.writeStream
    .foreachBatch(rebuild_ohlc_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_ohlc)
    .start())

print("4. Dang tao bang Gold Whale_Alert bang PySpark SQL...")

df_silver.createOrReplaceTempView("silver_trades_stream")

df_whale_alert = spark.sql(f"""
    SELECT
        symbol,
        event_time,
        price,
        quantity,
        CAST(price * quantity AS DOUBLE) AS trade_value_usdt
    FROM silver_trades_stream
    WHERE (price * quantity) > {whale_threshold_usdt}
""")

query_whale = (df_whale_alert.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_whale)
    .start(gold_whale_path))

print(f"   -> Gold OHLC_1Min dang duoc cap nhat tai: {gold_ohlc_path}")
print(f"   -> Gold Whale_Alert dang duoc cap nhat tai: {gold_whale_path}")
print(f"   -> Nguong canh bao giao dich lon: > {whale_threshold_usdt:,.0f} USDT/lenh")

spark.streams.awaitAnyTermination()
