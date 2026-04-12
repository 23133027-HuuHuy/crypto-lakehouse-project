from delta.tables import DeltaTable
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
gold_maker_taker_path = "s3a://lakehouse/gold/maker_taker_flow_1min"
gold_vwap_path = "s3a://lakehouse/gold/VWAP_1Min"
checkpoint_ohlc = "s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min"
checkpoint_whale = "s3a://lakehouse/checkpoints/silver_to_gold_whale_alert"
checkpoint_maker_taker = "s3a://lakehouse/checkpoints/silver_to_gold_maker_taker_flow_1min"
checkpoint_vwap = "s3a://lakehouse/checkpoints/silver_to_gold_vwap_1min"
whale_threshold_usdt = 50000.0


def path_exists(path_str):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path_str), hadoop_conf)
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    return fs.exists(path_obj)


def is_delta_table(path_str):
    try:
        return DeltaTable.isDeltaTable(spark, path_str)
    except Exception:
        return False


def merge_into_delta(source_df, target_path, merge_condition, set_map):
    if source_df.rdd.isEmpty():
        return

    if not is_delta_table(target_path):
        (source_df.write
            .format("delta")
            .mode("append")
            .save(target_path))
        return

    delta_table = DeltaTable.forPath(spark, target_path)
    (delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=set_map)
        .whenNotMatchedInsertAll()
        .execute())


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
        col("quote_qty").isNotNull() &
        col("event_time").isNotNull()
    ))


def upsert_ohlc_1min(df_batch, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang OHLC_1Min...")

    df_batch.createOrReplaceTempView("silver_trades_batch")

    df_ohlc_batch = spark.sql("""
        SELECT
            symbol,
            window(event_time, '1 minute').start AS candle_time,
            min_by(price, event_time) AS open_price,
            min(event_time) AS open_event_time,
            max(price) AS high_price,
            min(price) AS low_price,
            max_by(price, event_time) AS close_price,
            max(event_time) AS close_event_time,
            sum(quantity) AS total_quantity,
            sum(quote_qty) AS total_quote_qty,
            count(*) AS total_trades
        FROM silver_trades_batch
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    merge_into_delta(
        source_df=df_ohlc_batch,
        target_path=gold_ohlc_path,
        merge_condition="""
            target.symbol = source.symbol
            AND target.candle_time = source.candle_time
        """,
        set_map={
            "open_price": """
                CASE
                    WHEN source.open_event_time < target.open_event_time THEN source.open_price
                    ELSE target.open_price
                END
            """,
            "open_event_time": "least(target.open_event_time, source.open_event_time)",
            "high_price": "greatest(target.high_price, source.high_price)",
            "low_price": "least(target.low_price, source.low_price)",
            "close_price": """
                CASE
                    WHEN source.close_event_time > target.close_event_time THEN source.close_price
                    ELSE target.close_price
                END
            """,
            "close_event_time": "greatest(target.close_event_time, source.close_event_time)",
            "total_quantity": "target.total_quantity + source.total_quantity",
            "total_quote_qty": "target.total_quote_qty + source.total_quote_qty",
            "total_trades": "target.total_trades + source.total_trades",
        }
    )


def upsert_maker_taker_flow(df_batch, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang maker_taker_flow_1min...")

    df_batch.createOrReplaceTempView("silver_trades_batch")

    df_maker_taker_batch = spark.sql("""
        SELECT
            symbol,
            window(event_time, '1 minute').start AS window_start,
            sum(CASE WHEN is_buyer_maker = false THEN quantity ELSE 0 END) AS buy_aggressive_qty,
            sum(CASE WHEN is_buyer_maker = true THEN quantity ELSE 0 END) AS sell_aggressive_qty,
            sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END) AS buy_aggressive_quote_qty,
            sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS sell_aggressive_quote_qty,
            sum(CASE WHEN is_buyer_maker = false THEN quote_qty ELSE 0 END)
                - sum(CASE WHEN is_buyer_maker = true THEN quote_qty ELSE 0 END) AS net_flow
        FROM silver_trades_batch
        WHERE is_buyer_maker IS NOT NULL
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    merge_into_delta(
        source_df=df_maker_taker_batch,
        target_path=gold_maker_taker_path,
        merge_condition="""
            target.symbol = source.symbol
            AND target.window_start = source.window_start
        """,
        set_map={
            "buy_aggressive_qty": "target.buy_aggressive_qty + source.buy_aggressive_qty",
            "sell_aggressive_qty": "target.sell_aggressive_qty + source.sell_aggressive_qty",
            "buy_aggressive_quote_qty": "target.buy_aggressive_quote_qty + source.buy_aggressive_quote_qty",
            "sell_aggressive_quote_qty": "target.sell_aggressive_quote_qty + source.sell_aggressive_quote_qty",
            "net_flow": "target.net_flow + source.net_flow",
        }
    )


def upsert_vwap_1min(df_batch, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang VWAP_1Min...")

    df_batch.createOrReplaceTempView("silver_trades_batch")

    df_vwap_batch = spark.sql("""
        SELECT
            symbol,
            window(event_time, '1 minute').start AS window_start,
            sum(quantity) AS total_quantity,
            sum(quote_qty) AS total_quote_qty,
            count(*) AS trade_count,
            max_by(price, event_time) AS close_price,
            max(event_time) AS close_event_time
        FROM silver_trades_batch
        GROUP BY symbol, window(event_time, '1 minute')
    """)

    if df_vwap_batch.rdd.isEmpty():
        return

    if not is_delta_table(gold_vwap_path):
        df_vwap_initial = df_vwap_batch.selectExpr(
            "symbol",
            "window_start",
            "total_quantity",
            "total_quote_qty",
            "trade_count",
            "close_price",
            "close_event_time",
            "total_quote_qty / total_quantity AS vwap_price",
            "total_quantity / trade_count AS avg_trade_size",
            "close_price - (total_quote_qty / total_quantity) AS close_vs_vwap_diff",
            "((close_price - (total_quote_qty / total_quantity)) / (total_quote_qty / total_quantity)) * 100 AS close_vs_vwap_pct",
        )
        (df_vwap_initial.write
            .format("delta")
            .mode("append")
            .save(gold_vwap_path))
        return

    delta_table = DeltaTable.forPath(spark, gold_vwap_path)
    (delta_table.alias("target")
        .merge(
            df_vwap_batch.alias("source"),
            """
                target.symbol = source.symbol
                AND target.window_start = source.window_start
            """
        )
        .whenMatchedUpdate(set={
            "total_quantity": "target.total_quantity + source.total_quantity",
            "total_quote_qty": "target.total_quote_qty + source.total_quote_qty",
            "trade_count": "target.trade_count + source.trade_count",
            "close_price": """
                CASE
                    WHEN source.close_event_time > target.close_event_time THEN source.close_price
                    ELSE target.close_price
                END
            """,
            "close_event_time": "greatest(target.close_event_time, source.close_event_time)",
            "vwap_price": """
                (target.total_quote_qty + source.total_quote_qty)
                / (target.total_quantity + source.total_quantity)
            """,
            "avg_trade_size": """
                (target.total_quantity + source.total_quantity)
                / (target.trade_count + source.trade_count)
            """,
            "close_vs_vwap_diff": """
                (
                    CASE
                        WHEN source.close_event_time > target.close_event_time THEN source.close_price
                        ELSE target.close_price
                    END
                ) - (
                    (target.total_quote_qty + source.total_quote_qty)
                    / (target.total_quantity + source.total_quantity)
                )
            """,
            "close_vs_vwap_pct": """
                (
                    (
                        CASE
                            WHEN source.close_event_time > target.close_event_time THEN source.close_price
                            ELSE target.close_price
                        END
                    ) - (
                        (target.total_quote_qty + source.total_quote_qty)
                        / (target.total_quantity + source.total_quantity)
                    )
                ) / (
                    (target.total_quote_qty + source.total_quote_qty)
                    / (target.total_quantity + source.total_quantity)
                ) * 100
            """,
        })
        .whenNotMatchedInsert(values={
            "symbol": "source.symbol",
            "window_start": "source.window_start",
            "total_quantity": "source.total_quantity",
            "total_quote_qty": "source.total_quote_qty",
            "trade_count": "source.trade_count",
            "close_price": "source.close_price",
            "close_event_time": "source.close_event_time",
            "vwap_price": "source.total_quote_qty / source.total_quantity",
            "avg_trade_size": "source.total_quantity / source.trade_count",
            "close_vs_vwap_diff": "source.close_price - (source.total_quote_qty / source.total_quantity)",
            "close_vs_vwap_pct": """
                ((source.close_price - (source.total_quote_qty / source.total_quantity))
                / (source.total_quote_qty / source.total_quantity)) * 100
            """,
        })
        .execute())


print("3. Dang tao bang Gold OHLC_1Min theo logic incremental merge...")
query_ohlc = (df_silver.writeStream
    .foreachBatch(upsert_ohlc_1min)
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
        quote_qty,
        is_buyer_maker,
        CAST(quote_qty AS DOUBLE) AS trade_value_usdt
    FROM silver_trades_stream
    WHERE quote_qty > {whale_threshold_usdt}
""")

query_whale = (df_whale_alert.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_whale)
    .start(gold_whale_path))

print("5. Dang tao bang Gold maker_taker_flow_1min theo logic incremental merge...")
query_maker_taker = (df_silver.writeStream
    .foreachBatch(upsert_maker_taker_flow)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_maker_taker)
    .start())

print("6. Dang tao bang Gold VWAP_1Min theo logic incremental merge...")
query_vwap = (df_silver.writeStream
    .foreachBatch(upsert_vwap_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_vwap)
    .start())

print(f"   -> Gold OHLC_1Min dang duoc cap nhat tai: {gold_ohlc_path}")
print(f"   -> Gold Whale_Alert dang duoc cap nhat tai: {gold_whale_path}")
print(f"   -> Gold maker_taker_flow_1min dang duoc cap nhat tai: {gold_maker_taker_path}")
print(f"   -> Gold VWAP_1Min dang duoc cap nhat tai: {gold_vwap_path}")
print(f"   -> Nguong canh bao giao dich lon: > {whale_threshold_usdt:,.0f} USDT/lenh")

spark.streams.awaitAnyTermination()
