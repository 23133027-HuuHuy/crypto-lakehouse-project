from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# 1. Khởi tạo Spark (Dùng cấu hình Spark 4.1.1 và Scala 2.13 như file Batch)
spark = (SparkSession.builder
    .appName("Lakehouse_Stream_To_Bronze")
    # KHẮC PHỤC: Nâng lên hadoop-aws 3.4.0 để khớp với hadoop-client-runtime của Spark 4.1.1
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.23.19")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Ép Spark tắt bộ vi xử lý Vectorised của Hadoop
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "normal")
    
    # KHẮC PHỤC: Thêm config timeout đúng format (milliseconds thay vì "60s")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

# 2. Đọc luồng từ Kafka
df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "binance_trades")
    .option("startingOffsets", "latest")
    .load())

# Kafka trả về dữ liệu ở cột 'value' dạng Binary, ta chuyển sang String
df_stream = df_kafka.selectExpr("CAST(value AS STRING)")

# 3. Ghi luồng vào lớp BRONZE (Nạp chồng vào cùng folder với dữ liệu Batch)
# Đây chính là điểm "Unified" của đồ án!
bronze_path = "s3a://lakehouse/bronze/batch_data"
checkpoint_path = "s3a://lakehouse/checkpoints/stream_to_bronze"

query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")  # CHO PHÉP CẬP NHẬT LẠI SCHEMA (Tự thêm cột value vào bảng có sẵn)
    .start(bronze_path))

print(f"Đang bắt đầu luồng Streaming từ Kafka sang Bronze tại: {bronze_path}")
query.awaitTermination()