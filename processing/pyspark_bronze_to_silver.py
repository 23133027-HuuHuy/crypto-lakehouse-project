from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Khởi tạo Spark (Cấu hình đầy đủ MinIO và Delta)
spark = (SparkSession.builder
    .appName("Lakehouse_Bronze_To_Silver")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.767")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

# Tắt log rác
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema chuẩn của Binance để bóc tách JSON
# (Dựa trên dữ liệu 'p', 'q', 'T' từ stream_to_kafka.py)
json_schema = StructType([
    StructField("p", StringType(), True), # Price
    StructField("q", StringType(), True), # Quantity
    StructField("T", LongType(), True),   # Event time
    StructField("s", StringType(), True)  # Symbol (BTCUSDT)
])

# 3. Đọc luồng dữ liệu từ lớp BRONZE
bronze_path = "s3a://lakehouse/bronze/batch_data"
# Đọc dưới dạng Stream để bắt dữ liệu liên tục chảy từ Bronze
df_bronze = spark.readStream.format("delta").load(bronze_path)

# Thêm hàm coalesce và lit để xử lý hợp nhất từ Batch và Stream
from pyspark.sql.functions import coalesce, lit, when

# 4. Xử lý làm sạch đa luồng (Handling both Batch & Stream struct)
df_silver = (df_bronze
    # Kiểm tra: nếu có cột 'value' (từ Stream) thì parse JSON, còn không thì None
    .withColumn("stream_data", when(col("value").isNotNull(), from_json(col("value"), json_schema)).otherwise(None))
    .select(
        coalesce(col("stream_data.s"), lit("BTCUSDT")).alias("symbol"),
        
        # Nếu Stream có giá thì lấy, nếu không lấy cột Price của luồng Batch
        coalesce(col("stream_data.p"), col("Price")).cast(DoubleType()).alias("price"),
        coalesce(col("stream_data.q"), col("Quantity")).cast(DoubleType()).alias("quantity"),
        
        # Xử lý thời gian (Unix miliseconds sang Timestamp)
        coalesce(
            to_timestamp(from_unixtime(col("stream_data.T") / 1000)),
            to_timestamp(from_unixtime(col("Timestamp").cast(DoubleType()) / 1000))
        ).alias("event_time")
    )
    .filter(col("price").isNotNull() & (col("price") > 0)) # Loại bỏ dữ liệu rác/lỗi
)

# 5. Ghi xuống lớp SILVER
silver_path = "s3a://lakehouse/silver/btc_trades"
checkpoint_silver = "s3a://lakehouse/checkpoints/bronze_to_silver"

query = (df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_silver)
    .start(silver_path))

print(f"✨ Lớp Silver đang được tinh chế tại: {silver_path}")
query.awaitTermination()