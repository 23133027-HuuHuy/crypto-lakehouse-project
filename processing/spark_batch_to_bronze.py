from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

print("1. Đang khởi tạo Spark Session và tải các thư viện kết nối MinIO, Delta Lake...")
print("   (Lưu ý: Lần chạy đầu tiên sẽ mất 1-3 phút để tải file .jar, vui lòng đợi!)")

# Khởi tạo Spark với cấu hình thư viện (.jar) để nói chuyện với S3 (MinIO) và Delta
spark = (SparkSession.builder
    .appName("Lakehouse_Batch_Raw_To_Bronze")
    # Cập nhật Delta 4.1.0 và Scala 2.13 cho khớp với PySpark 4.1.1
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.23.19")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

# Tắt bớt log rác của Spark cho dễ nhìn terminal
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema dạng String (Giữ nguyên vẹn dữ liệu thô)
bronze_schema = StructType([
    StructField("Trade_ID", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Quote_Qty", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("is_Buyer_Maker", StringType(), True),
    StructField("is_Best_Match", StringType(), True)
])

# Đường dẫn trên MinIO
# Đường dẫn thư mục chứa file CSV 1GB của bạn
raw_path = "s3a://lakehouse/raw_data/batch/" 
# Nơi lưu kết quả Delta Lake
bronze_path = "s3a://lakehouse/bronze/batch_data"

print("\n2. Đang đọc dữ liệu từ lớp RAW (CSV)...")
# Đọc file CSV không có Header, ốp Schema vào
df_raw = spark.read.csv(raw_path, schema=bronze_schema, header=False)

print("   -> Xem thử 5 dòng dữ liệu chuẩn bị ghi xuống Bronze:")
df_raw.show(5)

print("3. Đang ghi dữ liệu xuống lớp BRONZE dưới định dạng Delta Lake...")
# Ghi đè (overwrite) xuống MinIO bằng định dạng delta
df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

print(f"\n✅ HOÀN TẤT! Dữ liệu Lô (Batch) đã được đưa vào Lakehouse thành công tại: {bronze_path}")

# Dừng Spark
spark.stop()