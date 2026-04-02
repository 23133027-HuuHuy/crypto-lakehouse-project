"""
Script để XÓA SẠCH checkpoint và delta log bị corrupt
Chạy script này TRƯỚC KHI chạy lại pyspark_stream_to_bronze.py
"""

from pyspark.sql import SparkSession

print("🧹 BẮT ĐẦU DỌN DẸP CHECKPOINT BỊ CORRUPT...")

# Khởi tạo Spark với cấu hình mới (ĐÚNG VERSION)
spark = (SparkSession.builder
    .appName("Cleanup_Corrupt_Checkpoints")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.23.19")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Đường dẫn cần dọn
checkpoint_path = "s3a://lakehouse/checkpoints/stream_to_bronze"
bronze_delta_log = "s3a://lakehouse/bronze/batch_data/_delta_log"

print(f"\n1️⃣ Xóa checkpoint tại: {checkpoint_path}")
try:
    # Lấy Hadoop FileSystem để xóa
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(checkpoint_path), hadoop_conf)
    
    checkpoint_path_obj = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
    if fs.exists(checkpoint_path_obj):
        fs.delete(checkpoint_path_obj, True)  # True = recursive delete
        print(f"   ✅ Đã xóa checkpoint: {checkpoint_path}")
    else:
        print(f"   ℹ️ Checkpoint không tồn tại (OK)")
except Exception as e:
    print(f"   ⚠️ Lỗi khi xóa checkpoint: {e}")

print(f"\n2️⃣ Xóa Delta transaction log bị corrupt tại: {bronze_delta_log}")
try:
    delta_log_obj = spark._jvm.org.apache.hadoop.fs.Path(bronze_delta_log)
    if fs.exists(delta_log_obj):
        fs.delete(delta_log_obj, True)
        print(f"   ✅ Đã xóa _delta_log: {bronze_delta_log}")
    else:
        print(f"   ℹ️ _delta_log không tồn tại (OK)")
except Exception as e:
    print(f"   ⚠️ Lỗi khi xóa _delta_log: {e}")

print("\n3️⃣ TẠO LẠI Delta Lake table mới (để tạo _delta_log mới)")
try:
    bronze_path = "s3a://lakehouse/bronze/batch_data"
    
    # Đọc dữ liệu Parquet hiện có (nếu có)
    from pyspark.sql.utils import AnalysisException
    try:
        df_existing = spark.read.format("parquet").load(bronze_path)
        print(f"   📊 Tìm thấy {df_existing.count()} dòng dữ liệu cũ")
        
        # Ghi đè lại dưới dạng Delta mới
        df_existing.write \
            .format("delta") \
            .mode("overwrite") \
            .save(bronze_path)
        print(f"   ✅ Đã tạo lại Delta table với _delta_log MỚI")
        
    except AnalysisException:
        print(f"   ℹ️ Không có dữ liệu cũ (sẽ tạo mới khi stream chạy)")
        
except Exception as e:
    print(f"   ⚠️ Lỗi khi tạo lại Delta table: {e}")

spark.stop()

print("\n🎉 HOÀN TẤT! Bây giờ bạn có thể chạy lại:")
print("   python processing/pyspark_stream_to_bronze.py")
