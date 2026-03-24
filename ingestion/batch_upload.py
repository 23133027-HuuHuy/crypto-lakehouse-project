import os
from minio import Minio

# --- CẤU HÌNH KẾT NỐI ---
client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket_name = "lakehouse"

# CHỈNH LẠI: Trỏ đến thư mục chứa các file, không trỏ đến 1 file duy nhất
local_data_path = r"D:\Term2_Pharse1\BigDataAnalysyst\PROJECT_BIGDATA\crypto-lakehouse-project\crypto-lakehouse-project\infra\workspace"

# --- THỰC HIỆN ---

# 1. Kiểm tra và tạo Bucket
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"✔️ Đã tạo bucket: {bucket_name}")

# 2. Duyệt qua thư mục để upload
print("Đang bắt đầu nạp dữ liệu vào lớp Bronze...")

# Kiểm tra xem đường dẫn có tồn tại không trước khi chạy
if os.path.isdir(local_data_path):
    for file_name in os.listdir(local_data_path):
        # Chỉ lấy các file CSV
        if file_name.endswith(".csv"):
            file_path = os.path.join(local_data_path, file_name)
            
            # Cấu trúc đích trên MinIO theo đúng Medallion Architecture
            minio_path = f"bronze/crypto/batch/{file_name}"
            
            # 3. Thực hiện Upload
            client.fput_object(bucket_name, minio_path, file_path)
            print(f"Đã nạp thành công: {file_name}")
else:
    print(f"Lỗi: Đường dẫn {local_data_path} không phải là một thư mục hợp lệ!")

print("NHIỆM VỤ HOÀN THÀNH: Dữ liệu đã nằm an toàn trong MinIO!")