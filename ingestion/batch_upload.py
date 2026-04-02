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

# Trỏ đến thư mục chứa file
local_data_path = r"D:\Term2_Pharse1\BigDataAnalysyst\PROJECT_BIGDATA\crypto-lakehouse-project\crypto-lakehouse-project\infra\workspace"

# --- THỰC HIỆN ---

# 1. Kiểm tra và tạo Bucket
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"Đã tạo bucket: {bucket_name}")

# 2. Duyệt qua thư mục để upload
print("Đang bắt đầu nạp dữ liệu thô (Raw Data) vào MinIO...")

if os.path.isdir(local_data_path):
    for file_name in os.listdir(local_data_path):
        # Chỉ lấy các file CSV
        if file_name.endswith(".csv"):
            file_path = os.path.join(local_data_path, file_name)
            
           
            minio_path = f"raw_data/batch/{file_name}"
            
            # 3. Thực hiện Upload
            client.fput_object(bucket_name, minio_path, file_path)
            print(f"Đã nạp thành công file {file_name} vào lớp Raw!")
else:
    print(f"Lỗi: Đường dẫn {local_data_path} không phải là một thư mục hợp lệ!")

print("NHIỆM VỤ HOÀN THÀNH: File CSV đã nằm ở lớp Raw trên MinIO, sẵn sàng cho Spark xử lý!")