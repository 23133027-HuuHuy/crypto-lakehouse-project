import os

# Đường dẫn file 13.1GB hiện tại của bạn
input_file = r'D:\Term2_Pharse1\BigDataAnalysyst\PROJECT_BIGDATA\crypto-lakehouse-project\crypto-lakehouse-project\infra\workspace\BTCUSDT-trades-2026-02.csv' 
# Tên file mới bạn muốn tạo ra
output_file = r'D:\Term2_Pharse1\BigDataAnalysyst\PROJECT_BIGDATA\crypto-lakehouse-project\crypto-lakehouse-project\infra\workspace\BTCUSDT-trades.csv'

# Mức dung lượng muốn cắt (1 GB = 1024 * 1024 * 1024 bytes)
target_size_bytes = 1 * 1024 * 1024 * 1024 

current_size = 0
row_count = 0

print(f"Bắt đầu cắt file... Mục tiêu: 1GB")

# Mở file gốc để đọc, file mới để ghi
with open(input_file, 'r', encoding='utf-8') as f_in, \
     open(output_file, 'w', encoding='utf-8') as f_out:
    
    # Đọc và ghi dòng Tiêu đề (Header) đầu tiên
    header = f_in.readline()
    f_out.write(header)
    
    # Đọc từng dòng tiếp theo
    for line in f_in:
        f_out.write(line)
        
        # Cộng dồn dung lượng của dòng vừa ghi (tính bằng bytes)
        current_size += len(line.encode('utf-8'))
        row_count += 1
        
        # In tiến độ cho mỗi 1 triệu dòng để bạn biết máy không bị treo
        if row_count % 1000000 == 0:
            print(f"Đã cắt được {row_count} dòng... Dung lượng: {current_size / (1024*1024):.2f} MB")
        
        # Nếu dung lượng đạt mốc 1GB thì dừng vòng lặp
        if current_size >= target_size_bytes:
            print("ĐÃ ĐẠT 1GB. DỪNG CẮT!")
            break

print(f"✅ Hoàn tất! File mới có {row_count} dòng, lưu tại: {output_file}")