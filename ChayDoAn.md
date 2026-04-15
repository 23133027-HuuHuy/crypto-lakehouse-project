# Chạy đồ án từ đầu đến lúc BI kết nối Gold API

Tài liệu này chạy theo luồng: **BI -> Gold API -> Trino -> Gold**.

## 1. Điều kiện trước khi chạy

- Đã cài Docker Desktop và bật Docker Engine.
- Máy có Internet (để pull image và stream Binance).
- (Tuỳ chọn) Có file CSV batch trong `infra\workspace\` nếu muốn nạp thêm dữ liệu lịch sử.

## 2. Khởi động toàn bộ hạ tầng

Từ thư mục gốc dự án:

```powershell
Set-Location "d:\Documents\Phân Tích Dữ Liệu Lớn\Đồ Án\crypto-lakehouse-project\infra"
docker compose --profile full up -d --build
docker compose ps
```

Kỳ vọng có các service chính: `kafka`, `minio`, `stream-producer`, `stream-bronze`, `stream-silver`, `stream-gold`, `prefect-server`, `prefect-worker`, `trino`, `gold-api`, `metabase`.

## 3. Deploy Prefect flows

```powershell
docker compose exec prefect-worker sh /app/scripts/prefect_deploy.sh
```

## 4. Chạy batch (nếu có CSV trong `infra\workspace`)

```powershell
docker compose exec prefect-worker sh /app/scripts/prefect_run_batch.sh
```

> Nếu không có CSV thì pipeline streaming vẫn tự chạy và vẫn tạo dữ liệu Gold realtime.

## 5. Kiểm tra dữ liệu Gold đã sẵn sàng

Streaming services sẽ tự đẩy dữ liệu theo chuỗi:
`Kafka -> Bronze -> Silver -> Gold`.

Khi đã có dữ liệu Gold, thực hiện đăng ký bảng Delta vào Trino.

## 6. Đăng ký các bảng Gold trong Trino

Mở Trino CLI:

```powershell
docker compose exec trino trino
```

Chạy SQL (thực thi từng câu, xong câu nào chờ `Query ... finished` rồi chạy câu tiếp theo):

```sql
CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3://lakehouse/gold/');

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'ohlc_1min',
  table_location => 's3://lakehouse/gold/OHLC_1Min'
);

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'whale_alert',
  table_location => 's3://lakehouse/gold/Whale_Alert'
);

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'maker_taker_flow_1min',
  table_location => 's3://lakehouse/gold/maker_taker_flow_1min'
);

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'vwap_1min',
  table_location => 's3://lakehouse/gold/VWAP_1Min'
);

SHOW TABLES FROM delta.gold;

SELECT symbol, candle_time, close_price
FROM delta.gold.ohlc_1min
ORDER BY candle_time DESC
LIMIT 20;
```

## 7. Kiểm tra Gold API (đọc dữ liệu qua Trino)

```powershell
curl "http://localhost:5000/api/health"
curl "http://localhost:5000/api/ohlc/latest?limit=20"
```

Nếu API trả JSON thì luồng `API -> Trino -> Gold` đã sẵn sàng.

## 8. Kết nối BI vào Gold API

Ví dụ endpoint cho BI:

- `GET http://localhost:5000/api/ohlc/latest?limit=200`
- `GET http://localhost:5000/api/whale/latest?limit=100&min_value=50000`
- `GET http://localhost:5000/api/flow/latest?limit=200`
- `GET http://localhost:5000/api/vwap/latest?limit=200`
- `GET http://localhost:5000/api/dashboard/summary`

Trong Power BI có thể dùng Web connector để gọi các URL trên rồi vẽ dashboard.

## 9. URL dịch vụ

- Prefect UI: `http://localhost:4200`
- MinIO Console: `http://localhost:9001`
- Trino: `http://localhost:8080`
- Gold API: `http://localhost:5000`
- Metabase: `http://localhost:3000`
