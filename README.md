<div align="center">

# Real-time Crypto Data Lakehouse

### Unified Batch & Streaming Pipeline with Spark, Kafka, Delta Lake, MinIO and Prefect

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-4.1.0-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Prefect](https://img.shields.io/badge/Prefect-2.20.25-1565C0?logo=prefect&logoColor=white)](https://www.prefect.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

**Đồ án môn Phân tích Dữ liệu lớn - HCMUTE**

Hệ thống Data Lakehouse xử lý dữ liệu giao dịch BTC/USDT theo cả hai chế độ: batch từ file CSV lịch sử và streaming real-time từ Binance WebSocket.

</div>

---

## Mục Lục

- [Tổng Quan](#tổng-quan)
- [Kiến Trúc](#kiến-trúc)
- [Luồng Dữ Liệu](#luồng-dữ-liệu)
- [Công Nghệ](#công-nghệ)
- [Cấu Trúc Project](#cấu-trúc-project)
- [Hướng Dẫn Chạy](#hướng-dẫn-chạy)
- [Trino, Gold API Và BI](#trino-gold-api-và-bi)
- [Prefect](#prefect)
- [Lệnh Hữu Ích](#lệnh-hữu-ích)
- [Giấy Phép](#giấy-phép)

---

## Tổng Quan

Project xây dựng một nền tảng lakehouse cho dữ liệu crypto, kết hợp:

- **Batch ingestion:** đọc các file CSV lịch sử trong `infra/workspace`.
- **Streaming ingestion:** nhận trade real-time từ Binance WebSocket và đẩy vào Kafka topic `binance_trades`.
- **Medallion Architecture:** chuẩn hóa dữ liệu qua các tầng `Bronze -> Silver -> Gold`.
- **Delta Lake trên MinIO:** lưu dữ liệu dạng Delta table trên object storage S3-compatible.
- **Prefect orchestration:** tự động deploy và trigger batch flow.
- **Gold API:** cung cấp REST API để BI tools đọc dữ liệu Gold thông qua Trino.

Nguồn dữ liệu chính:

| Loại | Nguồn | Mô tả |
|---|---|---|
| Batch | CSV local | File giao dịch BTC/USDT đặt trong `infra/workspace/*.csv` |
| Streaming | Binance WebSocket | Stream `btcusdt@aggTrade` theo thời gian thực |

---

## Kiến Trúc

```text
Binance WebSocket
       |
       v
stream-producer  ->  Kafka topic: binance_trades
       |                         |
       |                         v
       |                 stream-bronze
       |                         |
CSV files                         v
infra/workspace/*.csv  ->  Bronze: all_crypto_trades
                                 |
                                 v
                         Silver: btc_trades
                                 |
                                 v
            Gold: OHLC, Whale Alert, Maker/Taker Flow, VWAP
                                 |
                                 v
                         Trino / Gold API / BI
```

Hạ tầng chạy bằng Docker Compose:

| Nhóm | Service | Vai trò |
|---|---|---|
| Streaming | `stream-producer` | Đọc Binance WebSocket và gửi JSON vào Kafka |
| Streaming | `stream-bronze` | Đọc Kafka, append vào Delta Bronze |
| Streaming | `stream-silver` | Đọc Bronze, parse/clean/dedupe, ghi Silver |
| Streaming | `stream-gold` | Đọc Silver, tạo các bảng Gold analytics |
| Storage | `minio` | Object storage lưu Delta tables |
| Message broker | `kafka` | Kafka KRaft, không dùng ZooKeeper |
| Orchestration | `prefect-server`, `prefect-worker` | UI/API, worker và batch deployment |
| Optional BI | `trino`, `gold-api`, `metabase` | Query Gold, REST API và dashboard |

---

## Luồng Dữ Liệu

### Bronze

Đường dẫn:

```text
s3a://lakehouse/bronze/all_crypto_trades
```

Bronze là bảng Delta hợp nhất:

- Batch CSV được ghi bởi `processing/spark_batch_to_bronze.py`.
- Streaming Kafka được ghi bởi `processing/pyspark_stream_to_bronze.py`.
- Schema được merge để cùng chứa dữ liệu CSV và cột `value` từ Kafka JSON.

### Silver

Đường dẫn:

```text
s3a://lakehouse/silver/btc_trades
```

Silver được tạo bởi `processing/pyspark_bronze_to_silver.py`:

- Parse JSON từ Kafka.
- Chuẩn hóa schema CSV và stream về cùng một định dạng.
- Tạo `event_id` để dedupe và upsert idempotent.
- Lọc dữ liệu lỗi như giá, khối lượng hoặc thời gian không hợp lệ.

Schema chính:

| Cột | Ý nghĩa |
|---|---|
| `event_id` | Khóa định danh giao dịch |
| `symbol` | Cặp giao dịch, mặc định `BTCUSDT` |
| `price` | Giá giao dịch |
| `quantity` | Khối lượng |
| `quote_qty` | Giá trị theo USDT |
| `event_time` | Thời gian giao dịch |
| `is_buyer_maker` | Maker/taker side từ Binance |

### Gold

Gold được tạo bởi `processing/pyspark_silver_to_gold.py`.

| Bảng Delta | Đường dẫn | Nội dung |
|---|---|---|
| `OHLC_1Min` | `s3a://lakehouse/gold/OHLC_1Min` | Nến 1 phút: open, high, low, close, volume |
| `Whale_Alert` | `s3a://lakehouse/gold/Whale_Alert` | Giao dịch lớn trên 50,000 USDT |
| `maker_taker_flow_1min` | `s3a://lakehouse/gold/maker_taker_flow_1min` | Dòng mua/bán chủ động theo phút |
| `VWAP_1Min` | `s3a://lakehouse/gold/VWAP_1Min` | VWAP, average trade size, độ lệch close so với VWAP |

---

## Công Nghệ

| Thành phần | Công nghệ |
|---|---|
| Streaming broker | Apache Kafka 7.7.0, KRaft mode |
| Stream source | Binance WebSocket API |
| Processing | PySpark 4.1.1, Spark Structured Streaming |
| Table format | Delta Lake 4.1.0 |
| Object storage | MinIO |
| Orchestration | Prefect 2.20.25 |
| Query engine | Trino 443 |
| API | Flask, Flask-CORS, Trino Python client |
| Dashboard | Metabase, hoặc BI tool gọi Gold API |
| Runtime | Docker Compose |

---

## Cấu Trúc Project

```text
crypto-lakehouse-project/
|-- api/
|   |-- gold_api.py                    # REST API đọc Gold qua Trino
|   `-- requirements.txt
|-- infra/
|   |-- docker-compose.yml             # Kafka, MinIO, streaming, Prefect, optional BI
|   |-- Dockerfile.prefect
|   |-- Dockerfile.spark
|   |-- Dockerfile.stream
|   `-- trino/catalog/delta.properties
|-- ingestion/
|   |-- stream_to_kafka.py             # Binance -> Kafka
|   `-- batch_upload.py                # Upload CSV vào MinIO raw_data, legacy/helper
|-- orchestration/
|   |-- batch_flow.py                  # Prefect batch flow
|   |-- monitor_flow.py                # Prefect monitor flow
|   `-- deployments/
|-- processing/
|   |-- spark_batch_to_bronze.py       # CSV -> Bronze
|   |-- pyspark_stream_to_bronze.py    # Kafka -> Bronze
|   |-- pyspark_bronze_to_silver.py    # Bronze -> Silver
|   |-- pyspark_silver_to_gold.py      # Silver -> Gold
|   |-- check_data_silver.py
|   |-- clean_old_medallion_data.py
|   `-- clean_corrupt_checkpoints.py
|-- scripts/
|   |-- prefect_run_batch.sh
|   |-- prefect_run_monitor.sh
|   |-- prefect_deploy.sh
|   `-- run_trino_register_gold.ps1
|-- prefect.yaml
|-- ChayDoAn.md
|-- HuongDanChayDoAn.md
`-- README.md
```

---

## Hướng Dẫn Chạy

### Yêu Cầu

- Docker Desktop đang chạy.
- Máy có Internet để pull image, tải Spark packages và nhận stream Binance.
- RAM tối thiểu 8 GB, khuyến nghị 16 GB.
- Nếu chạy batch, đặt file CSV vào:

```text
infra/workspace/
```

Ví dụ:

```text
infra/workspace/BTCUSDT-trades-*.csv
```

### Chạy Pipeline Cốt Lõi

Từ thư mục gốc project:

```powershell
cd infra
docker compose up -d --build
docker compose ps
```

Sau khi chạy lệnh trên:

- Kafka và MinIO khởi động.
- `stream-producer`, `stream-bronze`, `stream-silver`, `stream-gold` chạy liên tục.
- Prefect Server mở tại `http://localhost:4200`.
- Prefect Worker tự tạo `default-pool`, deploy flow trong `prefect.yaml` và bắt đầu nhận job.

### Chạy Batch Ingestion

Nếu có CSV trong `infra/workspace`, trigger batch flow:

```powershell
docker compose exec prefect-worker sh /app/scripts/prefect_run_batch.sh
```

Batch flow sẽ chạy:

```text
processing/spark_batch_to_bronze.py
```

Dữ liệu CSV được append vào Bronze, sau đó các service streaming Silver và Gold sẽ tự xử lý tiếp.

### URL Dịch Vụ Cơ Bản

| Service | URL | Ghi chú |
|---|---|---|
| Prefect UI | http://localhost:4200 | Theo dõi deployment và flow run |
| MinIO Console | http://localhost:9001 | `admin` / `password123` |

---

## Trino, Gold API Và BI

Các service BI là optional và chỉ chạy khi bật profile `full`.

```powershell
cd infra
docker compose --profile full up -d --build
docker compose ps
```

URL optional:

| Service | URL |
|---|---|
| Trino | http://localhost:8080 |
| Gold API | http://localhost:5000 |
| Metabase | http://localhost:3000 |

### Đăng Ký Bảng Gold Vào Trino

Sau khi Gold đã có dữ liệu, quay về thư mục gốc project và chạy script:

```powershell
cd ..
.\scripts\run_trino_register_gold.ps1
```

Script sẽ:

- Tạo schema `delta.gold` nếu chưa có.
- Register 4 bảng Gold: `ohlc_1min`, `whale_alert`, `maker_taker_flow_1min`, `vwap_1min`.
- Chạy query mẫu để kiểm tra.

### Gold API

API đọc dữ liệu từ Trino:

| Endpoint | Mô tả |
|---|---|
| `GET /api/health` | Kiểm tra kết nối API -> Trino |
| `GET /api/ohlc/latest?limit=100` | Dữ liệu OHLC mới nhất |
| `GET /api/whale/latest?limit=50&min_value=50000` | Giao dịch lớn |
| `GET /api/flow/latest?limit=100` | Maker/taker flow |
| `GET /api/vwap/latest?limit=100` | VWAP |
| `GET /api/dashboard/summary` | Summary cho dashboard |
| `GET /api/stats` | Số dòng từng bảng Gold |

Ví dụ:

```powershell
curl "http://localhost:5000/api/health"
curl "http://localhost:5000/api/ohlc/latest?limit=20"
curl "http://localhost:5000/api/dashboard/summary?symbol=BTCUSDT"
```

---

## Prefect

File `prefect.yaml` hiện deploy deployment:

```text
lakehouse-batch-flow/batch-prod
```

Lịch chạy:

```text
0 */2 * * *  Asia/Ho_Chi_Minh
```

Worker trong `infra/docker-compose.yml` tự chạy:

```text
prefect work-pool create default-pool --type process
prefect deploy --all
prefect worker start --pool default-pool
```

Trigger thủ công:

```powershell
docker compose exec prefect-worker sh /app/scripts/prefect_run_batch.sh
```

Project cũng có `orchestration/monitor_flow.py` và script:

```powershell
docker compose exec prefect-worker sh /app/scripts/prefect_run_monitor.sh
```

Lưu ý: `prefect_run_monitor.sh` cần deployment `lakehouse-stream-monitor-flow/monitor-prod`. Nếu muốn dùng monitor flow bằng Prefect, cần deploy thêm deployment này hoặc bổ sung vào `prefect.yaml`.

---

## Lệnh Hữu Ích

### Xem Log

```powershell
cd infra
docker compose logs -f stream-producer
docker compose logs -f stream-bronze
docker compose logs -f stream-silver
docker compose logs -f stream-gold
docker compose logs -f prefect-worker
```

### Kiểm Tra Silver

```powershell
cd infra
docker compose exec stream-silver python /app/processing/check_data_silver.py
```

### Dọn Checkpoint Kafka -> Bronze Bị Lỗi

```powershell
cd infra
docker compose exec stream-bronze python /app/processing/clean_corrupt_checkpoints.py
```

### Dọn Toàn Bộ Bronze, Silver, Gold Và Checkpoints

Lệnh này xóa dữ liệu medallion trên MinIO, dùng khi muốn chạy lại từ đầu.

```powershell
cd infra
docker compose exec stream-bronze python /app/processing/clean_old_medallion_data.py
```

### Dừng Hệ Thống

```powershell
cd infra
docker compose down
```

Nếu muốn xóa cả Docker volumes:

```powershell
docker compose down -v
```

---

## Ghi Chú Vận Hành

- `stream-*` là các service chạy liên tục và có `restart: unless-stopped`.
- Batch không upload CSV qua MinIO raw layer; code hiện đọc trực tiếp CSV local rồi ghi vào Bronze.
- `ingestion/batch_upload.py` vẫn tồn tại như helper/legacy script, nhưng không nằm trong flow chính hiện tại.
- MinIO bucket mặc định là `lakehouse`.
- Credentials mặc định chỉ phù hợp cho môi trường học tập/local demo, không dùng trực tiếp cho production thật.

---

## Giấy Phép

Dự án được phát hành theo giấy phép [MIT License](LICENSE).

<div align="center">

**HCMUTE - Khoa Công nghệ Thông tin**

Đồ án Phân tích Dữ liệu lớn - Học kỳ 2, 2025-2026

</div>
