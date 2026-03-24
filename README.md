<div align="center">

# 🏗️ Real-time Crypto Data Lakehouse Architecture

### Unified Batch & Streaming Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

> 🎓 **Đồ án môn Phân tích Dữ liệu lớn (Big Data Analytics)**
>
> Trường Đại học Sư phạm Kỹ thuật TP.HCM (HCMUTE)

---

*Xây dựng hệ thống Data Lakehouse 5 lớp xử lý **15GB** dữ liệu Bitcoin lịch sử và luồng **Real-time** từ Binance API, sử dụng Spark, Kafka và Delta Lake với kiến trúc **Medallion Architecture**.*

</div>

---

## 📋 Mục lục

- [🔭 Tổng quan dự án](#-tổng-quan-dự-án)
- [🏛️ Kiến trúc hệ thống](#️-kiến-trúc-hệ-thống)
- [🥇 Medallion Architecture](#-medallion-architecture)
- [🧬 ACID & Time Travel với Delta Lake](#-acid--time-travel-với-delta-lake)
- [🛠️ Công nghệ sử dụng](#️-công-nghệ-sử-dụng)
- [📁 Cấu trúc dự án](#-cấu-trúc-dự-án)
- [🚀 Hướng dẫn cài đặt](#-hướng-dẫn-cài-đặt)
- [🎬 Demo Script](#-demo-script)
- [📊 Kết quả đạt được](#-kết-quả-đạt-được)
- [📝 Giấy phép](#-giấy-phép)

---

## 🔭 Tổng quan dự án

Dự án xây dựng một hệ thống **Data Lakehouse** hoàn chỉnh, kết hợp ưu điểm của cả Data Lake (lưu trữ linh hoạt, chi phí thấp) và Data Warehouse (quản lý schema, truy vấn nhanh) để xử lý dữ liệu giao dịch tiền điện tử Bitcoin (BTC/USDT).

### 🎯 Mục tiêu chính

| # | Mục tiêu | Mô tả |
|---|----------|--------|
| 1 | **Unified Pipeline** | Xây dựng pipeline thống nhất xử lý cả **Batch** (15GB dữ liệu lịch sử) và **Streaming** (luồng real-time từ Binance) |
| 2 | **Medallion Architecture** | Triển khai kiến trúc dữ liệu 3 tầng: **Bronze → Silver → Gold** để đảm bảo chất lượng dữ liệu |
| 3 | **ACID Compliance** | Đảm bảo tính toàn vẹn dữ liệu với **Delta Lake** (Atomicity, Consistency, Isolation, Durability) |
| 4 | **Modern Infrastructure** | Sử dụng hạ tầng hiện đại, hoàn toàn **Dockerized**, dễ dàng triển khai và mở rộng |

### 📦 Nguồn dữ liệu

| Loại | Nguồn | Mô tả | Khối lượng |
|------|--------|--------|------------|
| 📂 **Batch** | BTC Tick-Data CSV | Dữ liệu giao dịch lịch sử BTC/USDT theo từng tick | **~15 GB** (13.1GB đã nạp) |
| ⚡ **Streaming** | Binance WebSocket API | Luồng giao dịch real-time `btcusdt@aggTrade` | Liên tục, vô hạn |

---

## 🏛️ Kiến trúc hệ thống

Hệ thống được thiết kế theo mô hình **5 lớp (5-Layer Architecture)**, mỗi lớp đảm nhận một chức năng riêng biệt:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    🏗️ CRYPTO DATA LAKEHOUSE                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ LỚP 5: CONSUMPTION (Tiêu thụ dữ liệu)                     │   │
│  │   📊 Dashboard  │  📈 Analytics  │  🤖 ML Models            │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 4: COMPUTE (Xử lý - Apache Spark)                      │   │
│  │   🔄 Spark Master ←→ Spark Worker                            │   │
│  │   • Batch Processing (SparkSQL)                              │   │
│  │   • Stream Processing (Structured Streaming)                 │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 3: METADATA (Quản lý siêu dữ liệu - Delta Lake)       │   │
│  │   📋 Delta Transaction Log  │  🕐 Time Travel               │   │
│  │   🔒 ACID Transactions      │  📐 Schema Enforcement        │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 2: STORAGE (Lưu trữ - MinIO S3)                        │   │
│  │   🥉 Bronze (Raw)  →  🥈 Silver (Clean)  →  🥇 Gold (Agg)  │   │
│  │   Object Storage tương thích S3 API                          │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 1: INGESTION (Nạp dữ liệu)                             │   │
│  │   📂 Batch: CSV → MinIO     ⚡ Stream: Binance → Kafka      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Chi tiết từng lớp

#### 🔽 Lớp 1: Ingestion (Nạp dữ liệu)

Lớp chịu trách nhiệm thu thập dữ liệu từ các nguồn bên ngoài vào hệ thống.

| Kênh | Công nghệ | Nguồn | Đích |
|------|-----------|--------|------|
| **Batch** | Python + MinIO SDK | File CSV 15GB trên Local | MinIO `bronze/crypto/batch/` |
| **Streaming** | Python + Binance WebSocket + `confluent_kafka` | Binance API real-time | Kafka topic `binance_trades` |

#### 💾 Lớp 2: Storage (Lưu trữ)

**MinIO** đóng vai trò là Object Storage tương thích S3 API. Tổ chức dữ liệu theo **Medallion Architecture** trong bucket `lakehouse`:

```
lakehouse/
├── bronze/crypto/batch/    ← Dữ liệu thô, chưa xử lý
├── silver/crypto/          ← Dữ liệu đã làm sạch, chuẩn hóa
└── gold/crypto/            ← Dữ liệu tổng hợp, sẵn sàng phân tích
```

#### 📋 Lớp 3: Metadata (Quản lý siêu dữ liệu)

**Delta Lake** hoạt động như lớp metadata, cung cấp:
- 🔒 **ACID Transactions**: Đảm bảo mọi thao tác ghi đều nguyên tử (atomic)
- 📐 **Schema Enforcement**: Kiểm tra và bắt buộc cấu trúc dữ liệu
- 🕐 **Time Travel**: Cho phép truy vấn dữ liệu tại bất kỳ thời điểm nào trong quá khứ
- 📋 **Transaction Log**: Ghi nhận toàn bộ lịch sử thay đổi của dữ liệu

#### ⚙️ Lớp 4: Compute (Xử lý)

**Apache Spark** cluster (1 Master + 1 Worker) thực hiện:
- **Batch Processing**: SparkSQL để transform dữ liệu Bronze → Silver → Gold
- **Stream Processing**: Structured Streaming đọc từ Kafka và ghi vào Delta Lake
- **Data Quality**: Loại bỏ bản ghi lỗi, chuẩn hóa kiểu dữ liệu, tính toán aggregation

#### 📊 Lớp 5: Consumption (Tiêu thụ)

Lớp cuối cùng cung cấp dữ liệu đã xử lý cho các ứng dụng phân tích:
- Dashboard trực quan hóa
- Báo cáo phân tích
- Input cho các mô hình Machine Learning

---

## 🥇 Medallion Architecture

> ⭐ **Đây là kiến trúc cốt lõi của dự án** — Được giáo viên yêu cầu triển khai.

Medallion Architecture (Kiến trúc Huy chương) chia dữ liệu thành **3 tầng chất lượng** tăng dần:

```
   ┌──────────┐         ┌──────────┐         ┌──────────┐
   │  🥉      │  ETL    │  🥈      │  ETL    │  🥇      │
   │  BRONZE  │ ──────► │  SILVER  │ ──────► │   GOLD   │
   │  (Raw)   │         │ (Clean)  │         │  (Agg)   │
   └──────────┘         └──────────┘         └──────────┘
    Dữ liệu thô         Đã làm sạch         Sẵn sàng
    từ nguồn gốc         chuẩn hóa           phân tích
```

| Tầng | Trạng thái | Mô tả | Ví dụ trong dự án |
|------|-----------|--------|-------------------|
| 🥉 **Bronze** | Raw Data | Dữ liệu thô, giữ nguyên định dạng gốc. Mọi dữ liệu đều đổ vào đây trước. | File CSV gốc & JSON từ Kafka |
| 🥈 **Silver** | Cleaned Data | Dữ liệu đã được **loại bỏ lỗi**, **chuẩn hóa kiểu** (cast type), **loại bỏ trùng lặp**. | Bảng Delta với schema rõ ràng |
| 🥇 **Gold** | Aggregated Data | Dữ liệu đã **tổng hợp** (aggregation), sẵn sàng để phân tích và báo cáo. | OHLCV theo phút/giờ/ngày |

### Tại sao cần Medallion Architecture?

1. **Traceability** (Truy vết): Luôn có thể truy ngược lại dữ liệu gốc tại Bronze
2. **Data Quality** (Chất lượng): Mỗi tầng tăng thêm một lớp kiểm tra chất lượng
3. **Reprocessing** (Xử lý lại): Nếu logic xử lý sai, chỉ cần chạy lại từ Bronze mà không mất dữ liệu gốc
4. **Separation of Concerns**: Tách biệt giữa lưu trữ thô, làm sạch, và phân tích

---

## 🧬 ACID & Time Travel với Delta Lake

> ⭐ **ACID Compliance là yêu cầu bắt buộc** — Delta Lake đảm bảo tính toàn vẹn dữ liệu cho toàn bộ pipeline.

### Tính chất ACID

| Tính chất | Ý nghĩa | Áp dụng trong dự án |
|-----------|---------|---------------------|
| **A**tomicity (Nguyên tử) | Mọi thao tác ghi đều thành công hoàn toàn hoặc không ghi gì cả | Đảm bảo không có file lỗi nửa chừng trong MinIO |
| **C**onsistency (Nhất quán) | Dữ liệu luôn ở trạng thái hợp lệ sau mỗi transaction | Schema được enforce bởi Delta Lake |
| **I**solation (Cô lập) | Các thao tác đọc/ghi song song không ảnh hưởng lẫn nhau | Batch và Streaming có thể ghi đồng thời an toàn |
| **D**urability (Bền vững) | Dữ liệu đã commit sẽ không bị mất ngay cả khi hệ thống sập | Transaction log được lưu trữ bền vững trên MinIO |

### Time Travel (Du hành thời gian)

Delta Lake cho phép truy vấn dữ liệu tại các phiên bản trước đó:

```sql
-- Xem dữ liệu ở phiên bản cụ thể (version)
SELECT * FROM delta.`s3a://lakehouse/gold/crypto/` VERSION AS OF 3;

-- Xem dữ liệu tại thời điểm cụ thể (timestamp)
SELECT * FROM delta.`s3a://lakehouse/silver/crypto/` TIMESTAMP AS OF '2026-03-24 10:00:00';

-- Xem lịch sử thay đổi
DESCRIBE HISTORY delta.`s3a://lakehouse/gold/crypto/`;
```

---

## 🛠️ Công nghệ sử dụng

| Lớp | Công nghệ | Phiên bản | Vai trò |
|-----|-----------|-----------|---------|
| 🔽 Ingestion | **Apache Kafka** (KRaft Mode) | `confluentinc/cp-kafka:latest` | Message Broker cho luồng streaming. Chạy chế độ **KRaft** — loại bỏ hoàn toàn Zookeeper |
| 🔽 Ingestion | **Binance WebSocket API** | — | Nguồn dữ liệu giao dịch BTC real-time |
| 💾 Storage | **MinIO** | `minio/minio:latest` | Object Storage tương thích S3 API. Lưu trữ toàn bộ dữ liệu Medallion |
| 📋 Metadata | **Delta Lake** | Tích hợp Spark | Quản lý ACID transactions, Schema, Time Travel |
| ⚙️ Compute | **Apache Spark** | `bitnamilegacy/spark:latest` | Distributed compute engine (1 Master + 1 Worker) |
| 🐳 Infra | **Docker Compose** | — | Container orchestration cho toàn bộ hệ thống |
| 🐍 Client | **Python 3** | — | Script nạp dữ liệu Batch & Streaming |

### Thư viện Python

| Thư viện | Mục đích |
|----------|----------|
| `minio` | SDK kết nối và upload dữ liệu lên MinIO |
| `confluent-kafka` | Kafka Producer — đẩy dữ liệu real-time vào topic |
| `websocket-client` | Kết nối Binance WebSocket API |

---

## 📁 Cấu trúc dự án

```
crypto-lakehouse-project/
│
├── 📄 README.md                        ← Tài liệu dự án (file này)
├── 📄 LICENSE                          ← Giấy phép MIT
├── 📄 .gitignore                       ← Loại trừ file CSV, minio_data, ...
│
├── 🐳 infra/                           ← Hạ tầng Docker
│   ├── docker-compose.yml              ← Định nghĩa 4 services (6 containers)
│   ├── minio_data/                     ← Volume lưu trữ MinIO (gitignored)
│   │   └── lakehouse/                  ← Bucket chứa Bronze/Silver/Gold
│   └── workspace/                      ← Shared volume giữa Host ↔ Spark
│       └── BTCUSDT-trades-*.csv        ← File dữ liệu BTC lịch sử
│
└── 🐍 ingestion/                       ← Scripts nạp dữ liệu
    ├── batch_upload.py                 ← Nạp CSV lên MinIO (Batch Pipeline)
    └── stream_to_kafka.py              ← Hứng Binance → Kafka (Stream Pipeline)
```

---

## 🚀 Hướng dẫn cài đặt

### ✅ Yêu cầu hệ thống

| Yêu cầu | Chi tiết |
|----------|---------|
| **Docker Desktop** | Phiên bản mới nhất, đã bật WSL2 (nếu dùng Windows) |
| **RAM** | Tối thiểu **8 GB** (khuyến nghị 16 GB cho Spark) |
| **Disk** | Tối thiểu **25 GB** trống (15GB data + containers) |
| **Python** | >= 3.8 |
| **Internet** | Cần để pull Docker images & kết nối Binance API |

### 📝 Bước 1: Clone Repository

```bash
git clone https://github.com/<your-username>/crypto-lakehouse-project.git
cd crypto-lakehouse-project
```

### 🐳 Bước 2: Khởi động hạ tầng Docker

```bash
cd infra
docker-compose up -d
```

Kiểm tra tất cả container đã chạy:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**Kết quả mong đợi (6/6 containers):**

| Container | Trạng thái | Cổng |
|-----------|-----------|------|
| `lakehouse-kafka` | ✅ Up | `9092`, `29092` |
| `lakehouse-minio` | ✅ Up | `9000` (API), `9001` (Console) |
| `lakehouse-spark-master` | ✅ Up | `8080` (Web UI), `7077` |
| `lakehouse-spark-worker` | ✅ Up | — |

### 🌐 Bước 3: Kiểm tra các Web UI

| Service | URL | Ghi chú |
|---------|-----|---------|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Login: `admin` / `password123` |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Xem trạng thái cluster |

### 🐍 Bước 4: Cài đặt thư viện Python

```bash
pip install minio confluent-kafka websocket-client
```

### 📂 Bước 5: Chuẩn bị dữ liệu Batch

Đặt các file CSV dữ liệu BTC tick-data vào thư mục:

```
infra/workspace/BTCUSDT-trades-*.csv
```

> 💡 **Lưu ý**: Dữ liệu có dung lượng lớn (~15GB) nên đã được thêm vào `.gitignore`. Cần tải riêng.

### ▶️ Bước 6: Chạy Batch Ingestion

```bash
cd ingestion
python batch_upload.py
```

**Output mong đợi:**

```
✔️ Đã tạo bucket: lakehouse
Đang bắt đầu nạp dữ liệu vào lớp Bronze...
Đã nạp thành công: BTCUSDT-trades-2026-01.csv
Đã nạp thành công: BTCUSDT-trades-2026-02.csv
...
NHIỆM VỤ HOÀN THÀNH: Dữ liệu đã nằm an toàn trong MinIO!
```

### ⚡ Bước 7: Chạy Stream Ingestion

```bash
python stream_to_kafka.py
```

**Output mong đợi (cập nhật liên tục):**

```
Đang hứng luồng dữ liệu Bitcoin và đẩy vào Kafka topic: binance_trades...
⚡ Real-time: Price 87234.56 | Qty 0.005 | Time 1711302041000
⚡ Real-time: Price 87235.12 | Qty 0.012 | Time 1711302041500
⚡ Real-time: Price 87233.89 | Qty 0.001 | Time 1711302042000
...
```

---

## 🎬 Demo Script

> 📋 **Kịch bản trình bày trước giáo viên** — Thực hiện theo thứ tự từ trên xuống.

### 🎬 Phần 1: Giới thiệu kiến trúc (2 phút)

1. Mở README.md → Trình bày sơ đồ **5 lớp** và **Medallion Architecture**
2. Nhấn mạnh:
   - ✅ **Medallion Architecture**: Bronze → Silver → Gold
   - ✅ **ACID Compliance** với Delta Lake
   - ✅ **Unified Pipeline**: Batch + Streaming trong cùng một hệ thống

### 🎬 Phần 2: Demo hạ tầng Docker (2 phút)

```bash
# Bước 1: Kiểm tra các container đang chạy
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Bước 2: Mở MinIO Console trên trình duyệt
# → http://localhost:9001 (admin / password123)
# → Chỉ cho cô xem bucket "lakehouse" và cấu trúc bronze/silver/gold

# Bước 3: Mở Spark Master UI
# → http://localhost:8080
# → Chỉ cho cô xem cluster: 1 Master + 1 Worker đang alive
```

### 🎬 Phần 3: Demo Batch Pipeline (3 phút)

```bash
# Chạy script nạp dữ liệu batch
cd ingestion
python batch_upload.py

# → Quay lại MinIO Console
# → Vào lakehouse/bronze/crypto/batch/
# → Chỉ cho cô thấy 13.1GB dữ liệu CSV đã nằm trong Bronze layer
```

**Điểm nhấn khi trình bày:**
> *"Dữ liệu thô 15GB được nạp trực tiếp vào tầng Bronze, giữ nguyên định dạng gốc theo đúng nguyên tắc của Medallion Architecture."*

### 🎬 Phần 4: Demo Streaming Pipeline (3 phút)

```bash
# Chạy script hứng dữ liệu real-time
python stream_to_kafka.py

# → Dữ liệu BTC real-time sẽ hiện liên tục trên terminal
# → Mỗi dòng = 1 giao dịch thực tế trên sàn Binance
# ⚡ Real-time: Price 87234.56 | Qty 0.005 | Time 1711302041000
```

**Điểm nhấn khi trình bày:**
> *"Đây là dữ liệu giao dịch Bitcoin thực tế, đang diễn ra ngay lúc này trên sàn Binance, được stream trực tiếp vào Kafka topic binance_trades."*

### 🎬 Phần 5: Tổng kết & Q&A (2 phút)

Tóm tắt thành tựu:

| ✅ Thành tựu | Chi tiết |
|-------------|---------|
| Hạ tầng Docker | **6/6** containers hoạt động ổn định |
| Batch Ingestion | **13.1 GB** CSV → MinIO Bronze layer |
| Streaming Ingestion | Real-time BTC trades → Kafka topic `binance_trades` |
| Medallion Architecture | Cấu trúc Bronze/Silver/Gold trên MinIO |
| ACID Compliance | Delta Lake sẵn sàng cho tầng Silver & Gold |
| Kafka KRaft | Chế độ hiện đại, **không cần Zookeeper** |

---

## 📊 Kết quả đạt được

### ✅ Infrastructure Status

```
┌──────────────────────────┬──────────┬─────────────────────┐
│ Component                │ Status   │ Details             │
├──────────────────────────┼──────────┼─────────────────────┤
│ Kafka (KRaft Mode)       │ ✅ UP    │ Port 9092, 29092    │
│ MinIO Object Storage     │ ✅ UP    │ Port 9000, 9001     │
│ Spark Master             │ ✅ UP    │ Port 8080, 7077     │
│ Spark Worker             │ ✅ UP    │ Connected to Master │
├──────────────────────────┼──────────┼─────────────────────┤
│ Total Containers         │ 6/6      │ All Healthy         │
└──────────────────────────┴──────────┴─────────────────────┘
```

### ✅ Data Pipeline Status

| Pipeline | Trạng thái | Dữ liệu |
|----------|-----------|----------|
| Batch Ingestion | ✅ Thành công | 13.1 GB CSV → MinIO `bronze/` |
| Stream Ingestion | ✅ Thành công | Binance real-time → Kafka `binance_trades` |

---

## 📝 Giấy phép

Dự án được phát hành dưới giấy phép [MIT License](LICENSE).

---

<div align="center">

**🎓 HCMUTE — Khoa Công nghệ Thông tin**

*Đồ án Phân tích Dữ liệu lớn — Học kỳ 2, 2025-2026*

Made with ❤️ and ☕

</div>
