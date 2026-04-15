<div align="center">

# 🏗️ Real-time Crypto Data Lakehouse Architecture

### Unified Batch & Streaming Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Prefect](https://img.shields.io/badge/Prefect-Orchestration-1565C0?logo=prefect&logoColor=white)](https://www.prefect.io/)

> 🎓 **Đồ án môn Phân tích Dữ liệu lớn (Big Data Analytics)**
>
> Trường Đại học Sư phạm Kỹ thuật TP.HCM (HCMUTE)

---

*Xây dựng hệ thống Data Lakehouse 5 lớp xử lý **15GB** dữ liệu Bitcoin lịch sử và luồng **Real-time** từ Binance API, sử dụng Spark, Kafka và Delta Lake với kiến trúc **Medallion Architecture** hợp nhất.*

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
| 1 | **Unified Pipeline** | Xây dựng pipeline thống nhất xử lý cả **Batch** (15GB dữ liệu lịch sử) và **Streaming** (luồng real-time từ Binance). Không cần lưu qua lớp Raw trung gian. |
| 2 | **Medallion Architecture** | Triển khai kiến trúc dữ liệu 3 tầng: **Bronze → Silver → Gold** để cải thiện dần chất lượng dữ liệu |
| 3 | **ACID Compliance** | Đảm bảo tính toàn vẹn dữ liệu với **Delta Lake** (Atomicity, Consistency, Isolation, Durability) |
| 4 | **Modern Infrastructure** | Sử dụng hạ tầng hiện đại điều phối bằng **Prefect 2.x**, hoàn toàn **Dockerized**, dễ dàng một chạm triển khai |

### 📦 Nguồn dữ liệu

| Loại | Nguồn | Mô tả | Khối lượng |
|------|--------|--------|------------|
| 📂 **Batch** | BTC Tick-Data CSV | Dữ liệu giao dịch lịch sử BTC/USDT theo từng tick, đọc trực tiếp từ Local Mount | **~15 GB** (13.1GB đã nạp) |
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
│  │ LỚP 4: COMPUTE (Xử lý - Apache Spark / Điều phối: Prefect) │   │
│  │   • Batch Processing (SparkSQL, tự động hóa Job)             │   │
│  │   • Stream Processing (Structured Streaming)                 │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 3: METADATA (Quản lý siêu dữ liệu - Delta Lake)       │   │
│  │   📋 Delta Transaction Log  │  🕐 Time Travel               │   │
│  │   🔒 ACID Transactions      │  📐 Schema Evolution        │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 2: STORAGE (Lưu trữ - MinIO S3)                        │   │
│  │   🥉 Bronze (Raw)  →  🥈 Silver (Clean)  →  🥇 Gold (Agg)  │   │
│  │   Object Storage hợp nhất Batch/Stream vào một bảng          │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│  ┌──────────────────────────▼──────────────────────────────────┐   │
│  │ LỚP 1: INGESTION (Nạp dữ liệu)                             │   │
│  │   📂 Batch: CSV Volumes   ⚡ Stream: Binance → Kafka      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Chi tiết từng lớp

#### 🔽 Lớp 1: Ingestion (Nạp dữ liệu)
Thu thập gốc dữ liệu:
- **Batch:** Data tĩnh nặng 15GB đặt sẵn ở ổ cứng (`infra/workspace/*.csv`).
- **Streaming:** Script Python cào WebSocket thả qua Kafka Topic `binance_trades`.

#### 💾 Lớp 2: Storage (Lưu trữ Medallion)
Lưu vào Object Storage **MinIO**. Sắp xếp cấu trúc `lakehouse` chuẩn:
- `all_crypto_trades` (Bronze): Khúc gỗ thô, trộn chung 2 luồng Batch & Kafka JSON.
- `btc_trades` (Silver): Gỗ bào, ghép cột/Schema, bóc tách JSON và lọc rác chuẩn chỉ.
- Tầng Gold: Data tổng hợp.

#### 📋 Lớp 3: Metadata (Delta Lake)
- Kể chuyện bằng **Schema Evolution**: khi Stream Kafka ném thêm JSON, Delta tự tạo thêm cột `value` tại bảng đã có CSV mà không làm vỡ cấu trúc.
- Quản lý phiên bản Checkpoint Stream và phiên bản bảng tin.

#### ⚙️ Lớp 4: Compute & Orchestration (Spark & Prefect)
- Cụm Spark Structured Streaming đọc 24/7 trực tiếp trên bảng Bronze để tinh luyện ra Silver.
- Master node **Prefect** dàn xếp các Flow chạy tự động và trigger dọn dẹp data hệ thống (Monitoring, Restart Containers).

---

## 🥇 Medallion Architecture (Hợp Nhất)

Medallion Architecture (Kiến trúc Huy chương) chia dữ liệu thành **3 tầng chất lượng** tăng dần:

```
    CSV / KAFKA
         │
         ▼
   ┌──────────┐         ┌──────────┐         ┌──────────┐
   │  🥉      │  Spark  │  🥈      │  Spark  │  🥇      │
   │  BRONZE  │ ──────► │  SILVER  │ ──────► │   GOLD   │
   │ (Unified)│  Stream │ (Clean)  │  Batch  │  (Agg)   │
   └──────────┘         └──────────┘         └──────────┘
    Hợp nhất 1 bảng      Đã làm sạch         Sẵn sàng
    Batch & Stream        chuẩn hóa          phân tích
```

| Tầng | Cũ (Legacy) | Hiện Tại (Unified) |
|------|-----------|-------------------|
| 🥉 **Bronze** | Đẩy qua Raw layer MinIO riêng biệt, hoặc đứt đoạn từng thư mục. | **Trực tiếp từ Nguồn**. Spark đọc thẳng Local CSV và Stream Kafka nối đuôi vào chung 1 bảng duy nhất (`all_crypto_trades`) nhờSchema Evolution. |
| 🥈 **Silver** | Code rườm rà. | Spark Streaming 24/7 đứng gác ở Bronze Table. Parsing JSON và cột CSV bù trừ nhau, đổi Epoch Timestamps, xuất ra bảng sạch `btc_trades`. |
| 🥇 **Gold** | — | Dữ liệu tổng hợp theo thời gian (OHLCV), sẵn sàng gọi lên các UI báo cáo. |

### Tại sao cần Medallion Architecture Này?

1. **Hiệu suất (Performance):** Lược bỏ được bước đẩy file lên Raw Data Lake giúp bớt tắc nghẽn IO.
2. **Unified Data:** Gói gọn cả 2 hình thái Batch & Stream vô 1 bảng duy nhất tại Bronze. Kiến trúc trở nên rất sạch.
3. **Data Quality:** Mỗi tầng tăng thêm một lớp kiểm tra chất lượng chặt chẽ.

---

## 🧬 ACID & Time Travel với Delta Lake

### Tính chất ACID

| Tính chất | Áp dụng trong dự án |
|-----------|---------------------|
| **A**tomicity | File ghi xuống MinIO thành công hoàn toàn hoặc không tạo rác. |
| **C**onsistency | Schema được enforce chặt, Evolution được allow explicitly. |
| **I**solation | *Batch và Streaming đang ghi APPEND song song trên cùng 1 bảng* mà không bị crash. |
| **D**urability | Transaction log được lưu trữ bền vững trong thư mục `_delta_log/`. |

---

## 🛠️ Công nghệ sử dụng

| Lớp | Công nghệ | File/Code | Vai trò |
|-----|-----------|-----------|---------|
| 🔽 Ingestion | **Apache Kafka** (KRaft) | `docker-compose.yml` | Broker Streaming, KHÔNG dùng Zookeeper |
| 🔽 Ingestion | **Binance WebSocket** | `stream_to_kafka.py` | Cung cấp luồng trades real-time |
| 💾 Storage | **MinIO** | `lakehouse/` | Bể chứa S3 lưu Data |
| 📋 Metadata | **Delta Lake** | Thư viện Spark | Cân schema, nối file, Checkpointing |
| ⚙️ Compute | **Apache Spark** | `processing/*.py` | Lọc, parse, và push dữ liệu các tầng |
| 🤖 Orchestration | **Prefect 2.20+** | `prefect.yaml`, `orchestration/` | Lên lịch, tạo Worker nhúng luồng Batch |
| 🐳 Infra | **Docker Compose** | `infra/` | Setup lên toàn bộ hệ thống bằng 1 lệnh |

---

## 📁 Cấu trúc dự án

```
crypto-lakehouse-project/
│
├── 🐳 infra/                           ← Hạ tầng Docker (Docker Compose & Dockerfiles)
│   ├── workspace/                      ← Mount CSV Local (Chứa khối 15GB BTC Trades)
│   └── docker-compose.yml              ← Dựng toàn bộ Kafka, MinIO, Spark, Prefect cực êm
│
├── 🐍 ingestion/                       ← Nguồn Streaming
│   └── stream_to_kafka.py              ← Binance → Kafka
│
├── 🧠 orchestration/                   ← Prefect Flows
│   ├── batch_flow.py                   ← Định nghĩa tiến trình chạy ngầm
│   └── monitor_flow.py                 ← Theo dõi sức khỏe hệ thống
│
└── ⚙️ processing/                      ← Lõi Spark Transformation
    ├── spark_batch_to_bronze.py        ← Volume CSV → Delta Bronze
    ├── pyspark_stream_to_bronze.py     ← Kafka → Delta Bronze (Append chung)
    └── pyspark_bronze_to_silver.py     ← Bronze → Silver (Streaming 24/7 Cleaning)
```

---

## 🚀 Hướng dẫn cài đặt

### ✅ Yêu cầu hệ thống

- **Docker Desktop** (Đã bật WSL2 trên Windows)
- **RAM**: Tối thiểu **8 GB** (Khuyến nghị 16 GB)
- **Disk**: 25 GB trống để chạy hạ tầng và dữ liệu.
- Tài nguyên mạng ổn định (Để tải Docker Image và hứng Data).

### 📝 Bước 1: Clone Repository & Data

```bash
git clone https://github.com/HuynhThach/crypto-lakehouse-project.git
cd crypto-lakehouse-project
```

- Nhét các file BTC CSV lịch sử của bạn vào thư mục `infra/workspace/` (VD: `BTCUSDT-trades-*.csv`).

### 🐳 Bước 2: Kích hoạt hệ thống vĩnh cửu (1 Lệnh duy nhất)

Kiến trúc đã được Dockerized tiêu chuẩn. Chạy lệnh:

```bash
cd infra
docker-compose up -d --build
```

**Điều gì thực sự xảy ra đằng sau lệnh này?**
1. Docker gọi lên Kafka, MinIO.
2. Các Container **Stream (Producer, Bronze, Silver)** tự động run background kết nối Binance API -> Kafka -> Bronze -> Silver 24/7.
3. Server **Prefect** đứng lên, kích hoạt Worker. Worker tự động đọc `prefect.yaml` ghim sẵn các task schedule mà **không cần deploy bằng tay**.

### 🌐 Bước 3: Xem các Bảng điều khiển

| Service | URL | Ghi chú |
|---------|-----|---------|
| **Prefect UI** | [http://localhost:4200](http://localhost:4200) | Xem các Flow màu xanh mướt |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` |

### ▶️ Bước 4: Chạy nạp luồng quá khứ (Batch Ingestion)

Streaming tự chạy rồi, giờ mình ném 15GB dữ liệu tĩnh vào. Mở Terminal và ra lệnh cho Prefect trigger Flow Batch:

```bash
docker-compose exec prefect-worker sh /app/scripts/prefect_run_batch.sh
```

Dữ liệu sẽ được đẩy thẳng từ khối local vào chung "nồi lẩu" `all_crypto_trades` (Bronze), sau đó tầng Silver Stream sẽ tự tóm cổ khối data này làm sạch dần.

---

## 🎬 Demo Script Mới

> Dành cho buổi bảo vệ đồ án trước hội đồng

**Phần 1: Show tính tự động hóa (1 phút)**
- Bật `docker ps` show 6 container vững chãi.
- Mở `localhost:4200` chứng minh Prefect Worker vừa quét xong và dàn trận Flow chuẩn xác.

**Phần 2: Show độ dẻo dai Medallion / Schema Evolution (2 phút)**
- Kích hoạt lệnh chạy Batch (`prefect_run_batch.sh`).
- Show Code `spark_batch_to_bronze.py`: Nhấn mạnh là chạy thẳng xuống `lakehouse/bronze/all_crypto_trades` mà không mượn vùng đệm.
- Show Code `pyspark_stream_to_bronze.py`: Nhấn mạnh là bảng đang được Append trực tiếp với chế độ Kafka, sinh ra cột `value` kẹp chung schema CSV.

**Phần 3: Show "Nồi lọc" Silver (2 phút)**
- Mở `pyspark_bronze_to_silver.py`. Trình bày đoạn logic tự động phát hiện cột `value` (Của Stream) và cột `Price` (Của Batch) để Parsing bù trừ nhau. Đưa về 1 chuẩn Schema duy nhất dưới dạng Micro-batch liên tục.

---

## 📝 Giấy phép

Dự án được phát hành dưới giấy phép [MIT License](LICENSE).

<div align="center">

**🎓 HCMUTE — Khoa Công nghệ Thông tin**

*Đồ án Phân tích Dữ liệu lớn — Học kỳ 2, 2025-2026*

Made with ❤️ and ☕

</div>
