Bạn là một Senior Data Engineer có kinh nghiệm về Data Platform, Lakehouse architecture và workflow orchestration.

Tôi đã xây dựng một hệ thống Data Lakehouse realtime với stack như sau:

* Kafka (KRaft mode) để ingest dữ liệu streaming
* PySpark Structured Streaming để xử lý realtime
* MinIO (S3-compatible) để lưu trữ (Raw, Bronze, Silver, Gold)
* Trino để query
* Metabase để làm dashboard
* Gold API (đọc Trino lớp Gold) để BI tools (Power BI/Metabase) kết nối qua API URL
* Docker Compose để quản lý hạ tầng

---

HIỆN TRẠNG (CHẠY THỦ CÔNG):

Hệ thống hiện đang chạy bằng nhiều lệnh thủ công:

Batch:

* python ingestion/batch_upload.py
* docker compose exec spark-env python /app/processing/spark_batch_to_bronze.py

Streaming (chạy nhiều terminal):

* python ingestion/stream_to_kafka.py
* docker compose exec spark-env python /app/processing/pyspark_stream_to_bronze.py
* docker compose exec spark-env python /app/processing/pyspark_bronze_to_silver.py
* docker compose exec spark-env python /app/processing/pyspark_silver_to_gold.py

---

MỤC TIÊU:

Tôi muốn đưa hệ thống này lên production bằng Prefect với các yêu cầu:

1. Sử dụng Prefect Server + Prefect Worker chạy trong Docker
2. Prefect Worker phải mount Docker socket:

   * /var/run/docker.sock:/var/run/docker.sock
3. Prefect Worker phải mount source code project:

   * để container chạy flow có thể đọc code từ local (/app)
4. Sử dụng Docker infrastructure:

   * Mỗi lần chạy flow → tạo container mới
   * Chạy flow trong container đó
   * Sau khi chạy xong → container bị exit

---

YÊU CẦU STREAMING:

* Streaming phải chạy dưới dạng Docker services (chạy liên tục 24/7)
* KHÔNG để Prefect chạy trực tiếp streaming

Các service gồm:

* stream-producer (Python Kafka producer)
* stream-bronze (Spark streaming)
* stream-silver (Spark streaming)
* stream-gold (Spark streaming)

Các service này phải nằm trong docker-compose và chạy bằng:
docker compose up -d

---

VAI TRÒ CỦA PREFECT:

* Orchestration cho batch pipeline
* Monitoring streaming services (health check)
* Tự động restart container nếu bị lỗi
* Schedule flow (cron)
* Logging và quan sát hệ thống

---

NHIỆM VỤ CỦA BẠN:

1. Thiết kế kiến trúc production đầy đủ:

   * Prefect Server
   * Prefect Worker
   * Docker infrastructure
   * Tách biệt streaming services

2. Viết file docker-compose.yml hoàn chỉnh:

   * Bao gồm:

     * kafka
     * minio
     * spark-env
      * trino
      * gold-api
      * metabase
     * stream-producer
     * stream-bronze
     * stream-silver
     * stream-gold
     * prefect-server
     * prefect-worker
   * Prefect Worker phải mount docker socket và source code

3. Viết Dockerfile:

   * Dockerfile.stream (cho Kafka producer)
   * Sử dụng Dockerfile.spark cho Spark jobs

4. Viết Prefect flows:

   (A) monitor_flow.py

   * kiểm tra container streaming có đang chạy không
   * nếu chết thì restart
   * ghi log trạng thái

   (B) batch_flow.py

   * chạy:

     * batch_upload
     * spark_batch_to_bronze
   * có retry và logging

5. Cấu hình Prefect deployment:

   * tạo Docker work pool
   * dùng Docker infrastructure
   * mỗi lần chạy flow → tạo container mới
   * mount volume /app

6. Cung cấp các lệnh CLI:

   * prefect server start
   * prefect work-pool create docker-pool --type docker
   * prefect worker start --pool docker-pool
   * prefect deployment build
   * prefect deployment apply
   * prefect deployment schedule (cron)

7. Giải thích luồng hoạt động đầy đủ:

   * Khi chạy docker compose up -d thì chuyện gì xảy ra
   * Streaming chạy như thế nào
   * Prefect trigger flow ra sao
   * Worker tạo container như thế nào
   * Monitoring hoạt động ra sao

8. Đề xuất best practices production:

   * logging
   * retry
   * naming container

---

YÊU CẦU OUTPUT:

* Code phải đầy đủ và chạy được
* Cấu trúc rõ ràng
* Dễ đọc, có comment
* Dùng Prefect 2.x
* Không over-engineer
* Bám sát thực tế production

---

MỤC TIÊU CUỐI:

Biến hệ thống lakehouse đang chạy thủ công thành một data platform tự động, chuẩn production sử dụng Prefect với Docker-based execution và streaming services tách biệt.
