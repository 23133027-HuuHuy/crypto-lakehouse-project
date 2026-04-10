# HuongDanChayDoAn - Production với Prefect 2.x

> Version đã kiểm chứng tương thích:
> - `prefect==2.20.25`
> - `prefect-docker==0.5.5`

## 1) Kiến trúc production-lite (tiết kiệm tài nguyên)

- **Streaming 24/7 bằng Docker services**:
  - `stream-producer` (Binance -> Kafka)
  - `stream-bronze` (Kafka -> Bronze)
  - `stream-silver` (Bronze -> Silver)
  - `stream-gold` (Silver -> Gold)
- **Prefect orchestration cho batch + monitoring**:
  - `prefect-server` (UI/API)
  - `prefect-worker` (Docker worker)
- **Flow chạy theo Docker infrastructure**:
  - Mỗi flow run tạo **container mới**
  - Run xong thì container **auto remove**

## 2) File đã thêm/chỉnh

- `infra/docker-compose.yml` (production-lite)
- `infra/Dockerfile.stream`
- `infra/Dockerfile.prefect`
- `orchestration/batch_flow.py`
- `orchestration/monitor_flow.py`
- `prefect.yaml`
- `ingestion/stream_to_kafka.py` (env-driven)
- `infra/Dockerfile.spark` (thêm prefect libs)

## 3) Chạy hệ thống

```bash
cd infra
docker-compose up -d --build
```

```bash
# deploy prefect (ngắn gọn)
docker-compose exec prefect-worker sh /app/scripts/prefect_deploy.sh
```

Truy cập:

- Prefect UI: `http://localhost:4200`
- MinIO: `http://localhost:9001`
- Gold API: `http://localhost:5000` (chỉ khi chạy profile `full`)
- Metabase/Trino: chạy profile `full`

Chế độ đầy đủ (optional):

```bash
docker-compose --profile full up -d
```

## 4) CLI Prefect cần dùng

> Có thể chạy trong container `prefect-worker` để đồng nhất môi trường.

```bash
# vào worker container
docker-compose exec prefect-worker sh
```

```bash
# (A) server start (nếu chạy local ngoài compose)
prefect server start

# (B) tạo work pool docker
prefect work-pool create docker-pool --type docker

# (C) start worker
prefect worker start --pool docker-pool --type docker

# (D) build deployment
prefect deployment build orchestration/batch_flow.py:run_batch_pipeline -n batch-prod -p docker-pool
prefect deployment build orchestration/monitor_flow.py:monitor_streaming_services -n monitor-prod -p docker-pool

# (E) apply deployment
prefect deployment apply run_batch_pipeline-deployment.yaml
prefect deployment apply monitor_streaming_services-deployment.yaml

# (F) schedule (cron)
prefect deployment schedule "lakehouse-batch-flow/batch-prod" --cron "0 */2 * * *"
prefect deployment schedule "lakehouse-stream-monitor-flow/monitor-prod" --cron "*/2 * * * *"
```

Ngoài ra có thể dùng `prefect.yaml`:

```bash
prefect deploy --all
```

Lưu ý:
- `prefect.yaml` đã dùng `prefect-version: 2.20.25`.
- Nếu đổi version Prefect, nên test lại `prefect worker start --type docker`.

### Lệnh ngắn để trigger flow

```bash
docker-compose exec prefect-worker sh /app/scripts/prefect_run_batch.sh
docker-compose exec prefect-worker sh /app/scripts/prefect_run_monitor.sh
```

## 5) Flow hoạt động như thế nào

### `batch_flow.py`

Chạy tuần tự:
1. `python processing/spark_batch_to_bronze.py` (đọc trực tiếp CSV local từ `/app/workspace`)
2. `python processing/rebuild_silver_gold.py`

Có retry + logging ở từng task.

### `monitor_flow.py`

- Kiểm tra container streaming:
  - `lakehouse-stream-producer`
  - `lakehouse-stream-bronze`
  - `lakehouse-stream-silver`
  - `lakehouse-stream-gold`
- Nếu status khác `running` thì restart.
- Ghi log trạng thái cho từng container.

## 6) Luồng end-to-end khi `docker-compose up -d`

1. Core services lên trước: Kafka, MinIO, Spark env, Trino, Metabase, Gold API.
2. Streaming services chạy liên tục và tự restart (`restart: unless-stopped`).
3. Prefect Server mở API/UI.
4. Prefect Worker kết nối Server và poll job từ `docker-pool`.
5. Khi deployment được schedule/trigger:
   - Worker tạo 1 flow-run container mới (image `lakehouse-spark-env:latest`)
   - Mount source code vào `/app`
   - Chạy flow entrypoint
   - Kết thúc thì container thoát và bị remove.

## 7) Best practices production (ngắn gọn, thực tế)

- **Logging**
  - Chuẩn hóa format JSON log cho producer/flows.
  - Gắn `flow_run_id`, `container_name` vào log context.

- **Retry**
  - Retry ngắn cho lỗi mạng (Binance/Kafka/MinIO).
  - Retry dài hơn cho Spark batch tasks.

- **Container naming**
  - Dùng prefix rõ ràng: `lakehouse-stream-*`, `lakehouse-prefect-*`.
  - Tránh rename thủ công ngoài compose để monitor flow tracking đúng.
