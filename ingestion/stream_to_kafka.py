import websocket
import json
from confluent_kafka import Producer

# --- CẤU HÌNH KAFKA ---
# Sử dụng port 29092 vì script này chạy bên ngoài Docker (External)
conf = {'bootstrap.servers': '127.0.0.1:29092'}
producer = Producer(conf)

TOPIC_NAME = 'binance_trades'

def on_message(ws, message):
    # 1. Nhận dữ liệu JSON từ Binance
    data = json.loads(message)
    normalized_data = {
        "s": data.get("s"),
        "p": data.get("p"),
        "q": data.get("q"),
        "T": data.get("T"),
        "m": data.get("m")
    }
    
    # 2. Đẩy dữ liệu vào Kafka topic
    producer.produce(TOPIC_NAME, value=json.dumps(normalized_data))
    producer.flush() # Đẩy dữ liệu đi ngay lập tức
    
    # 3. In ra màn hình để Demo "Velocity" cho cô giáo xem
    print(
        f"Real-time: Price {normalized_data['p']} | "
        f"Qty {normalized_data['q']} | "
        f"BuyerMaker {normalized_data['m']} | "
        f"Time {normalized_data['T']}"
    )

def on_error(ws, error):
    print(f"Lỗi kết nối: {error}")

def on_close(ws, close_status_code, close_msg):
    print(" Đã ngắt kết nối với Binance")

# --- KẾT NỐI BINANCE WEBSOCKET ---
socket_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

ws = websocket.WebSocketApp(
    socket_url,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print(f"Đang hứng luồng dữ liệu Bitcoin và đẩy vào Kafka topic: {TOPIC_NAME}...")
ws.run_forever()
