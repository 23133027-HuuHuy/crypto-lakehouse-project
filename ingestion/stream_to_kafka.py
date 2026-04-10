import json
import os

import websocket
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "binance_trades")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@aggTrade")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def on_message(ws, message):
    data = json.loads(message)
    normalized_data = {
        "s": data.get("s"),
        "p": data.get("p"),
        "q": data.get("q"),
        "T": data.get("T"),
        "m": data.get("m")
    }
    
    producer.produce(TOPIC_NAME, value=json.dumps(normalized_data))
    producer.flush()

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

ws = websocket.WebSocketApp(
    BINANCE_WS_URL,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print(
    f"Đang stream Binance -> Kafka. topic={TOPIC_NAME}, "
    f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}"
)
ws.run_forever()
