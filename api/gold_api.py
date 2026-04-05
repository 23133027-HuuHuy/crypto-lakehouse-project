"""
Gold Layer REST API for Power BI / Dashboard Integration
Cung cấp endpoints để truy vấn dữ liệu realtime từ Gold Layer (Delta Lake)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import os
import json
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Cho phép Power BI gọi API

# Spark Session singleton
_spark = None

def get_spark():
    """Tạo hoặc lấy Spark Session đã có"""
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
            .appName("GoldLayerAPI") \
            .config("spark.jars.packages", 
                    "io.delta:delta-spark_2.13:4.1.0,"
                    "org.apache.hadoop:hadoop-aws:3.4.2,"
                    "software.amazon.awssdk:bundle:2.29.52") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "password123")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        _spark.sparkContext.setLogLevel("WARN")
    return _spark

# ==========================================
# GOLD LAYER PATHS
# ==========================================
GOLD_PATHS = {
    "ohlc": "s3a://lakehouse/gold/OHLC_1Min",
    "whale": "s3a://lakehouse/gold/Whale_Alert",
    "flow": "s3a://lakehouse/gold/maker_taker_flow_1min",
    "vwap": "s3a://lakehouse/gold/VWAP_1Min"
}


def df_to_json(df, limit=100):
    """Convert Spark DataFrame to JSON-serializable format"""
    pandas_df = df.limit(limit).toPandas()
    # Convert timestamps to string format (handle out-of-range years)
    for col_name in pandas_df.columns:
        if pandas_df[col_name].dtype == 'datetime64[ns]':
            # Handle timestamps with out-of-range years by converting to string
            try:
                pandas_df[col_name] = pandas_df[col_name].dt.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                # Fallback: convert to string directly
                pandas_df[col_name] = pandas_df[col_name].astype(str)
        elif str(pandas_df[col_name].dtype).startswith('datetime'):
            pandas_df[col_name] = pandas_df[col_name].astype(str)
    
    # Handle any remaining non-serializable types
    for col_name in pandas_df.columns:
        if pandas_df[col_name].dtype == 'object':
            pandas_df[col_name] = pandas_df[col_name].apply(
                lambda x: str(x) if x is not None and not isinstance(x, (str, int, float, bool)) else x
            )
    
    return pandas_df.to_dict(orient='records')


# ==========================================
# API ENDPOINTS
# ==========================================

@app.route('/')
def index():
    """API Health Check"""
    return jsonify({
        "status": "running",
        "service": "Crypto Lakehouse Gold API",
        "timestamp": datetime.now().isoformat(),
        "endpoints": [
            "/api/ohlc/latest",
            "/api/whale/latest", 
            "/api/flow/latest",
            "/api/vwap/latest",
            "/api/dashboard/summary"
        ]
    })


@app.route('/api/ohlc/latest')
def get_ohlc_latest():
    """
    Lấy dữ liệu OHLC (nến giá) mới nhất
    Query params:
        - limit: số lượng records (default: 100)
        - symbol: filter theo symbol (default: all)
    """
    try:
        spark = get_spark()
        limit = request.args.get('limit', 100, type=int)
        symbol = request.args.get('symbol', None)
        
        df = spark.read.format("delta").load(GOLD_PATHS["ohlc"])
        
        if symbol:
            df = df.filter(col("symbol") == symbol)
        
        df = df.orderBy(desc("candle_time"))
        data = df_to_json(df, limit)
        
        return jsonify({
            "success": True,
            "count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/whale/latest')
def get_whale_latest():
    """
    Lấy cảnh báo Whale (giao dịch lớn) mới nhất
    Query params:
        - limit: số lượng records (default: 50)
        - min_value: giá trị tối thiểu USDT (default: 50000)
    """
    try:
        spark = get_spark()
        limit = request.args.get('limit', 50, type=int)
        min_value = request.args.get('min_value', 50000, type=float)
        
        df = spark.read.format("delta").load(GOLD_PATHS["whale"])
        df = df.filter(col("trade_value_usdt") >= min_value)
        df = df.orderBy(desc("event_time"))
        data = df_to_json(df, limit)
        
        return jsonify({
            "success": True,
            "count": len(data),
            "min_value_filter": min_value,
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/flow/latest')
def get_flow_latest():
    """
    Lấy dữ liệu Maker/Taker Flow mới nhất
    Query params:
        - limit: số lượng records (default: 100)
        - symbol: filter theo symbol (default: all)
    """
    try:
        spark = get_spark()
        limit = request.args.get('limit', 100, type=int)
        symbol = request.args.get('symbol', None)
        
        df = spark.read.format("delta").load(GOLD_PATHS["flow"])
        
        if symbol:
            df = df.filter(col("symbol") == symbol)
        
        df = df.orderBy(desc("window_start"))
        data = df_to_json(df, limit)
        
        return jsonify({
            "success": True,
            "count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/vwap/latest')
def get_vwap_latest():
    """
    Lấy dữ liệu VWAP mới nhất
    Query params:
        - limit: số lượng records (default: 100)
        - symbol: filter theo symbol (default: all)
    """
    try:
        spark = get_spark()
        limit = request.args.get('limit', 100, type=int)
        symbol = request.args.get('symbol', None)
        
        df = spark.read.format("delta").load(GOLD_PATHS["vwap"])
        
        if symbol:
            df = df.filter(col("symbol") == symbol)
        
        df = df.orderBy(desc("window_start"))
        data = df_to_json(df, limit)
        
        return jsonify({
            "success": True,
            "count": len(data),
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/dashboard/summary')
def get_dashboard_summary():
    """
    Lấy tổng hợp dữ liệu cho Dashboard
    Trả về: giá mới nhất, whale gần đây, net flow
    """
    try:
        spark = get_spark()
        
        # Latest OHLC
        ohlc_df = spark.read.format("delta").load(GOLD_PATHS["ohlc"])
        latest_ohlc = ohlc_df.orderBy(desc("candle_time")).limit(1).toPandas()
        
        # Recent Whales (last 10)
        whale_df = spark.read.format("delta").load(GOLD_PATHS["whale"])
        recent_whales = whale_df.orderBy(desc("event_time")).limit(10).toPandas()
        
        # Latest Flow
        flow_df = spark.read.format("delta").load(GOLD_PATHS["flow"])
        latest_flow = flow_df.orderBy(desc("window_start")).limit(1).toPandas()
        
        # Format response
        summary = {
            "latest_price": {
                "symbol": latest_ohlc['symbol'].iloc[0] if len(latest_ohlc) > 0 else None,
                "price": float(latest_ohlc['close_price'].iloc[0]) if len(latest_ohlc) > 0 else None,
                "time": str(latest_ohlc['candle_time'].iloc[0]) if len(latest_ohlc) > 0 else None
            },
            "whale_count_recent": len(recent_whales),
            "whale_total_value": float(recent_whales['trade_value_usdt'].sum()) if len(recent_whales) > 0 else 0,
            "net_flow": float(latest_flow['net_flow'].iloc[0]) if len(latest_flow) > 0 else 0,
            "market_sentiment": "BULLISH" if (len(latest_flow) > 0 and latest_flow['net_flow'].iloc[0] > 0) else "BEARISH"
        }
        
        return jsonify({
            "success": True,
            "summary": summary,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/stats')
def get_stats():
    """Thống kê tổng quan về Gold Layer"""
    try:
        spark = get_spark()
        
        stats = {}
        for name, path in GOLD_PATHS.items():
            df = spark.read.format("delta").load(path)
            stats[name] = {
                "total_records": df.count(),
                "path": path
            }
        
        return jsonify({
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    
    print(f"""
    ╔══════════════════════════════════════════════════════╗
    ║     🚀 Crypto Lakehouse Gold Layer API               ║
    ║     Running on http://0.0.0.0:{port}                  ║
    ╠══════════════════════════════════════════════════════╣
    ║  Endpoints:                                          ║
    ║    GET /api/ohlc/latest    - OHLC candles            ║
    ║    GET /api/whale/latest   - Whale alerts            ║
    ║    GET /api/flow/latest    - Maker/Taker flow        ║
    ║    GET /api/vwap/latest    - VWAP data               ║
    ║    GET /api/dashboard/summary - Dashboard summary    ║
    ║    GET /api/stats          - Gold layer statistics   ║
    ╚══════════════════════════════════════════════════════╝
    """)
    
    app.run(host='0.0.0.0', port=port, debug=debug)
