from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType
from cassandra.cluster import Cluster
from pymongo import MongoClient
import json
import urllib.request
import math
import statistics
import pandas as pd


har_weight_15m = 0.17862620024636272
har_weight_1h = 0.011214103226812332
har_weight_24h = 0.00035420663272866257
har_intercept = 1.896524003230036e-05
anomaly_threshold = 4.0
history_size = 288

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT",
           "AVAXUSDT", "DOTUSDT", "LINKUSDT", "POLUSDT", "LTCUSDT", "TRXUSDT", "ATOMUSDT",
           "UNIUSDT", "NEARUSDT", "APTUSDT", "FILUSDT", "ETCUSDT", "XLMUSDT"]


cassandra = Cluster(["cassandra"]).connect("binance")
save_forecast = cassandra.prepare("""
    INSERT INTO forecasts (symbol, day, event_time, close, rv_15m, forecast, vol_forecast, vol_realized, residual, zscore)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
anomalies = MongoClient("mongodb://mongodb:27017")["binance"]["anomalies"]


spark = SparkSession.builder.appName("binance-stream").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "true")
spark.sparkContext.setLogLevel("WARN")


kline_format = StructType([
    StructField("s", StringType()),
    StructField("k", StructType([
        StructField("T", LongType()),
        StructField("c", StringType()),
        StructField("h", StringType()),
        StructField("l", StringType()),
        StructField("v", StringType()),
        StructField("V", StringType()),
    ])),
])


live_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw_klines")
    .option("startingOffsets", "latest")
    .load())

candles = (live_stream
    .select(F.from_json(F.col("value").cast("string"), kline_format).alias("data"))
    .select(
        F.col("data.s").alias("symbol"),
        (F.col("data.k.T") / 1000).cast("timestamp").alias("event_time"),
        F.col("data.k.c").cast("double").alias("close"),
        F.col("data.k.h").cast("double").alias("high"),
        F.col("data.k.l").cast("double").alias("low"),
        F.col("data.k.v").cast("double").alias("volume"),
    ))

minute_bars = (candles
    .withWatermark("event_time", "30 seconds")
    .groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
    .agg(
        F.last("close").alias("close"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.sum("volume").alias("volume"),
    )
    .select(
        F.col("symbol"),
        F.col("window.end").alias("bar_time"),
        "close", "high", "low", "volume",
    ))


def download_last_24h(symbol):
    # Best-effort warm-up from REST. If a single symbol fails (network blip,
    # rate limit, REST hiccup), return an empty buffer and let it warm up from
    # the live stream instead of crashing the whole processor at startup.
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=1000"
        raw = json.loads(urllib.request.urlopen(url, timeout=10).read())
        table = pd.DataFrame(raw, columns=range(12))[[6, 4]]
        table.columns = ["bar_time", "close"]
        table["close"] = table["close"].astype(float)
        table["bar_time"] = pd.to_datetime(table["bar_time"], unit="ms", utc=True)
        return table[["bar_time", "close"]]
    except Exception as e:
        print(f"warm-up failed for {symbol}: {e} (will warm up from live stream)")
        return pd.DataFrame({"bar_time": pd.Series([], dtype="datetime64[ns, UTC]"),
                             "close": pd.Series([], dtype="float64")})


print("warming up buffers...")
price_history = {symbol: download_last_24h(symbol) for symbol in symbols}
error_history = {symbol: [] for symbol in symbols}
last_processed = {symbol: None for symbol in symbols}
print("buffers ready")


def forecast_one_bar(symbol, bar_time, close):
    if last_processed[symbol] == bar_time:
        return None
    last_processed[symbol] = bar_time

    prices = price_history[symbol]
    prices = pd.concat([prices, pd.DataFrame([{"bar_time": bar_time, "close": close}])], ignore_index=True)
    prices = prices.drop_duplicates("bar_time").tail(1500)
    price_history[symbol] = prices

    if len(prices) < 16:
        return None

    series = prices["close"]
    log_return = (series / series.shift(1)).apply(lambda x: math.log(x) if x and x > 0 else 0.0)
    squared_return = log_return ** 2

    rv_15m = squared_return.tail(15).sum()
    rv_1h = squared_return.tail(60).sum()
    rv_24h = squared_return.tail(1440).sum()

    forecast = (har_intercept
                + har_weight_15m * math.log1p(rv_15m)
                + har_weight_1h * math.log1p(rv_1h)
                + har_weight_24h * math.log1p(rv_24h))

    # Residual in log space, defined exactly as in the offline notebook
    # (4_evaluate): log(realized RV) - log(HAR forecast). Both rv_15m and
    # `forecast` are compared in the same space the HAR model was trained on,
    # so this is one log on each side, not a log of a log. An eps guard keeps
    # log() finite if either value is ever zero.
    eps = 1e-12
    residual = math.log(rv_15m + eps) - math.log(forecast + eps)

    errors = error_history[symbol]
    errors.append(residual)
    errors[:] = errors[-history_size:]

    zscore = None
    is_anomaly = False
    if len(errors) >= 30:
        past = errors[:-1]   # exclude current point, matches notebook shift(1)
        mean = statistics.mean(past)
        # Sample std (ddof=1) to match pandas rolling().std() used offline.
        std = statistics.stdev(past)
        if std > 0:
            zscore = (residual - mean) / std
            is_anomaly = abs(zscore) > anomaly_threshold

    vol_forecast = math.sqrt(max(math.expm1(forecast), 0.0)) * 100
    vol_realized = math.sqrt(max(rv_15m, 0.0)) * 100

    return {
        "symbol": symbol,
        "bar_time": bar_time,
        "close": close,
        "rv_15m": rv_15m,
        "forecast": forecast,
        "vol_forecast": vol_forecast,
        "vol_realized": vol_realized,
        "residual": residual,
        "zscore": zscore,
        "is_anomaly": is_anomaly,
    }


def process_minute(batch, batch_id):
    bars = batch.collect()
    print(f">>> batch {batch_id}: {len(bars)} bars")

    for bar in bars:
        result = forecast_one_bar(bar["symbol"], bar["bar_time"], float(bar["close"]))
        if result is None:
            continue

        time = result["bar_time"]
        if hasattr(time, "to_pydatetime"):
            time = time.to_pydatetime()

        cassandra.execute(save_forecast, (
            result["symbol"], time.date(), time, float(result["close"]),
            float(result["rv_15m"]), float(result["forecast"]),
            float(result["vol_forecast"]), float(result["vol_realized"]),
            float(result["residual"]) if result["residual"] is not None else None,
            float(result["zscore"]) if result["zscore"] is not None else None,
        ))

        if result["is_anomaly"]:
            anomalies.insert_one({
                "symbol": result["symbol"],
                "event_time": time,
                "close": float(result["close"]),
                "forecast": float(result["forecast"]),
                "rv_15m": float(result["rv_15m"]),
                "zscore": float(result["zscore"]),
            })
            print(f"{result['symbol']:9} z={result['zscore']:.2f}  <<< ANOMALY (saved)")


stream = (minute_bars.writeStream
    .foreachBatch(process_minute)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start())

stream.awaitTermination()