from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

from cassandra.cluster import Cluster
from pymongo import MongoClient

import json
import urllib.request 
import math
import pandas as pd

# ---- hardcoded artifacts (HAR coeffs + detector config) ----
HAR_COEF = {"rv_15m": 0.16732474788682988,
            "rv_1h": 0.009128281423598323,
            "rv_24h": 0.00032560033442801447}
HAR_INTERCEPT = 1.9270090168323285e-05
K = 4.0
ROLL = 288

SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT",
           "AVAXUSDT","DOTUSDT","LINKUSDT","POLUSDT","LTCUSDT","TRXUSDT","ATOMUSDT",
           "UNIUSDT","NEARUSDT","APTUSDT","FILUSDT","ETCUSDT","XLMUSDT"]

cass = Cluster(["cassandra"]).connect("binance")
cass_insert = cass.prepare("""
    INSERT INTO forecasts (symbol, day, event_time, close, rv_15m, forecast, vol_forecast, vol_realized, residual, zscore)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

mongo = MongoClient("mongodb://mongodb:27017")["binance"]["anomalies"]
print("storage connected")

spark = (SparkSession.builder
    .appName("binance-stream")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate())
spark.conf.set("spark.sql.caseSensitive", "true")
spark.sparkContext.setLogLevel("WARN")

# Binance kline_1s inner "data" shape (the env.Data your producer forwards)
kline_schema = StructType([
    StructField("e", StringType()),          # event type
    StructField("E", LongType()),            # event time (ms)
    StructField("s", StringType()),          # symbol
    StructField("k", StructType([
        StructField("t", LongType()),        # kline start (ms)
        StructField("T", LongType()),        # kline close (ms)
        StructField("c", StringType()),      # close price
        StructField("h", StringType()),      # high
        StructField("l", StringType()),      # low
        StructField("v", StringType()),      # base volume
        StructField("V", StringType()),      # taker buy base volume
        StructField("x", BooleanType()),     # is closed
    ])),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw_klines")
    .option("startingOffsets", "latest")
    .load())

parsed = (raw
    .select(F.from_json(F.col("value").cast("string"), kline_schema).alias("d"))
    .select(
        F.col("d.s").alias("symbol"),
        (F.col("d.k.T") / 1000).cast("timestamp").alias("event_time"),
        F.col("d.k.c").cast("double").alias("close"),
        F.col("d.k.h").cast("double").alias("high"),
        F.col("d.k.l").cast("double").alias("low"),
        F.col("d.k.v").cast("double").alias("volume"),
        F.col("d.k.V").cast("double").alias("taker_buy_base"),
    ))

bars_1m = (parsed
    .withWatermark("event_time", "30 seconds")        # tolerate 30s late events
    .groupBy(
        F.window("event_time", "1 minute"),
        F.col("symbol"),
    )
    .agg(
        F.last("close").alias("close"),               # last price in the minute
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.sum("volume").alias("volume"),
        F.sum("taker_buy_base").alias("taker_buy_base"),
    )
    .select(
        F.col("symbol"),
        F.col("window.end").alias("bar_time"),        # the minute this bar closes
        "close", "high", "low", "volume", "taker_buy_base",
    ))

def fetch_24h(symbol):
    rows = []
    for _ in range(2):  # 1440 bars > 1000/call -> 2 calls
        url = (f"https://api.binance.com/api/v3/klines?symbol={symbol}"
               f"&interval=1m&limit=1000")
        data = json.loads(urllib.request.urlopen(url, timeout=10).read())
        rows = data  # last 1000 is enough for rv_24h≈1440? -> see note
        break
    df = pd.DataFrame(rows, columns=range(12))
    df = df[[6, 4]].copy()                      # close_time(ms), close
    df.columns = ["t", "close"]
    df["close"] = df["close"].astype(float)
    df["bar_time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    return df[["bar_time", "close"]]

print("warming up buffers...")
buffers = {s: fetch_24h(s) for s in SYMBOLS}
hist = {s: [] for s in SYMBOLS}  # residual history for the detector
print("buffers ready")

def compute_and_forecast(sym, bar_time, close):
    buf = buffers[sym]
    # append the new live bar
    buf = pd.concat([buf, pd.DataFrame([{"bar_time": bar_time, "close": close}])],
                    ignore_index=True)
    buf = buf.drop_duplicates("bar_time").tail(1500)  # ring buffer
    buffers[sym] = buf
    if len(buf) < 16:
        return None

    c = buf["close"].values
    lr = pd.Series(c)
    lr = (pd.Series(c) / pd.Series(c).shift(1)).apply(lambda x: math.log(x) if x and x > 0 else 0.0)
    sq = lr ** 2

    def trail_sum(n): 
        return sq.tail(n).sum() 
    
    rv15, rv1h, rv24 = trail_sum(15), trail_sum(60), trail_sum(1440)

    if len(sq) < 16:
        return None

    # HAR on log1p features
    fc = (HAR_INTERCEPT
          + HAR_COEF["rv_15m"] * math.log1p(rv15)
          + HAR_COEF["rv_1h"]  * math.log1p(rv1h)
          + HAR_COEF["rv_24h"] * math.log1p(rv24))

    realized = math.log1p(rv15)       
    resid = math.log(realized) - math.log(fc)  

    h = hist[sym]
    h.append(resid)
    h[:] = h[-ROLL:]
    z = None
    anomaly = False

    vol_forecast = math.sqrt(max(math.expm1(fc), 0.0)) * 100
    vol_realized = math.sqrt(max(rv15, 0.0)) * 100

    if len(h) >= 30:
        import statistics
        m, sd = statistics.mean(h[:-1]), statistics.pstdev(h[:-1])
        if sd > 0:
            z = (resid - m) / sd
            anomaly = abs(z) > K
    return {"symbol": sym, "bar_time": bar_time, "close": close, "rv_15m": rv15,
            "forecast": fc, "vol_forecast": vol_forecast, "vol_realized": vol_realized,
            "residual": resid, "zscore": z, "anomaly": anomaly}

def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    print(f">>> batch {batch_id}: {len(rows)} bars")
    for r in rows:
        try:
            o = compute_and_forecast(r["symbol"], r["bar_time"], float(r["close"]))
            if not o:
                continue

            # write forecast to Cassandra (every bar)
            bt = o["bar_time"].to_pydatetime() if hasattr(o["bar_time"], "to_pydatetime") else o["bar_time"]
            cass.execute(cass_insert, (
                o["symbol"], bt.date(), bt, float(o["close"]),
                float(o["rv_15m"]), float(o["forecast"]),
                float(o["vol_forecast"]), float(o["vol_realized"]),
                float(o["residual"]) if o["residual"] is not None else None,
                float(o["zscore"]) if o["zscore"] is not None else None,
            ))

            # write anomaly to Mongo (only when flagged)
            if o["anomaly"]:
                mongo.insert_one({
                    "symbol": o["symbol"],
                    "event_time": bt,
                    "close": float(r["close"]),
                    "forecast": float(o["forecast"]),
                    "rv_15m": float(o["rv_15m"]),
                    "zscore": float(o["zscore"]),
                })
                print(f"{o['symbol']:9} z={o['zscore']:.2f}  <<< ANOMALY (saved)")
        except Exception as e:
            print("error:", r["symbol"], e)

query = (bars_1m.writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()