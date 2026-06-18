CREATE KEYSPACE IF NOT EXISTS binance
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS binance.forecasts (
  symbol text,
  day date,
  event_time timestamp,
  rv_15m double,
  forecast double,
  residual double,
  zscore double,
  PRIMARY KEY ((symbol, day), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);