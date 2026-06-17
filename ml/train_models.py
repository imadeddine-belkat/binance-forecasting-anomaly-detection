import numpy as np
import pandas as pd
from pyspark.sql.connect.functions import window
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader


SYMBOL = "BTCUSDT"
PARQUET_DIR = "../data/parquet/features"

df = pd.read_parquet(f"{PARQUET_DIR}/symbol={SYMBOL}")
df = df.sort_values("event_time").reset_index(drop=True)

