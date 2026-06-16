"""
train_models.py
================
Forecasting pipeline that runs AFTER the Spark batch_etl notebook has written
features to data/parquet/features.

This is plain Python (pandas / sklearn / PyTorch), NOT Spark. We leave Spark
because one symbol's data fits comfortably in memory, and sklearn/PyTorch need
pandas/numpy anyway.

Pipeline:
  1. Build the forward-looking target (leakage-critical)
  2. Chronological train/test split (never shuffle time)
  3. Persistence baseline   (the floor every model must beat)
  4. HAR-RV                 (linear benchmark)
  5. LSTM                   (deep model; must beat HAR-RV to justify itself)

Results on BTCUSDT (test RMSE on log-variance, lower is better):
  Persistence : 0.7259
  HAR-RV      : 0.6616
  LSTM        : 0.6492   <- winner, small train/test gap (generalizes)

NOTE: the forward target is built here in pandas for a self-contained script.
In the notebook it was built in Spark via a forward window
(rowsBetween(1, h)). Both produce the same strictly-future target.
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader


# ----------------------------------------------------------------------
# 0. Load features (written by the Spark notebook) and pick one symbol
# ----------------------------------------------------------------------
# If running outside Spark, read the Parquet directly with pandas/pyarrow.
# partitionBy("symbol") means each symbol is its own folder.
SYMBOL = "BTCUSDT"
PARQUET_DIR = "data/parquet/features"

df = pd.read_parquet(f"{PARQUET_DIR}/symbol={SYMBOL}")
df = df.sort_values("event_time").reset_index(drop=True)


# ----------------------------------------------------------------------
# 1. Forward-looking target  (LEAKAGE-CRITICAL)
# ----------------------------------------------------------------------
# Target = realised variance over the NEXT h minutes, (t, t+h].
# Built from sq_return, summed over a STRICTLY FUTURE window.
# The shift(-1) start is what keeps the current row out of the target.
def forward_rv(sq_return: pd.Series, h: int) -> pd.Series:
    # reverse -> rolling sum of h -> reverse back gives a forward sum,
    # then shift so it excludes the current row (strictly future).
    fwd = sq_return[::-1].rolling(h).sum()[::-1]
    return fwd.shift(-1)

df["target_rv_1h"] = forward_rv(df["sq_return"], 60)
df["target_log_rv_1h"] = np.log(df["target_rv_1h"])

# Drop warm-up nulls (start) and target nulls (last 60 rows).
df = df.dropna(subset=["target_log_rv_1h"]).reset_index(drop=True)


# ----------------------------------------------------------------------
# 2. Chronological split  (NEVER shuffle time)
# ----------------------------------------------------------------------
n = len(df)
split = int(n * 0.8)
train_df = df.iloc[:split]
test_df = df.iloc[split:]
print(f"train: {len(train_df)}  test: {len(test_df)}")
print("train ends:", train_df["event_time"].max())
print("test starts:", test_df["event_time"].min())


# ----------------------------------------------------------------------
# 3. Persistence baseline:  next-hour vol = this-hour vol
# ----------------------------------------------------------------------
# The dumbest honest forecast. Volatility is so persistent this is hard to beat.
persistence_pred = np.log(test_df["rv_1h"])
persistence_rmse = np.sqrt(
    mean_squared_error(test_df["target_log_rv_1h"], persistence_pred)
)
print(f"\nPersistence RMSE: {persistence_rmse:.4f}")


# ----------------------------------------------------------------------
# 4. HAR-RV:  linear regression on RV at 3 timescales
# ----------------------------------------------------------------------
# "future vol = weighted blend of recent / medium / long past vol."
# Linear models don't care about feature scale, so no normalization needed.
har_features = ["rv_1h", "rv_4h", "rv_24h"]

Xtr = np.log(train_df[har_features])
ytr = train_df["target_log_rv_1h"]
Xte = np.log(test_df[har_features])
yte = test_df["target_log_rv_1h"]

har = LinearRegression().fit(Xtr, ytr)
har_pred = har.predict(Xte)
har_rmse = np.sqrt(mean_squared_error(yte, har_pred))

print(f"HAR-RV RMSE:      {har_rmse:.4f}")
print("coefficients:", dict(zip(har_features, har.coef_.round(3))))
# Expect rv_1h to dominate -> volatility clustering (recent matters most).


# ----------------------------------------------------------------------
# 5. LSTM:  reads a sequence of timesteps, learns temporal patterns
# ----------------------------------------------------------------------
# Richer feature set than HAR-RV: give the deep model more to learn from,
# otherwise it's just relearning linear regression.
lstm_features = [
    "rv_1h", "rv_4h", "rv_24h",
    "log_return", "volume_zscore", "parkinson_1h", "buy_ratio",
]
TARGET = "target_log_rv_1h"
SEQ_LEN = 48          # 48 timesteps of history per prediction
BATCH = 256
EPOCHS = 8

# 5a. normalize features: FIT on train only, TRANSFORM both (no scaler leak)
scaler = StandardScaler()
Xtr_raw = scaler.fit_transform(train_df[lstm_features])
Xte_raw = scaler.transform(test_df[lstm_features])
ytr_raw = train_df[TARGET].values
yte_raw = test_df[TARGET].values


# 5b. build sequences: flat table -> (samples, SEQ_LEN, n_features)
# Build train and test sequences SEPARATELY so no sequence straddles the
# train/test boundary (that would leak).
def make_sequences(X, y, seq_len):
    Xs, ys = [], []
    for i in range(len(X) - seq_len):
        Xs.append(X[i:i + seq_len])      # seq_len timesteps
        ys.append(y[i + seq_len - 1])    # target at the window's last step
    return np.array(Xs), np.array(ys)


X_train, y_train = make_sequences(Xtr_raw, ytr_raw, SEQ_LEN)
X_test, y_test = make_sequences(Xte_raw, yte_raw, SEQ_LEN)
print(f"\nX_train: {X_train.shape}  X_test: {X_test.shape}")


# 5c. model: 2-layer LSTM (hidden 64, dropout 0.2) + linear head
class VolLSTM(nn.Module):
    def __init__(self, n_features=7, hidden=64, layers=2, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features, hidden_size=hidden,
            num_layers=layers, dropout=dropout, batch_first=True,
        )
        self.head = nn.Linear(hidden, 1)

    def forward(self, x):
        out, _ = self.lstm(x)     # (batch, seq_len, hidden)
        last = out[:, -1, :]      # summary after reading the whole sequence
        return self.head(last)    # (batch, 1)


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("training on:", device)

model = VolLSTM(n_features=len(lstm_features)).to(device)

Xtr_t = torch.tensor(X_train, dtype=torch.float32)
ytr_t = torch.tensor(y_train, dtype=torch.float32).unsqueeze(1)
Xte_t = torch.tensor(X_test, dtype=torch.float32)
yte_t = torch.tensor(y_test, dtype=torch.float32).unsqueeze(1)

train_loader = DataLoader(TensorDataset(Xtr_t, ytr_t), batch_size=BATCH, shuffle=True)


# 5d. training loop — the 5 steps every neural net runs:
# zero grads -> forward -> loss -> backward -> step
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

for epoch in range(EPOCHS):
    model.train()
    total = 0.0
    for xb, yb in train_loader:
        xb, yb = xb.to(device), yb.to(device)
        optimizer.zero_grad()
        pred = model(xb)
        loss = criterion(pred, yb)
        loss.backward()
        optimizer.step()
        total += loss.item() * len(xb)
    print(f"epoch {epoch + 1}/{EPOCHS}  train MSE: {total / len(Xtr_t):.4f}")

# save weights so a crash never costs the retrain
torch.save(model.state_dict(), "lstm_btc.pt")


# 5e. evaluate — BATCH the inference too (one big forward pass OOMs the kernel)
model.eval()
preds = []
test_loader = DataLoader(TensorDataset(Xte_t, yte_t), batch_size=BATCH, shuffle=False)
with torch.no_grad():
    for xb, _ in test_loader:
        preds.append(model(xb.to(device)).cpu().numpy())
lstm_pred = np.concatenate(preds).flatten()
lstm_rmse = np.sqrt(mean_squared_error(y_test, lstm_pred))


# ----------------------------------------------------------------------
# Final comparison
# ----------------------------------------------------------------------
print("\n=== Test RMSE (log-variance, lower is better) ===")
print(f"Persistence : {persistence_rmse:.4f}")
print(f"HAR-RV      : {har_rmse:.4f}")
print(f"LSTM        : {lstm_rmse:.4f}")