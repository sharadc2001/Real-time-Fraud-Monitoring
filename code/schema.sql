CREATE TABLE IF NOT EXISTS fraud_events (
  transaction_id TEXT PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL,
  customer_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  merchant_id TEXT NOT NULL,
  merchant_category TEXT,
  channel TEXT,
  amount_inr NUMERIC(16,2),
  currency TEXT,
  city TEXT,
  country TEXT,
  device_id TEXT,
  is_new_device BOOLEAN,
  synthetic_fraud_label BOOLEAN,
  synthetic_scenario TEXT,
  velocity_count_60s INTEGER,
  risk_score INTEGER,
  risk_level TEXT,
  processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fraud_event_time ON fraud_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_risk ON fraud_events (risk_level, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_merchant ON fraud_events (merchant_id, event_time DESC);
