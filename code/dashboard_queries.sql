-- Superset SQL Lab: high risk transactions over the last 15 minutes
SELECT date_trunc('minute', event_time) AS minute,
       COUNT(*) AS total_transactions,
       COUNT(*) FILTER (WHERE risk_level = 'HIGH') AS high_risk_transactions,
       ROUND(100.0 * COUNT(*) FILTER (WHERE risk_level = 'HIGH') / NULLIF(COUNT(*),0),2) AS high_risk_pct
FROM fraud_events
WHERE event_time >= NOW() - INTERVAL '15 minutes'
GROUP BY 1 ORDER BY 1;

-- Superset SQL Lab: top merchants by high-risk amount
SELECT merchant_id,
       COUNT(*) AS high_risk_count,
       SUM(amount_inr) AS high_risk_amount_inr
FROM fraud_events
WHERE risk_level = 'HIGH' AND event_time >= NOW() - INTERVAL '1 hour'
GROUP BY merchant_id ORDER BY high_risk_amount_inr DESC LIMIT 20;

-- Superset SQL Lab: geographic distribution
SELECT country, city, COUNT(*) AS transactions,
       COUNT(*) FILTER (WHERE risk_level='HIGH') AS high_risk_transactions
FROM fraud_events
WHERE event_time >= NOW() - INTERVAL '1 hour'
GROUP BY country, city ORDER BY high_risk_transactions DESC;
