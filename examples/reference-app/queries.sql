-- Diagnostic & Inspection Queries for Reference App

-- Inspect tier spending
SELECT tier, total_orders, total_amount FROM spending_by_tier ORDER BY tier;

-- Inspect store sales volume
SELECT store_id, order_count, total_volume FROM store_volume ORDER BY store_id;

-- Inspect detected fraud alerts
SELECT order_id, customer_id, store_id, amount, risk_score FROM fraud_alerts ORDER BY order_id;
