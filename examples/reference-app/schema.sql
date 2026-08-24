-- RockStream Reference Application Schema
-- Real-Time E-Commerce Analytics & Fraud Detection Pipeline

CREATE TABLE customers (
    customer_id BIGINT,
    tier VARCHAR(32)
);

CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    store_id BIGINT,
    amount BIGINT,
    risk_score BIGINT
);

-- Tier spending aggregation
CREATE MATERIALIZED VIEW spending_by_tier AS
SELECT
    c.tier,
    COUNT(*) AS total_orders,
    SUM(o.amount) AS total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.tier;

-- Store volume leaderboard
CREATE MATERIALIZED VIEW store_volume AS
SELECT
    store_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_volume
FROM orders
GROUP BY store_id;

-- High-risk fraud alerts view
CREATE MATERIALIZED VIEW fraud_alerts AS
SELECT
    order_id,
    customer_id,
    store_id,
    amount,
    risk_score
FROM orders
WHERE risk_score >= 80;
