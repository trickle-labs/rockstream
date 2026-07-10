-- RockStream v0.42 Reference App: Initial Schema

CREATE TABLE IF NOT EXISTS customers (
    id         INT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id          INT PRIMARY KEY,
    customer_id INT NOT NULL,
    amount      DECIMAL(12,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id        INT PRIMARY KEY,
    order_id  INT NOT NULL,
    product   TEXT NOT NULL,
    quantity  INT NOT NULL,
    price     DECIMAL(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    id         SERIAL PRIMARY KEY,
    channel    TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE VIEW sales_summary AS
    SELECT
        c.id   AS customer_id,
        c.name AS customer_name,
        COUNT(o.id)     AS order_count,
        SUM(o.amount)   AS total_amount
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.id
    GROUP BY c.id, c.name;

CREATE MATERIALIZED VIEW IF NOT EXISTS sales_summary_mv AS
    SELECT
        c.id   AS customer_id,
        c.name AS customer_name,
        COUNT(o.id)     AS order_count,
        SUM(o.amount)   AS total_amount
    FROM customers c
    LEFT JOIN orders o ON o.customer_id = c.id
    GROUP BY c.id, c.name;
