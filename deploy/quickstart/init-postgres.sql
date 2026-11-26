-- RockStream 5-Minute PostgreSQL CDC Quickstart Seed Schema
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    amount_cents BIGINT NOT NULL,
    status TEXT NOT NULL
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO customers (customer_id, customer_name, region) VALUES
    (1, 'Acme Corp', 'us-east'),
    (2, 'Nordic Retail', 'eu-north'),
    (3, 'Pacific Labs', 'us-west')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, amount_cents, status) VALUES
    (101, 1, 15000, 'paid'),
    (102, 2, 24000, 'paid'),
    (103, 1, 8000, 'paid'),
    (104, 3, 19500, 'pending')
ON CONFLICT (order_id) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'rockstream_pub') THEN
        CREATE PUBLICATION rockstream_pub FOR TABLE customers, orders;
    END IF;
END
$$;
