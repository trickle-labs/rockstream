-- RockStream Demo: Orders Scenario
-- Demonstrates DDL, DML, and deterministic incremental materialized view maintenance over PostgreSQL wire protocol.

-- 1. Create base table for incoming orders
CREATE TABLE orders (order_id BIGINT, store_id BIGINT, amount BIGINT);

-- 2. Define materialized view computing aggregate sum per store
CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;

-- 3. Insert initial orders across multiple stores
INSERT INTO orders VALUES (1, 100, 50), (2, 100, 70), (3, 200, 40);

-- 4. Query materialized view — initial aggregation state [(100, 120), (200, 40)]
SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;

-- 5. Update an existing order (increases store 100 total by 50)
UPDATE orders SET amount = 100 WHERE order_id = 1, store_id = 100, amount = 50;

-- 6. Query materialized view — updated aggregation state [(100, 170), (200, 40)]
SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;

-- 7. Delete an order for store 200
DELETE FROM orders WHERE order_id = 3, store_id = 200, amount = 40;

-- 8. Query materialized view — retracted state [(100, 170)]
SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;

