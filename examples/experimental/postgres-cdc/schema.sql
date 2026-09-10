-- RockStream Incremental View over CDC Tables
CREATE TABLE customers (
    id BIGINT,
    region_id BIGINT
);

CREATE TABLE orders (
    id BIGINT,
    customer_id BIGINT,
    total BIGINT
);

CREATE MATERIALIZED VIEW sales_by_region AS
SELECT
    c.region_id,
    SUM(o.total) AS total_sales
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.region_id;
