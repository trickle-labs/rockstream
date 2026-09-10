-- Source PostgreSQL Database Setup
CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    region VARCHAR(32) NOT NULL
);

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(id),
    total BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL
);

CREATE PUBLICATION rockstream_pub FOR ALL TABLES;

INSERT INTO customers (id, name, region) VALUES
(1, 'Alice', 'EMEA'),
(2, 'Bob', 'AMER'),
(3, 'Charlie', 'APAC');

INSERT INTO orders (id, customer_id, total, status) VALUES
(101, 1, 150, 'COMPLETED'),
(102, 2, 200, 'COMPLETED'),
(103, 1, 50, 'COMPLETED'),
(104, 3, 300, 'PENDING');
