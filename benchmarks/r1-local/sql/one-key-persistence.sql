CREATE TABLE r1_source (id BIGINT PRIMARY KEY, group_id BIGINT NOT NULL, dimension_id BIGINT NOT NULL, value BIGINT NOT NULL, active BOOLEAN NOT NULL);
CREATE MATERIALIZED VIEW r1_one_key AS SELECT group_id, SUM(value) AS total FROM r1_source GROUP BY group_id;
