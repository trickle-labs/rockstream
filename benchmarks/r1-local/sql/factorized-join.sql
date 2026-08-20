CREATE TABLE r1_source (id BIGINT PRIMARY KEY, group_id BIGINT NOT NULL, dimension_id BIGINT NOT NULL, value BIGINT NOT NULL, active BOOLEAN NOT NULL);
CREATE TABLE r1_dimension (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL);
CREATE MATERIALIZED VIEW r1_factorized AS SELECT d.bucket, SUM(s.value) AS total FROM r1_source s JOIN r1_dimension d ON s.dimension_id = d.id WHERE s.active = TRUE GROUP BY d.bucket;
