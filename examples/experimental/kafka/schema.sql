-- Kafka Streaming Schema & Incremental View
CREATE TABLE events (
    user_id BIGINT,
    duration_ms BIGINT
);

CREATE MATERIALIZED VIEW pageviews_by_user AS
SELECT
    user_id,
    COUNT(*) AS pageviews,
    SUM(duration_ms) AS total_duration_ms
FROM events
GROUP BY user_id;
