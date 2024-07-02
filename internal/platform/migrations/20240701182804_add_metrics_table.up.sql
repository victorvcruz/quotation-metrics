CREATE TABLE metrics
(
    id               SERIAL PRIMARY KEY,
    ticker           VARCHAR(255),
    max_range_value  DECIMAL(19, 4),
    max_daily_volume INT,
    trade_date       TIMESTAMP
);