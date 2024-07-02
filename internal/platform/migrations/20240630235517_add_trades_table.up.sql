CREATE TABLE trades
(
    id              SERIAL PRIMARY KEY,
    instrument_code VARCHAR(255),
    trade_price     DECIMAL(19, 4),
    trade_quantity  INT,
    close_time      VARCHAR(50),
    trade_date      TIMESTAMP
);