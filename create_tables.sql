CREATE TABLE sessions (
    id serial PRIMARY KEY,
    coin VARCHAR (10) NOT NULL,
    begin_time BIGINT,
    end_time BIGINT);

CREATE TABLE trades (
    id serial PRIMARY KEY,
    session_id INT NOT NULL,
    trade_time BIGINT NOT NULL,
    trade_id BIGINT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    buyer_order_id INT,
    seller_order_id INT,
    is_buyer_mm BOOLEAN,
    FOREIGN KEY (session_id) REFERENCES sessions (id)
);

CREATE TABLE klines_1s (
    id serial PRIMARY KEY,
    session_id INT NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    first_trade_id BIGINT,
    last_trade_id BIGINT,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    base_asset_vol DOUBLE PRECISION NOT NULL,
    num_of_trades INT NOT NULL,
    quote_asset_vol DOUBLE PRECISION NOT NULL,
    taker_buy_base_vol DOUBLE PRECISION NOT NULL,
    taker_buy_quote_vol DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions (id)
);