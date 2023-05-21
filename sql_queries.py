insert_session = "INSERT INTO sessions (coin, begin_time, end_time) \
            VALUES (%s, %s, %s) RETURNING id"


insert_trade = "INSERT INTO trades (session_id, trade_time, trade_id, price, \
            quantity, buyer_order_id, seller_order_id, is_buyer_mm) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"


insert_kline = "INSERT INTO klines_1s (session_id, open_time, close_time, \
            first_trade_id, last_trade_id, open_price, close_price, high_price,\
            low_price, base_asset_vol, num_of_trades, quote_asset_vol, \
            taker_buy_base_vol, taker_buy_quote_vol) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
