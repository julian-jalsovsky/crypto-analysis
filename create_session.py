import asyncio
import sys
from signal import SIGINT, SIGTERM
import psycopg

from binance_client import AsyncClient
import sql_queries as query
from config import api_key, secret_key, connection_string as conn_str

MAX_BATCH_SIZE = 30
RECENT_TRADES = 1000

TICKER = "ethusdt"


def signal_handler(sig):
    loop = asyncio.get_running_loop()
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()
    print(f'Got signal: {sig!s}, shutting down.')
    loop.remove_signal_handler(SIGTERM)
    loop.add_signal_handler(SIGINT, lambda: None)


async def create_session(db_conn):
    async with db_conn.cursor() as cursor:
        await cursor.execute(query.insert_session, (TICKER.upper(), None, None))
        last_index = await cursor.fetchone()

        return last_index[0]


async def get_recent_trades(client, db_conn, session_id):
    trades = await client.recent_trades(TICKER.upper(), RECENT_TRADES)

    rows = [(session_id, trade['time'], trade['id'], trade['price'],
             trade['qty'], None, None, trade['isBuyerMaker'])
            for trade in trades]

    async with db_conn.cursor() as cursor:
        await cursor.executemany(query.insert_trade, rows)

    return trades[0]['time'], trades[-1]['time'], trades[-1]['id']


async def get_candlesticks(client, db_conn, session_id, first_trade, last_trade):
    rows = []
    first_sec = int(first_trade / 1000) * 1000
    last_sec = int(last_trade / 1000) * 1000
    time_pointer = first_sec

    while time_pointer < last_sec:
        klines = await client.candlestick_data(TICKER.upper(), "1s", time_pointer)
        batch = [(session_id, kline[0], kline[6], None, None, kline[1],
                  kline[4], kline[2], kline[3], kline[5], kline[8],
                  kline[7], kline[9], kline[10]) for kline in klines]
        rows.extend(batch)
        time_pointer = klines[-1][0]

    async with db_conn.cursor() as cursor:
        await cursor.executemany(query.insert_kline, rows)

    return klines[-1][0]


async def ws_data(queue, db_conn, session_id, last_id, last_kline):
    trades_batch, klines_batch = [], []
    current_batch = 0

    while True:
        socket = await queue.get()
        try:
            print(socket)
            if socket['e'] == 'trade' and socket['t'] > last_id:
                trade = (session_id, socket['T'], socket['t'], socket['p'],
                         socket['q'], socket['b'], socket['a'], socket['m'])
                trades_batch.append(trade)
                current_batch += 1

            if socket['e'] == 'kline' and socket['k']['t'] > last_kline:
                kline = (session_id, socket['k']['t'], socket['k']['T'],
                         socket['k']['f'], socket['k']['L'], socket['k']['o'],
                         socket['k']['c'], socket['k']['h'], socket['k']['l'],
                         socket['k']['v'], socket['k']['n'], socket['k']['q'],
                         socket['k']['V'], socket['k']['Q'])
                klines_batch.append(kline)
                current_batch += 1

            if current_batch == MAX_BATCH_SIZE:
                async with db_conn.cursor() as cursor:
                    await cursor.executemany(query.insert_trade, trades_batch)
                    await cursor.executemany(query.insert_kline, klines_batch)
                trades_batch, klines_batch = [], []
                current_batch = 0

        except KeyError as error:
            print(f"Websocket KeyError: {error}. Socket: {socket}.")


async def update_session_timestamps(db_conn, session_id):
    async with db_conn.cursor() as cursor:
        await cursor.execute(f"SELECT trade_time FROM trades \
                             WHERE session_id = {session_id}")
        timestamps = await cursor.fetchall()
        await cursor.execute(f"UPDATE sessions SET begin_time={timestamps[0][0]},\
                        end_time={timestamps[-1][0]} WHERE id={session_id}")


async def main():
    queue = asyncio.Queue()
    client = AsyncClient(api_key, secret_key)
    connection = await asyncio.create_task(client.ws_connect())

    loop = asyncio.get_running_loop()
    for sig in (SIGTERM, SIGINT):
        try:
            loop.add_signal_handler(sig, signal_handler, sig)
        except NotImplementedError:
            pass

    async with await psycopg.AsyncConnection.connect(conn_str) as db_conn:
        try:
            async with connection as websocket:
                await asyncio.create_task(client.ws_subscribe(websocket,
                                                              [f"{TICKER}@trade",
                                                               f"{TICKER}@kline_1s"]))

                asyncio.create_task(client.ws_listen(websocket, queue))

                session_id = await asyncio.create_task(create_session(db_conn))

                first_trade, last_trade, last_id = await asyncio.create_task(
                    get_recent_trades(client, db_conn, session_id))

                last_kline = await asyncio.create_task(get_candlesticks(
                    client, db_conn, session_id, first_trade, last_trade))

                asyncio.create_task(
                    ws_data(queue, db_conn, session_id, last_id, last_kline))
                await asyncio.Future()

        except asyncio.CancelledError:
            print('<Your app is shutting down...>')

        finally:
            await update_session_timestamps(db_conn, session_id)
            print("FUCK OFF")


if __name__ == '__main__':

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
