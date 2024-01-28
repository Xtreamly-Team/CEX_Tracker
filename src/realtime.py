import asyncio
import gc
from typing import List
import ccxt.pro as ccxt

from asyncio import run, gather, sleep

from models import OHLCVC, Orderbook, Trade
from utils import send_to_db_sqs, write_ohlcvs_to_csv, write_orderbooks_to_csv, write_orderbooks_to_csv_with_panda, write_trades_to_csv

from copy import copy, deepcopy
from dotenv import dotenv_values


config = dotenv_values('./src/.env')
db_queue_url = config['SQS_URL']
db_collection = config['DB_COLLECTION']

async def main():

    spot_exchange = ccxt.binance({
        'apiKey': config['BINANCE_API_KEY'],
        'secret': config['BINANCE_API_SECRET'],
        'options': {
            'defaultType': 'spot',
            'tradesLimit': 10000,
        },
    })

    future_exchange = ccxt.binance({
        'apiKey': config['BINANCE_API_KEY'],
        'secret': config['BINANCE_API_SECRET'],
        'options': {
            'defaultType': 'future',
            'tradesLimit': 10000,
        },
    })


    eth = 'ETH/USDT'
    btc = 'BTC/USDT'

    def get_symbol_quotes(symbols):
     return [x.split('/')[0] for x in symbols]

    async def get_time():
        return await spot_exchange.fetch_time();

    async def get_formatted_time():
        return spot_exchange.iso8601(spot_exchange.milliseconds())

    async def get_trades_raw(symbol: str, since: int, until: int, market='spot'):
        last_trade_timestamp = since
        all_trades = []
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange
        else:
            raise Exception('Invalid market type')

        while last_trade_timestamp < until:
            each_step = 1000
            trades = await exchange.fetch_trades(symbol, last_trade_timestamp, each_step, params={'until': until})
            if len(trades):
                last_trade_timestamp = trades[len(trades) - 1]['timestamp'] + 1
                all_trades += trades
            else:
                break

        return all_trades

    def get_trades(symbol: str, trades_raw: List, market='spot'):
        all_trades = list(map(lambda t: Trade(
            symbol,
            t['timestamp'],
            t['amount'],
            t['price'],
            t['side'] == 'buy',
            market
        ), trades_raw))
        return all_trades


    async def watch_orderbooks_raw(symbols, market='spot', tick_limit: int=10000, depth: int=10):
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange
        else:
            raise Exception('Invalid market type')
        order_books = []
        i = 0
        while True:
            i += 1
            if i > tick_limit:
                break
            try:
                order_book = await exchange.watch_order_book_for_symbols(symbols, depth)
                order_books.append(copy(order_book))
            except Exception as e:
                print(e)
                await exchange.close()

        return order_books

    async def watch_orderbooks(queue: asyncio.Queue, symbols, depth: int, market='spot', tick_limit: int=10000, return_results=False):
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange
        else:
            raise Exception('Invalid market type')
        order_books: List[Orderbook] = []
        i = 0
        while True:
            i += 1
            # print(i)
            if i > tick_limit:
                break
            try:
                raw_order_book = await spot_exchange.watch_order_book_for_symbols(symbols, depth)

                order_book = Orderbook(raw_order_book['symbol'].split(':')[0], raw_order_book['timestamp'],
                                       list(map(lambda o: (o[0], round(o[1] * o[0], 2)), raw_order_book['asks'])),
                                       list(map(lambda o: (o[0], round(o[1] * o[0], 2)), raw_order_book['bids'])),
                                       market,
                                 )
                
                await queue.put(order_book)
                if return_results:
                    order_books.append(copy(order_book))
            except Exception as e:
                print(e)

        return order_books

    async def watch_trades(queue: asyncio.Queue, symbols, market='spot', tick_limit=10000, return_results=False):
        trades = []
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange
        else:
            raise Exception('Invalid market type')
        i = 0
        while True:
            i += 1
            # print(i)
            if i > tick_limit:
                break
            try:
                current_trades_raw = await exchange.watch_trades_for_symbols(symbols)
                # print(current_trades_raw)
                current_trades = list(map(lambda t: Trade(
                    t['symbol'].split(':')[0].replace('/', '-'),
                    t['timestamp'],
                    round(t['amount'] * t['price'], 2),
                    t['price'],
                    t['side'] == 'buy',
                    market,
                ), current_trades_raw))

                for trade in current_trades:
                    await queue.put(trade)
                if return_results:
                    trades = [* trades, *current_trades]
                
            except Exception as e:
                print(e)
                await spot_exchange.close()

        return trades


    async def trade_loop(symbols: List[str], market='spot', tick_num: int=10000, min_save_step=1000):
        trades_queue = asyncio.Queue()
        async def get_trades():
            await watch_trades(trades_queue, symbols, tick_num, market=market)

        async def save_trades():
            total_got = 0
            while True:
                await asyncio.sleep(min_save_step / 10)
                batch_size = trades_queue.qsize()
                if batch_size >= min_save_step:
                    batch: List[Trade] = []
                    while not trades_queue.empty():
                        batch.append(await trades_queue.get())
                    # This should return close to 0
                    print('Batch size to write', len(batch))
                    
                    send_to_db_sqs(db_queue_url, db_collection, batch)
                    # write_trades_to_csv(f'./data/trades/{"-".join(get_symbol_quotes(symbols))}_trades_{total_got + 1}_{total_got + batch_size}.csv', batch)
                    total_got += batch_size

                if total_got >= tick_num:
                    print("Finished saving trades")
                    break

        await gather(get_trades(), save_trades())

    async def order_book_loop(symbols: List[str], market='spot', tick_num: int=10000, min_save_step=1000, depth=10):
        orderbook_queue = asyncio.Queue()
        async def get_orderbooks():
            while True:
                await watch_orderbooks(orderbook_queue, symbols, depth, tick_num, market=market)

        async def save_orderbooks():
            total_got = 0
            while True:
                await asyncio.sleep(min_save_step / 20)
                batch_size = orderbook_queue.qsize()
                print(batch_size)
                if batch_size >= min_save_step:
                    batch: List[Orderbook] = []
                    while not orderbook_queue.empty():
                        batch.append(await orderbook_queue.get())
                    # This should return close to 0
                    print(orderbook_queue.qsize())
                    try:
                        # write_orderbooks_to_csv(f'data/orderbooks/orderbooks_{total_got + 1}_{total_got + batch_size}.csv', batch)
                        write_orderbooks_to_csv_with_panda(f'./data/orderbooks/{"-".join(get_symbol_quotes(symbols))}_orderbooks_{total_got + 1}_{total_got + batch_size}.csv', batch)
                        del batch

                    except Exception as e:
                        print(e)
                    total_got += batch_size

                if total_got + batch_size >= tick_num:
                    print("Finished saving orderbooks")
                    break

        await gather(get_orderbooks(), save_orderbooks())


    try:
        run_symbols = [eth, btc]

        await gather(
            order_book_loop(run_symbols, 'spot', 1_000, 200, 10),
            trade_loop(run_symbols, 'spot', 1_000, 200),
        )

    except Exception as e:
        print(e)

    finally:
        await spot_exchange.close()
        await future_exchange.close()

run(main())
