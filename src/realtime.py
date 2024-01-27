import asyncio
import gc
from typing import List
import ccxt.pro as ccxt

from asyncio import run, gather, sleep
from pprint import pprint

from models import OHLCVC, Orderbook, Trade
from utils import write_ohlcvs_to_csv, write_orderbooks_to_csv, write_orderbooks_to_csv_with_panda, write_trades_to_csv

from copy import copy, deepcopy

async def main():

    exchange = ccxt.binance({
        'apiKey': 'nxzLrvdf22SWpN2oydMUGXHXMNjOkeBbxHXI3lkyBF9FoyrqCByx6gewqCtEhd0s',
        'secret': 'ItHZgtXWeXJibop5sPTqKA2BygFZkTQLcCga4z8KevZtEprrHaD1jQwov6HwQDdM',
        'options': {
            'defaultType': 'spot',
            'tradesLimit': 10000,
        },
    })

    eth = 'ETH/USDT'
    btc = 'BTC/USDT'

    symbols = [eth, btc]
    def get_symbol_quotes(symbols):
     return [x.split('/')[0] for x in symbols]

    async def get_time():
        return await exchange.fetch_time();

    async def get_formatted_time():
        return exchange.iso8601(exchange.milliseconds())

    async def get_trades_raw(symbol: str, since: int, until: int):
        last_trade_timestamp = since
        all_trades = []

        while last_trade_timestamp < until:
            each_step = 1000
            trades = await exchange.fetch_trades(symbol, last_trade_timestamp, each_step, params={'until': until})
            if len(trades):
                last_trade_timestamp = trades[len(trades) - 1]['timestamp'] + 1
                all_trades += trades
            else:
                break

        return all_trades

    def get_trades(symbol: str, trades_raw: List):
        all_trades = list(map(lambda t: Trade(
            symbol,
            t['timestamp'],
            t['amount'],
            t['price'],
            t['side'] == 'buy'
        ), trades_raw))
        return all_trades


    def get_current_order_book(symbol: str, raw_order_book):
        return Orderbook(symbol, raw_order_book['timestamp'],
                         raw_order_book['asks'],
                         raw_order_book['bids']
                         )

    async def watch_orderbooks_raw(symbols, depth: int, tick_limit: int=10000):
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

    async def watch_orderbooks(queue: asyncio.Queue, symbols, depth: int, tick_limit: int=10000, return_results=False):
        order_books: List[Orderbook] = []
        i = 0
        while True:
            i += 1
            # print(i)
            if i > tick_limit:
                break
            try:
                raw_order_book = await exchange.watch_order_book_for_symbols(symbols, depth)

                order_book = Orderbook(raw_order_book['symbol'].split(':')[0], raw_order_book['timestamp'],
                                       list(map(lambda o: (o[0], round(o[1] * o[0], 2)), raw_order_book['asks'])),
                                       list(map(lambda o: (o[0], round(o[1] * o[0], 2)), raw_order_book['bids'])),
                                 )
                
                await queue.put(order_book)
                if return_results:
                    order_books.append(copy(order_book))
            except Exception as e:
                print(e)
                # await exchange.close()

        return order_books

    async def watch_trades(queue: asyncio.Queue, symbols, tick_limit=10000, return_results=False):
        trades = []
        i = 0
        while True:
            i += 1
            # print(i)
            if i > tick_limit:
                break
            try:
                current_trades_raw = await exchange.watch_trades_for_symbols(symbols)
                current_trades = list(map(lambda t: Trade(
                    t['symbol'].split(':')[0],
                    t['timestamp'],
                    round(t['amount'] * t['price'], 2),
                    t['price'],
                    t['side'] == 'buy'
                ), current_trades_raw))

                for trade in current_trades:
                    await queue.put(trade)
                if return_results:
                    trades = [* trades, *current_trades]
                
            except Exception as e:
                print(e)
                await exchange.close()

        return trades


    async def trade_loop(tick_num: int, min_save_step=1000):
        trades_queue = asyncio.Queue()
        async def get_trades():
            await watch_trades(trades_queue, symbols, tick_num)

        async def save_trades():
            total_got = 0
            while True:
                await asyncio.sleep(min_save_step / 10)
                batch_size = trades_queue.qsize()
                print(batch_size)
                if batch_size >= min_save_step:
                    batch: List[Trade] = []
                    while not trades_queue.empty():
                        batch.append(await trades_queue.get())
                    # This should return close to 0
                    print(trades_queue.qsize())
                    
                    write_trades_to_csv(f'./data/trades/{"-".join(symbolQuotes)}_trades_{total_got + 1}_{total_got + batch_size}.csv', batch)
                    total_got += batch_size

                if total_got >= tick_num:
                    print("Finished saving trades")
                    break

        await gather(get_trades(), save_trades())

    async def order_book_loop(symbols: List[str], tick_num: int, min_save_step=1000, depth=10):
        orderbook_queue = asyncio.Queue()
        async def get_orderbooks():
            while True:
                await watch_orderbooks(orderbook_queue, symbols, depth, tick_num)

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
            order_book_loop(run_symbols, 1_000_000, 2000, 1000),
            # trade_loop(1_000_000, 5000)
        )
    except Exception as e:
        print(e)

    await exchange.close()

run(main())

