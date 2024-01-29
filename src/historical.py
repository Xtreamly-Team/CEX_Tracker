import asyncio
import gc
from typing import List
import ccxt.pro as ccxt

from asyncio import run, gather, sleep
from pprint import pprint

from models import OHLCVC, Orderbook, Trade
from utils import send_to_db_sqs, write_ohlcvs_to_csv, write_orderbooks_to_csv, write_orderbooks_to_csv_with_panda, write_trades_to_csv

from copy import copy, deepcopy
from dotenv import dotenv_values


config = dotenv_values('./src/.env')
db_queue_url = config['SQS_URL']
trade_db_collection = config['TRADE_DB_COLLECTION']
orderbook_db_collection = config['ORDERBOOK_DB_COLLECTION']

async def main():

    spot_exchange = ccxt.binance({
        'apiKey': config['BINANCE_API_KEY_HISTORIC'],
        'secret': config['BINANCE_API_SECRET_HISTORIC'],
        'options': {
            'defaultType': 'spot',
            'tradesLimit': 10000,
        },
    })

    future_exchange = ccxt.binance({
        'apiKey': config['BINANCE_API_KEY_HISTORIC'],
        'secret': config['BINANCE_API_SECRET_HISTORIC'],
        'options': {
            'defaultType': 'future',
            'tradesLimit': 10000,
        },
    })


    eth = 'ETH/USDT'
    btc = 'BTC/USDT'

    def format_symbol_quotes(symbols):
     return [x.split('/')[0].replace('/','-') for x in symbols]

    async def get_time():
        return await spot_exchange.fetch_time();

    async def get_formatted_time():
        return spot_exchange.iso8601(spot_exchange.milliseconds())

    async def get_trades(symbol: str, since: int, until: int, step: int=1000, market='spot'):
        last_trade_timestamp = since
        all_trades = []
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange

        while last_trade_timestamp < until:
            each_step = step
            raw_trades = await exchange.fetch_trades(symbol, last_trade_timestamp, each_step, params={'until': until})
            if len(raw_trades):
                last_trade_timestamp = raw_trades[len(raw_trades) - 1]['timestamp'] + 1
                all_trades += raw_trades
            else:
                break

        all_trades = list(map(lambda t: Trade(
            symbol,
            t['timestamp'],
            t['amount'],
            t['price'],
            t['side'] == 'buy',
            market
        ), all_trades))
        return all_trades

    # def get_trades(symbol: str, trades_raw: List, market='spot'):
    #     all_trades = list(map(lambda t: Trade(
    #         symbol,
    #         t['timestamp'],
    #         t['amount'],
    #         t['price'],
    #         t['side'] == 'buy',
    #         market
    #     ), trades_raw))
    #     return all_trades


    async def watch_trades(queue: asyncio.Queue, symbols, market='spot', tick_limit=10000, return_results=False):
        trades = []
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange
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
            await watch_trades(trades_queue, symbols, market, tick_num)

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
                    print('Trade Batch size to write', len(batch))
                    
                    send_to_db_sqs(db_queue_url, trade_db_collection, batch)
                    # write_trades_to_csv(f'./data/trades/{"-".join(get_symbol_quotes(symbols))}_trades_{total_got + 1}_{total_got + batch_size}.csv', batch)
                    total_got += batch_size

                if total_got >= tick_num:
                    print("Finished saving trades")
                    break

        await gather(get_trades(), save_trades())


    try:
        run_symbols = [eth, btc]
        current_time = await get_time()
        start = current_time - 3 * 86400 * 1000
        end = start + 1 * 60 * 1000
        print(current_time)

        trades = await get_trades(eth, start, end)
        pprint(len(trades))
        send_to_db_sqs(db_queue_url, trade_db_collection, trades)

        # await gather(
        #     trade_loop(run_symbols, 'spot', 1_000_000_000, 10),
        # )

    except Exception as e:
        print(e)

    finally:
        await spot_exchange.close()
        await future_exchange.close()

run(main())
