import asyncio
import sys
import gc
from typing import List, no_type_check
import ccxt.pro as ccxt

from asyncio import run, gather, sleep
from pprint import pprint

from models import OHLCVC, Orderbook, Trade
from utils import send_to_db_sqs, write_ohlcvs_to_csv, write_orderbooks_to_csv, write_orderbooks_to_csv_with_panda, write_trades_to_csv

from copy import copy, deepcopy
from dotenv import dotenv_values


config = dotenv_values('./src/.env')
db_queue_url = config['SQS_URL']
trade_db_collection = config['TEST_DB_COLLECTION']
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
    eth_usdc = 'ETH/USDC'
    btc = 'BTC/USDT'

    def format_symbol_quotes(symbols):
     return [x.split('/')[0].replace('/','-') for x in symbols]

    async def get_time():
        return await spot_exchange.fetch_time();

    async def get_formatted_time():
        return spot_exchange.iso8601(spot_exchange.milliseconds())

    async def get_trades(symbol: str, since: int, until: int, api_step: int=1000, market='spot', save_step=100, api_sleep_ms=1000, db_sleep_ms = 10):
        total_number_got = 0
        last_trade_timestamp = since
        not_saved_trades = []
        this_step_trades = []
        exchange = spot_exchange
        if market == 'future':
            exchange = future_exchange

        while last_trade_timestamp < until:
            await asyncio.sleep(api_sleep_ms / 1000)
            raw_trades = await exchange.fetch_trades(symbol, last_trade_timestamp, api_step, params={'until': until})
            print(f"Got {len(raw_trades)} trades starting from {last_trade_timestamp}")
            total_number_got += len(raw_trades)
            not_saved_trades = [*list(map(lambda t: Trade(
                symbol,
                t['timestamp'],
                t['amount'],
                t['price'],
                t['side'] == 'buy',
                market
            ), raw_trades))]
            if len(not_saved_trades):
                last_trade_timestamp = raw_trades[len(raw_trades) - 1]['timestamp'] + 1
                while len(not_saved_trades):

                    batch = not_saved_trades[0:min(save_step, len(not_saved_trades))]
                    print(f"Sending {len(batch)} to sqs")
                    res = send_to_db_sqs(db_queue_url, trade_db_collection, batch)
                    if len(not_saved_trades) > save_step:
                        not_saved_trades = not_saved_trades[save_step:]
                    else:
                        not_saved_trades = []

            else:
                break

        return total_number_got

    try:
        run_symbols = [eth, eth_usdc, btc]
        current_time = await get_time()
        start = current_time - int(0.5 * 86400 * 1000)
        # end = start + 10 * 86400 * 1000
        end = current_time
        print(current_time)

        print(f"Getting from {start} to ${end}")
        total_got = await get_trades(eth, start, end, api_sleep_ms=5000, save_step=50)
        print(f"Got {total_got} from {start} to ${end}")
        print(await get_time())

    except Exception as e:
        print(e)

    finally:
        await spot_exchange.close()
        await future_exchange.close()

run(main())
