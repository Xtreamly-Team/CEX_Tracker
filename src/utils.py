import tailer
import io
from csv import DictWriter
from pathlib import Path
import pandas as pd
from typing import List
from models import Orderbook, OHLCVC, Trade
from dataclasses import asdict
from sys import argv
import gc

def table(values):
    first = values[0]
    keys = list(first.keys()) if isinstance(first, dict) else range(0, len(first))
    widths = [max([len(str(v[k])) for v in values]) for k in keys]
    string = ' | '.join(['{:<' + str(w) + '}' for w in widths])
    return "\n".join([string.format(*[str(v[k]) for k in keys]) for v in values])

def write_orderbooks_to_csv_with_panda(file_path, orderbooks: List[Orderbook]):

    orderbook_depth = len(orderbooks[0].asks)

    raw_df = pd.DataFrame.from_dict\
    (data=list(map(lambda x: asdict(x), orderbooks)))

    df_asks = pd.DataFrame(raw_df["asks"].to_list(), columns=[*[f'ask_{i}' for i in range(1, orderbook_depth + 1)]])
    df_bids = pd.DataFrame(raw_df["bids"].to_list(), columns=[*[f'bid_{i}' for i in range(1, orderbook_depth + 1)]])

    df_base = pd.DataFrame(raw_df, columns=['symbol', 'timestamp'])

    df_ask_columns = []
    for i in range(1, orderbook_depth + 1):
        df2 = pd.DataFrame(df_asks[f"ask_{i}"].to_list(), columns=[f'ask_{i}_price', f'ask_{i}_volume'])
        df_ask_columns.append(df2)

    # df_base = pd.concat(df_ask_columns,axis=1)

    df_bid_columns = []
    for i in range(1, orderbook_depth + 1):
        df3 = pd.DataFrame(df_bids[f"bid_{i}"].to_list(), columns=[f'bid_{i}_price', f'bid_{i}_volume'])
        df_bid_columns.append(df3)

    df_base = pd.concat([raw_df['symbol'], raw_df['timestamp'], *df_ask_columns, *df_bid_columns],axis=1)

    df_base.to_csv(file_path, header=True, index=False)

def write_orderbooks_to_csv(file_path, order_books: List[Orderbook]):
    field_names = ['symbol', 'timestamp']
    limit = len(order_books[0].asks)
    for i in range(1, limit + 1):
        field_names += [f'ask_{i}_price', f'ask_{i}_volume']
    for i in range(1, limit + 1):
        field_names += [f'bid_{i}_price', f'bid_{i}_volume']

    rows = []
    with open(file_path, 'w') as csvfile:
        writer = DictWriter(csvfile, field_names)
        writer.writeheader()
        for order_book in order_books:
            try:
                row_to_write = {
                        'symbol': order_book.symbol,
                        'timestamp': order_book.timestamp,
                    }
                for i in range(limit):
                    row_to_write = {
                        ** row_to_write,
                        ** {
                        f'ask_{i+1}_price': order_book.asks[i][0],
                        f'ask_{i+1}_volume': order_book.asks[i][1],
                        f'bid_{i+1}_price': order_book.bids[i][0],
                        f'bid_{i+1}_volume': order_book.bids[i][1]
                        }
                    }

                rows.append(row_to_write)
            except Exception as e:
                print(e)
                
        writer.writerows(rows)


def write_trades_to_csv(file_path, trades: List[Trade]):
    pd.DataFrame.from_dict\
    (data=list(map(lambda x: asdict(x), trades)))\
    .to_csv(file_path, header=True)

def write_ohlcvs_to_csv(file_path, ohlvcs: List[OHLCVC]):
    pd.DataFrame.from_dict\
    (data=list(map(lambda x: asdict(x), ohlvcs)))\
    .to_csv(file_path, header=True)


def join_rows_into_single_csv(file_paths: List[str], output_path: str):
    df = pd.concat( 
    map(pd.read_csv, file_paths), ignore_index=True)\
    .to_csv(output_path, header=True)

def gather_and_process_into_separate_csvs(file_paths: List[str], output_path: str):
    base_name = Path(output_path).stem
    print(base_name)
    df = pd.concat( 
    map(pd.read_csv, file_paths), ignore_index=True)

    symbols: List[str] = df.symbol.unique()
    print(symbols)

    dataFrameDict = {symbol: pd.DataFrame() for symbol in symbols}

    # IDEA: Use loc + filter for this
    for symbol in dataFrameDict.keys():
        dataFrameDict[symbol] = df[:][df.symbol == symbol]
        file_name = Path(output_path).with_stem(f'{base_name}_{(symbol.split("/")[0])}')
        dataFrameDict[symbol].to_csv(file_name, header=True)

def split_into_separate_csvs(file_path: str, segment_length: int):
    base_name = Path(file_path).stem
    df = pd.read_csv(file_path)
    total_rows = df.shape[0]
    segments = total_rows // length
    for i in range(segments):
        print(i, ' of ', segments)
        start = i * length
        end = min(((i+1) * length) - 1, total_rows)
        dff = df[start:end]
        dff.to_csv(Path(file_path).with_stem(f'{base_name}_{start}_{end}'), header=True)
        del dff
        print(gc.collect())

    # Last segment

def write_n_rows_csv(file_path: str, rows: int, from_end=False):
    path = Path(file_path)
    base_name = path.stem
    if from_end:
        # with open(file_path) as file:
        #     last_lines = tailer.tail(file, rows)
        # df = pd.read_csv(io.StringIO('\n'.join(last_lines)))
        df = pd.read_csv(file_path).iloc[-rows:]
    else:
        df = pd.read_csv(file_path, nrows=rows)

    save_path = path.with_stem(f'{base_name}_{rows}{"_end" if from_end else ""}')
    print(save_path)
    df.to_csv(save_path, header=True)


if __name__ == '__main__':
    join_rows_into_single_csv(argv[1:-1], argv[-1])

