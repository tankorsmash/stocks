import sqlite3

import argh
import arrow
import polygon
import business_calendar

import pandas as pd

import auth


_POLYGON_CLIENT = None
def create_client():
    global _POLYGON_CLIENT
    if _POLYGON_CLIENT: return _POLYGON_CLIENT

    _POLYGON_CLIENT = polygon.RESTClient(auth.POLYGON_API_KEY)
    return _POLYGON_CLIENT


def create_connection(db_file="tickers.db"):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print("Exception", e)
        if conn:
            conn.close()


CREATE_TABLE_SQL = """
create table %s (
  symbol text not null,
  open float, close float, high float, low float,
  volume float, vw float,
  date datetime,
  updated_at datetime,
  primary key (date, symbol)
)
"""


INSERT_TICKER_ROW_SQL = """
INSERT INTO tickers (
    symbol, open, close, high, low, volume, vw, date, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
)

ON CONFLICT(date, symbol) DO UPDATE
SET symbol=?, open=?, close=?, high=?, low=?, volume=?, vw=?, date=?, updated_at=?
"""

def add_row(conn, ticker_data):
   ticker = ticker_data["T"]
   volume = ticker_data["v"]
   vw = ticker_data.get("vw", ticker_data["o"])
   open_ = ticker_data["o"]
   close = ticker_data["c"]
   high = ticker_data["h"]
   low = ticker_data["l"]
   date = ticker_data["t"]

   import time
   updated_at = time.time()

   values = (
       ticker, open_, close, high, low, volume, vw, date, updated_at
   )
   conn.execute(INSERT_TICKER_ROW_SQL, values+values)
   # conn.commit() #TODO should this always be done?

@argh.aliases("download")
def download_days_of_market_data(days=14):
    today = arrow.get()
    two_weeks_ago = today.shift(days=-days)

    client = create_client()

    cal = business_calendar.Calendar()

    valid_dates = list(map(lambda date: date.format("YYYY-MM-DD"), cal.range(two_weeks_ago, today)))

    all_market_data = []
    for date in valid_dates:
        print("starting to download for date:", date)
        daily_market_data = client.stocks_equities_grouped_daily(locale="US", market="STOCKS", date=date)
        print("   done download for date:", date)
        all_market_data.append(daily_market_data.results)

    try:
        print("starting insert into db")
        conn = create_connection()
        i = 0
        for day in all_market_data:
            for row in day:
                add_row(conn, row)
            print("done", i)
            i += 1
        print("committing")
        conn.commit()
    except Exception as e:
        print("Got an error while inserting into db:", e)

    print("Done all")
    # print("returning all market data, raw")
    # return all_market_data

def is_consolidating(price_data, pct=2):
    last_two_weeks = price_data.iloc[-10:]

    max_close = last_two_weeks["close"].max()
    min_close = last_two_weeks["close"].min()

    thresh = 1 - (pct / 100)
    if min_close > max_close * thresh:
        return True

    return False

DISTINCT_SYMBOLS_SQL = """
    SELECT distinct symbol from tickers;
"""

@argh.aliases("parse")
def parse_db_for_symbol(symbol):
    conn = create_connection()

    old_row_fact = conn.row_factory
    conn.row_factory = None
    print("fetching all symbols")
    symbols = [s[0] for s in conn.execute(DISTINCT_SYMBOLS_SQL).fetchall()]
    print("done fetching all symbols")
    conn.row_factory = old_row_fact

    # rows = conn.execute("""
    #     SELECT * from tickers where symbol = ? order by `date`;
    # """, (symbol,))

    # symbols = ["AAPL", "MSFT", "TSLA", "SRNE", "IWM"]

    import time
    start = time.time()
    for symbol in symbols[:100]:
        print(f"fetching all rows for symbol {symbol} from db into df")
        df = pd.read_sql("Select * from tickers where symbol = '%s' order by `date`" % symbol, conn, index_col="date", parse_dates={"date": "ms", "updated_at":"s"})
        if is_consolidating(df):
            print(symbol, "is consolidating at", df.iloc[-1])
        print(f"done fetching all rows for symbol {symbol} from db into df")
    end = time.time()
    print("took", end-start);
    import ipdb; ipdb.set_trace() #TODO

def create_database(db_file="tickers.db", table_name="tickers"):
    conn = create_connection(db_file)
    conn.execute(CREATE_TABLE_SQL % table_name)
    conn.commit()



parser = argh.ArghParser()
parser.add_commands([create_database, download_days_of_market_data, parse_db_for_symbol])

if __name__ == '__main__':
    parser.dispatch()
