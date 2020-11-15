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
    # weekly_df = df.rolling(window=5)["close"]
    # consolidating = weekly_df.apply(lambda wk: wk.min() > (wk.max() * 0.98)).dropna()
    # consolidating = consolidating[consolidating != 0]

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
    symbols = conn.execute(DISTINCT_SYMBOLS_SQL).fetchall()
    print("done fetching all symbols")
    conn.row_factory = old_row_fact

    # rows = conn.execute("""
    #     SELECT * from tickers where symbol = ? order by `date`;
    # """, (symbol,))

    print(f"fetching all rows for symbol {symbol} from db into df")
    df = pd.read_sql("Select * from tickers where symbol = '%s' order by `date`" % symbol, conn, index_col="date", parse_dates={"date": "ms", "updated_at":"s"})
    print(f"done fetching all rows for symbol {symbol} from db into df")


    # this would get a number I can filter on date column in the tickers db, so that I'd only get the last two weeks of rows. returns 90008 rows. maybe some tickers didnt trade or something, idk
    ago = arrow.get().shift(days=-14).timestamp * 1000
    ## conn.execute("select count(*) from tickers where `date` > %s" % ago)
    df = pd.read_sql(("select * from tickers where `date` > %s" % ago), conn, index_col="date", parse_dates={"date": "ms", "updated_at":"s"})
    # weekly_df = df.groupby("symbol").rolling(window=5)["close"]
    ## consolidating = weekly_df.apply(lambda wk: wk.min() > (wk.max() * 0.98)).dropna()
    ## consolidating = consolidating[consolidating != 0]

    def cons(week):
        # import ipdb; ipdb.set_trace() #TODO
        print(week)
        return week.max()
        # close, vol = week.split(" ")
        # if not vol: return None
        return (close.min() > close.max() * 0.98, vol.sum() > 1_000_000)
    qwe = df.groupby("symbol").rolling(window=5)
    import ipdb; ipdb.set_trace() #TODO
    qwe = df.groupby("symbol").rolling(window=5)[("close", "volume")].apply(cons).dropna()
    weekly_df = df.groupby("symbol").rolling(window=5)["close", "volume"]
    raw_consolidating = weekly_df.apply(lambda wk: (wk[0].min() > (wk[0].max() * 0.98)) & (wk[1].sum() > 1_000_000), raw=True).dropna()
    consolidating = raw_consolidating.loc[(raw_consolidating["close"] != 0) & (raw_consolidating["volume"] != 0)]


    # for symbol in symbols:
    #     print(f"fetching all rows for symbol {symbol} from db into df")
    #     df = pd.read_sql("Select * from tickers where symbol = '%s' order by `date`" % symbol[0], conn, index_col="date", parse_dates={"date": "ms", "updated_at":"s"})
    #     print(f"done fetching all rows for symbol {symbol} from db into df")
    import ipdb; ipdb.set_trace() #TODO

def create_database(db_file="tickers.db", table_name="tickers"):
    conn = create_connection(db_file)
    conn.execute(CREATE_TABLE_SQL % table_name)
    conn.commit()



parser = argh.ArghParser()
parser.add_commands([create_database, download_days_of_market_data, parse_db_for_symbol])

if __name__ == '__main__':
    parser.dispatch()
