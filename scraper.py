# HOW TO USE:
#
# fetcher = BitstampFetcher()
# scraper = Scraper(fetcher, "btcusd", "1m")
# scraper.scrape_all()

import sqlite3
import datetime
import pandas as pd
from datetime import timezone
from dateutil import parser

from candle_fetcher import BitstampFetcher

TIMEFRAME_TO_DELTA = {"1m": datetime.timedelta(0, 60)}

class Scraper:

    def __init__(self, fetcher, symbol, timeframe):
        self.fetcher = fetcher
        self.symbol = symbol
        self.timeframe = timeframe
        self.db_table = fetcher.name() + "_" + symbol + "_" + str(timeframe)
        self.db_name = self.db_table + ".db"
        self.con = sqlite3.connect(self.db_name)
        self.cur = self.con.cursor()
        self.create_table()

    def create_table(self):
        cmd = "CREATE TABLE IF NOT EXISTS " + self.db_table + " ("
        cmd += "Datetime int PRIMARY KEY NOT NULL,"
        cmd += "Open float NOT NULL,"
        cmd += "High float NOT NULL,"
        cmd += "Low float NOT NULL,"
        cmd += "Close float NOT NULL,"
        cmd += "Volume float NOT NULL)"
        self.cur.execute(cmd)

    def oldest_datetime(self):
        cmd = "SELECT MIN(Datetime) FROM " + self.db_table
        val = self.cur.execute(cmd).fetchone()[0]
        if val is None:
            return datetime.datetime.now(tz=timezone.utc)
        else:
            return parser.parse(val)

    def newest_datetime(self):
        cmd = "SELECT MAX(Datetime) FROM " + self.db_table
        val = self.cur.execute(cmd).fetchone()[0]
        if val is None:
            return datetime.datetime.now(tz=timezone.utc)
        else:
            return parser.parse(val)

    def scrape_backward(self):
        ts = self.oldest_datetime()
        n = self.fetcher.max_num_candles()
        time_diff = n * TIMEFRAME_TO_DELTA[self.timeframe]
        candles = self.fetcher.fetch_candles(
            self.symbol, self.timeframe, ts - time_diff, ts
        )
        self.insert_candles(candles)
        return bool(candles)

    def scrape_forward(self):
        ts = self.newest_datetime()
        n = self.fetcher.max_num_candles()
        time_diff = n * TIMEFRAME_TO_DELTA[self.timeframe]
        candles = self.fetcher.fetch_candles(
            self.symbol, self.timeframe, ts, ts + time_diff
        )
        self.insert_candles(candles)
        return bool(candles)

    def scrape_all(self):
        while self.scrape_backward():
            pass
        while self.scrape_forward():
            pass

    def insert_candles(self, candles):
        cmd = "INSERT OR IGNORE INTO " + self.db_table
        cmd += " (Datetime, Open, High, Low, Close, Volume)"
        cmd += " VALUES (?, ?, ?, ?, ?, ?)"
        for candle in candles:
            entry = (
                candle["Datetime"],
                candle["Open"],
                candle["High"],
                candle["Low"],
                candle["Close"],
                candle["Volume"],
            )
            self.cur.execute(cmd, entry)
        self.con.commit()
    
    def get_candles_df(self):
        df = pd.read_sql_query("SELECT * from " + self.db_table + " ORDER BY Datetime", self.con, index_col="Datetime", parse_dates=["Datetime"])
        print(df.head())
        return df
