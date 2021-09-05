# HOW TO USE:
#
# fetcher = BitstampFetcher()
# scraper = Scraper(fetcher, "btcusd", "1m")
# while scraper.scrape_backward():
#     pass
# while scraper.scrape_forward():
#     pass

import sqlite3
import requests
import datetime
from datetime import timezone
from dateutil import parser


TIMEFRAME_TO_DELTA = {"1m": datetime.timedelta(0, 60)}
TIMEFRAME_TO_SECONDS = {"1m": 60}


class BitstampFetcher:
    BASE_URI = "https://www.bitstamp.net/api/v2/ohlc/"

    def fetch_candles(self, symbol, timeframe, datetime_start, datetime_end):
        start_unix = int(datetime_start.timestamp())
        end_unix = int(datetime_end.timestamp())
        url = self.BASE_URI + symbol
        url += "/?step=" + str(TIMEFRAME_TO_SECONDS[timeframe])
        url += "&start=" + str(start_unix)
        url += "&end=" + str(end_unix)
        url += "&limit=" + str(self.num_candles())
        response = requests.get(url)

        candles = []
        for item in response.json()["data"]["ohlc"]:
            if int(item["timestamp"]) >= start_unix:
                candle = {
                    "High": item["high"],
                    "Datetime": datetime.datetime.fromtimestamp(
                        int(item["timestamp"]), tz=timezone.utc
                    ),
                    "Volume": item["volume"],  # the volume of BTC in the BTC/USD Pair
                    "Low": item["low"],
                    "Close": item["close"],
                    "Open": item["open"],
                }
                candles.append(candle)
        return candles

    def num_candles(self):
        return 2

    def name(self):
        return "Bitstamp"


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
        n = self.fetcher.num_candles()
        time_diff = n * TIMEFRAME_TO_DELTA[self.timeframe]
        candles = self.fetcher.fetch_candles(
            self.symbol, self.timeframe, ts - time_diff, ts
        )
        self.insert_candles(candles)
        return bool(candles)

    def scrape_forward(self):
        ts = self.newest_datetime()
        n = self.fetcher.num_candles()
        time_diff = n * TIMEFRAME_TO_DELTA[self.timeframe]
        candles = self.fetcher.fetch_candles(
            self.symbol, self.timeframe, ts, ts + time_diff
        )
        self.insert_candles(candles)
        return bool(candles)

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
