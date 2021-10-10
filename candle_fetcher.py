import requests
import datetime
from datetime import timezone
import pandas as pd

INTERVAL_TO_SECONDS = {
    "1m": 1 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 1 * 60 * 60,
    "2h": 2 * 60 * 60,
    "4h": 4 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 1 * 24 * 60 * 60,
}

INTERVAL_TO_DELTA = {
    "1m": datetime.timedelta(minutes=1),
    "5m": datetime.timedelta(minutes=5),
    "15m": datetime.timedelta(minutes=15),
    "30m": datetime.timedelta(minutes=30),
    "1h": datetime.timedelta(hours=1),
    "2h": datetime.timedelta(hours=2),
    "4h": datetime.timedelta(hours=4),
    "12h": datetime.timedelta(hours=12),
    "1d": datetime.timedelta(days=1),
}


class BinanceFetcher:
    def __init__(self, client):
        self.client = client

    def fetch_candles(
        self,
        symbol,
        interval,
        datetime_start=None,
        datetime_end=datetime.datetime.now(tz=timezone.utc),
        num_candles=None,
    ):
        if num_candles is None:
            num_candles = self.max_num_candles()

        if datetime_start is None:
            datetime_start = datetime_end - num_candles * INTERVAL_TO_DELTA[interval]

        start_unix = int(datetime_start.timestamp()) * 1000
        end_unix = int(datetime_end.timestamp()) * 1000

        candles = self.client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_unix,
            end_str=end_unix,
            limit=num_candles,
        )
        df = pd.DataFrame(
            candles,
            columns=[
                "Datetime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "CloseTime",
                "QuoteAssetVolume",
                "NumberOfTrades",
                "TakerBuyBaseVol",
                "TakerBuyQuoteVol",
                "Ignore",
            ],
        )
        df.drop(
            [
                "CloseTime",
                "QuoteAssetVolume",
                "NumberOfTrades",
                "TakerBuyBaseVol",
                "TakerBuyQuoteVol",
                "Ignore",
            ],
            axis=1,
            inplace=True,
        )
        df["Datetime"] = pd.to_datetime(df["Datetime"], unit="ms")
        df["Open"] = pd.to_numeric(df["Open"])
        df["High"] = pd.to_numeric(df["High"])
        df["Low"] = pd.to_numeric(df["Low"])
        df["Close"] = pd.to_numeric(df["Close"])
        df["Volume"] = pd.to_numeric(df["Volume"])
        return df

    def max_num_candles(self):
        return 500

    def name(self):
        return "Binance"


class BitstampFetcher:
    BASE_URI = "https://www.bitstamp.net/api/v2/ohlc/"

    def fetch_candles(
        self,
        symbol,
        interval,
        datetime_start=None,
        datetime_end=datetime.datetime.now(tz=timezone.utc),
        num_candles=None,
    ):
        url = self.BASE_URI + symbol
        url += "/?step=" + str(INTERVAL_TO_SECONDS[interval])

        start_unix = None
        if datetime_start is not None:
            start_unix = int(datetime_start.timestamp())
            url += "&start=" + str(start_unix)

        end_unix = None
        if datetime_end is not None:
            end_unix = int(datetime_end.timestamp())
            url += "&end=" + str(end_unix)

        if num_candles is None:
            num_candles = self.max_num_candles()
        url += "&limit=" + str(num_candles)

        response = requests.get(url)
        print(url)
        print(response)
        candles = []
        for item in response.json()["data"]["ohlc"]:
            ts = int(item["timestamp"])
            keep = True
            if start_unix is not None and ts <= start_unix:
                keep = False
            if end_unix is not None and ts >= end_unix:
                keep = False
            if keep:
                candle = {
                    "High": float(item["high"]),
                    "Datetime": datetime.datetime.fromtimestamp(
                        int(item["timestamp"]), tz=timezone.utc
                    ),
                    "Volume": float(
                        item["volume"]
                    ),  # the volume of BTC in the BTC/USD Pair
                    "Low": float(item["low"]),
                    "Close": float(item["close"]),
                    "Open": float(item["open"]),
                }
                candles.append(candle)
        return candles

    def max_num_candles(self):
        return 1000

    def name(self):
        return "Bitstamp"
