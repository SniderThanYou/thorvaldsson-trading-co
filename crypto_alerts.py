import configparser
from binance.client import Client
from candle_fetcher import BinanceFetcher
from candle_fetcher import BitstampFetcher
from candle_notifier import BinanceCandleNotifier
from alerts.signal_listener import SignalListener
from alerts.heiken_ashi_mtfa import HeikenAshiMTFA
from alerts.high_of_low_bar import HighOfLowBar
from alerts.oversold_bounce import OversoldBounce


# Loading keys from config file
config = configparser.ConfigParser()
config.read_file(open("secret.cfg"))
actual_api_key = config.get("BINANCE", "ACTUAL_API_KEY")
actual_secret_key = config.get("BINANCE", "ACTUAL_SECRET_KEY")

client = Client(api_key=actual_api_key, api_secret=actual_secret_key, tld="us")
candle_fetcher = BinanceFetcher(client)
candle_notifier = BinanceCandleNotifier(client, "btcusdt")
signal_listener = SignalListener()


HeikenAshiMTFA("BTCUSD", candle_fetcher, candle_notifier, signal_listener)
OversoldBounce("BTCUSD", "5m", candle_fetcher, candle_notifier, signal_listener)
HighOfLowBar("ADABTC", "1h", candle_fetcher, candle_notifier, signal_listener)
