from scraper import BitstampFetcher
from scraper import Scraper
from candle_df_utils import resample
from backtesting import Backtest
from strategies.ema_cross import EmaCross

fetcher = BitstampFetcher()
scraper = Scraper(fetcher, "btcusd", "1m")
scraper.scrape_all()
df = scraper.get_candles_df()
df = resample(df, '4h')

bt = Backtest(df, EmaCross, commission=.002,
              exclusive_orders=True,
              cash=1000000)
stats = bt.run()
bt.plot()
