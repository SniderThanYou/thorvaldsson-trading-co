import talib

class OversoldBounce:
    def __init__(self, ticker, interval, candle_fetcher, candle_notifier, signal_listener):
        self.ticker = ticker
        self.candle_fetcher = candle_fetcher
        self.candle_notifier = candle_notifier
        self.signal_listener = signal_listener
        candle_notifier.add_new_candle_listener(interval, self)

    def on_new_candle(self, data, ticker, interval):
        candles = self.candle_fetcher.fetch_candles(self.ticker, interval, num_candles=20)
        print(candles.Close)
        rsi = talib.RSI(candles.Close, 14)
        print(rsi)
        if rsi.iloc[-1] < 30:
            self.trigger_buy_signal()
        if rsi.iloc[-1] > 70:
            self.trigger_sell_signal()

    SIGNAL_NAME = "Oversold bounce"

    def trigger_buy_signal(self):
        self.signal_listener.buy_signal(self.ticker, self.SIGNAL_NAME)

    def trigger_sell_signal(self):
        self.signal_listener.sell_signal(self.ticker, self.SIGNAL_NAME)
