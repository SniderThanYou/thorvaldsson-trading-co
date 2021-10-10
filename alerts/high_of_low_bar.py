class HighOfLowBar:
    def __init__(
        self, ticker, interval, candle_fetcher, candle_notifier, signal_listener
    ):
        self.ticker = ticker
        self.interval = interval
        self.candle_fetcher = candle_fetcher
        self.candle_notifier = candle_notifier
        self.signal_listener = signal_listener
        # 1m because I want it to notify on the break, not on the bar close,
        # regardless of monitored time frame
        candle_notifier.add_new_candle_listener("1m", self)

    def on_new_candle(self, data, ticker, interval):
        candles = self.candle_fetcher.fetch_candles(
            self.ticker, self.interval, num_candles=8
        )
        for i in range(1, 7):
            if candles["Close"][i] > candles["Close"][i - 1]:
                return
        if candles["Close"][7] > candles["High"][6]:
            self.trigger_buy_signal()

    SIGNAL_NAME = "High of low bar"

    def trigger_buy_signal(self):
        self.signal_listener.buy_signal(self.ticker, self.SIGNAL_NAME)

    def trigger_sell_signal(self):
        self.signal_listener.sell_signal(self.ticker, self.SIGNAL_NAME)
