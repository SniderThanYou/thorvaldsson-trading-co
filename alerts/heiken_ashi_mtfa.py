class HeikenAshiMTFA:
    def __init__(self, ticker, candle_fetcher, candle_notifier, signal_listener):
        self.ticker = ticker
        self.candle_fetcher = candle_fetcher
        self.candle_notifier = candle_notifier
        self.signal_listener = signal_listener

        self.state = "SELL"
        self.latest_heiken_ashi = {}
        self.update_heiken_ashi("15m")
        self.update_heiken_ashi("30m")
        self.update_heiken_ashi("1h")
        self.update_heiken_ashi("2h")
        self.update_heiken_ashi("4h")
        self.update_heiken_ashi("12h")
        self.update_heiken_ashi("1d")

        candle_notifier.add_new_candle_listener("15m", self)

    def update_heiken_ashi(self, interval):
        df = self.candle_fetcher.fetch_candles(self.ticker, interval, num_candles=20)
        df["HA_Close"] = 0.25 * (df["Open"] + df["High"] + df["Low"] + df["Close"])
        for i in range(df.shape[0]):
            if i > 0:
                df.loc[df.index[i], "HA_Open"] = 0.5 * (
                    df["HA_Open"][i - 1] + df["HA_Close"][i - 1]
                )
            else:
                df.loc[df.index[i], "HA_Open"] = df["Open"][i]

        o = df["HA_Open"].iloc[-1]
        c = df["HA_Close"].iloc[-1]
        if o > c:
            self.latest_heiken_ashi[interval] = "RED"
        else:
            self.latest_heiken_ashi[interval] = "GREEN"

    def update_state(self):
        self.update_heiken_ashi("15m")
        self.update_heiken_ashi("30m")
        self.update_heiken_ashi("1h")
        self.update_heiken_ashi("2h")
        self.update_heiken_ashi("4h")
        self.update_heiken_ashi("12h")
        self.update_heiken_ashi("1d")
        all_green = True
        all_red = True
        for k in self.latest_heiken_ashi:
            val = self.latest_heiken_ashi[k]
            if val == "RED":
                all_green = False
            if val == "GREEN":
                all_red = False
        if all_red:
            self.state = "RED"
        if all_green:
            self.state = "GREEN"

    def on_new_candle(self, data, ticker, interval):
        old_state = self.state
        self.update_state()
        if old_state == "RED" and self.state == "GREEN":
            self.trigger_buy_signal()
        if old_state == "GREEN" and self.state == "RED":
            self.trigger_sell_signal()

    SIGNAL_NAME = "M T F A Heikin Ashi"

    def trigger_buy_signal(self):
        self.signal_listener.buy_signal(self.ticker, self.SIGNAL_NAME)

    def trigger_sell_signal(self):
        self.signal_listener.sell_signal(self.ticker, self.SIGNAL_NAME)
