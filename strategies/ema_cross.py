from backtesting import Strategy
from backtesting.lib import crossover
import talib
import numpy as np


def rolling_min(a, window):
    return np.array([max(a[max(0, j-window):j+1]) for j in range(len(a))])


def rolling_max(a, window):
    return np.array([min(a[max(0, j-window):j+1]) for j in range(len(a))])


class EmaCross(Strategy):
    def init(self):
        price = self.data.Close
        self.sma200 = self.I(talib.SMA, price, 200)
        self.ema12 = self.I(talib.EMA, price, 12)
        self.ema26 = self.I(talib.EMA, price, 26)
        self.atr22 = self.I(talib.ATR, self.data.High, self.data.Low, self.data.Close, 22)
        self.chandelier_exit_long = self.I(rolling_max, price, 22) - 3 * self.atr22
        self.chandelier_exit_short = self.I(rolling_min, price, 22) + 3 * self.atr22

    def next(self):
        if self.data.Close[-1] > self.sma200 and crossover(self.ema12, self.ema26):
            self.buy()
        # elif self.data.Close[-1] < self.sma200 and crossover(self.ema26, self.ema12):
        #     self.sell()
        for trade in self.trades:
            if trade.is_long:
                trade.sl = self.chandelier_exit_long
            # else:
            #     trade.sl = self.chandelier_exit_short
