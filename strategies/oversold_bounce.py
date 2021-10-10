from backtesting import Strategy
from backtesting.lib import crossover
import talib
import numpy as np


def rolling_min(a, window):
    return np.array([max(a[max(0, j-window):j+1]) for j in range(len(a))])


def rolling_max(a, window):
    return np.array([min(a[max(0, j-window):j+1]) for j in range(len(a))])


def chandelier_exit_long(high, low, close):
    return rolling_max(close, 22) - 3 * talib.ATR(high, low, close, 22)


class OversoldBounce(Strategy):
    def init(self):
        price = self.data.Close
        self.sma200 = self.I(talib.SMA, price, 200)
        self.rsi = self.I(talib.RSI, price, 14)
        self.chandelier_exit_long = self.I(chandelier_exit_long, self.data.High, self.data.Low, self.data.Close)

    def next(self):
        if self.data.Close[-1] > self.sma200 and crossover(self.rsi, 30):
            self.buy(sl=self.data.Close[-1]*0.9)
        for trade in self.trades:
            #  TODO this stop loss is not working out. Need a better exit signal.
            if trade.is_long:
                trade.sl = self.chandelier_exit_long
                if self.rsi > 70:
                    trade.close()
