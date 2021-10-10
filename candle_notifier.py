from binance.websockets import BinanceSocketManager
from binance.client import Client

class BinanceCandleNotifier:

    def __init__(self, client, ticker):
        self.ticker = ticker
        self.prev_1m_end = 0
        socket_name = self.bm = BinanceSocketManager(client)
        self.bm.start_kline_socket(
            symbol=ticker,
            callback=self.cb,
            interval=Client.KLINE_INTERVAL_1MINUTE)
        self.bm.start()
        self.listeners = {
            '1m': [],
            '5m': [],
            '15m': [],
            '1h': [],
            '4h': [],
            '1d': []
        }

    def add_new_candle_listener(self, interval, listener):
        self.listeners[interval].append(listener)

    def cb(self, data):
        kline = data['k']
        candle_start = kline['t']
        candle_end = kline['T'] + 1
        if candle_end > self.prev_1m_end:
            self.prev_1m_end = candle_end
            self.notify_new_candle('1m', data)
            if candle_start % (5 * 60 * 1000) == 0:
                self.notify_new_candle('5m', data)
            if candle_start % (15 * 60 * 1000) == 0:
                self.notify_new_candle('15m', data)
            if candle_start % (60 * 60 * 1000) == 0:
                self.notify_new_candle('1h', data)
            if candle_start % (240 * 60 * 1000) == 0:
                self.notify_new_candle('4h', data)
            if candle_start % (1440 * 60 * 1000) == 0:
                self.notify_new_candle('1d', data)

    def notify_new_candle(self, interval, data):
        for listener in self.listeners[interval]:
            listener.on_new_candle(data, self.ticker, interval)
