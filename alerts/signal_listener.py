import os

class SignalListener:
    def buy_signal(self, ticker, signal_name):
        self.say("Buy " + self.split_chars(ticker) + ", " + signal_name)

    def sell_signal(self, ticker, signal_name):
        self.say("Sell " + self.split_chars(ticker) + ", " + signal_name)

    def split_chars(self, stock_name):
        return " ".join([char for char in stock_name])

    def say(self, words):
        print(words)
        os.system('spd-say "' + words + '"')
