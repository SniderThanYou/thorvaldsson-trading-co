CANDLE_AGGREGATION = {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }

def resample(df, timeframe):
    return (
        df.resample(timeframe)
        .agg(CANDLE_AGGREGATION)
        .interpolate()
    )
