import os
import shutil
from dotenv import load_dotenv
from alpaca_trade_api.rest import REST, TimeFrame
import pandas as pd
import time

import yfinance as yf
import matplotlib.pyplot as plt


load_dotenv()

API_KEY = os.getenv("ALPACA_API_KEY")
API_SECRET = os.getenv("ALPACA_SECRET_KEY")
BASE_URL = os.getenv("ALPACA_BASE_URL")

api = REST(API_KEY, API_SECRET, BASE_URL)

print("API Key loaded:", API_KEY[:5] + "..." if API_KEY else "Not loaded")

symbol = "AAPL"
start_cash = 1000
position = 0
cash = start_cash
buy_price = None

data = yf.download(symbol, start="2024-01-01", end="2025-01-01", interval="1d", auto_adjust=True)
data['Signal'] = 0

for i in range(1, len(data)):
    price = data['Close'].iloc[i].item()
    yesterday_price = data['Close'].iloc[i - 1].item()

    if position == 0:
        drop = (price - yesterday_price) / yesterday_price
        if drop < -0.02:
            position = cash // price
            buy_price = price
            cash -= position * price
            data.at[data.index[i], 'Signal'] = 1

    elif position > 0:
        gain = (price - buy_price) / buy_price
        if gain > 0.03:
            cash += position * price
            position = 0
            buy_price = None
            data.at[data.index[i], 'Signal'] = -1

# Calculate final value
final_value = cash + position * data['Close'].iloc[-1].item()
print(f"Start: ${start_cash:.2f}, End: ${final_value:.2f}, Profit: ${final_value - start_cash:.2f}")


start_price = data['Close'].iloc[0].item()
end_price = data['Close'].iloc[-1].item()

# Calculate buy-and-hold baseline
buy_and_hold_value = start_cash * (end_price / start_price)
print(f"Buy and Hold Returns: ${buy_and_hold_value:.2f}, Profit: ${buy_and_hold_value - start_cash:.2f}")

# Plot with signals
plt.figure(figsize=(12,6))
plt.plot(data['Close'], label='Close Price')
plt.plot(data[data['Signal'] == 1].index, data[data['Signal'] == 1]['Close'], '^', color='g', label='Buy', markersize=10)
plt.plot(data[data['Signal'] == -1].index, data[data['Signal'] == -1]['Close'], 'v', color='r', label='Sell', markersize=10)

plt.plot(data.index, start_cash * (data['Close'] / data['Close'].iloc[0]), label='Buy and Hold', linestyle='--', color='blue')
plt.title(f"{symbol} Backtest: ${start_cash} -> ${final_value:.2f}")
plt.legend()
#plt.show()
plt.savefig("backtest_plot.png")

if shutil.which("xdg-open"):
    os.system("xdg-open backtest_plot.png")
else:
    print("Plot saved as backtest_plot.png. Please open it manually.")
