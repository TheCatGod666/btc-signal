import json
import time
import websocket
import threading
from github import Github
from datetime import datetime

# === CONFIGURATION ===
GITHUB_TOKEN = "https://thecatgod666.github.io/btc-signal/"
REPO_NAME = "TheCatGod666/btc-signal"
SIGNAL_FILE = "signal.json"

# Strategy parameters (simple moving average crossover)
FAST_MA = 9   # 9-period
SLOW_MA = 21  # 21-period

# Globals
prices = []          # stores closing prices from 1-second candles
current_price = 0
lock = threading.Lock()

# GitHub client
g = Github(GITHUB_TOKEN)
repo = g.get_repo(REPO_NAME)

def update_signal(action, price):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    content = {
        "action": action,
        "price": round(price, 2),
        "time": timestamp
    }
    try:
        contents = repo.get_contents(SIGNAL_FILE)
        repo.update_file(
            contents.path,
            f"update signal {timestamp}",
            json.dumps(content),
            contents.sha
        )
        print(f"Signal updated: {action} at {price}")
    except Exception as e:
        print(f"GitHub update failed: {e}")

def on_message(ws, message):
    global current_price, prices
    data = json.loads(message)
    # Binance miniTicker stream gives close price
    if 'c' in data:
        price = float(data['c'])
        with lock:
            current_price = price
            prices.append(price)
            # Keep only the last SLOW_MA + 5 points
            if len(prices) > SLOW_MA + 10:
                prices.pop(0)

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed, reconnecting in 5s...")
    time.sleep(5)
    start_ws()

def on_open(ws):
    print("WebSocket connected")

def start_ws():
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@miniTicker",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    # Run WebSocket in a separate thread
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

def calculate_signal():
    global prices, current_price
    last_action = "HOLD"
    while True:
        time.sleep(1)  # evaluate every second
        with lock:
            if len(prices) >= SLOW_MA:
                # Calculate simple moving averages using the last N points
                fast = sum(prices[-FAST_MA:]) / FAST_MA
                slow = sum(prices[-SLOW_MA:]) / SLOW_MA
                price = current_price
            else:
                continue

        if fast > slow and last_action != "BUY":
            action = "BUY"
        elif fast < slow and last_action != "SELL":
            action = "SELL"
        else:
            action = "HOLD"

        if action != last_action:
            update_signal(action, price)
            last_action = action

# Start the WebSocket
start_ws()

# Run the signal logic in the main thread
calculate_signal()
