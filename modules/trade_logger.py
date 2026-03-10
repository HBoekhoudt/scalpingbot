import csv
import os
from datetime import datetime

LOG_FILE = "logs/trades.csv"


def log_trade(symbol, side, qty, entry_price, exit_price, pnl):

    os.makedirs("logs", exist_ok=True)

    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, "a", newline="") as f:

        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "timestamp",
                "symbol",
                "side",
                "qty",
                "entry_price",
                "exit_price",
                "pnl"
            ])

        writer.writerow([
            datetime.utcnow().isoformat(),
            symbol,
            side,
            qty,
            entry_price,
            exit_price,
            pnl
        ])