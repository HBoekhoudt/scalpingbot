import csv
import os
from datetime import datetime

LOG_FILE = "logs/trades.csv"

def log_trade(symbol, side, entry, stop, target, qty, grade):

    write_header = not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0

    with open(LOG_FILE, "a", newline="") as f:

        writer = csv.writer(f)

        if write_header:
            writer.writerow([
                "timestamp",
                "symbol",
                "side",
                "entry",
                "stop",
                "target",
                "qty",
                "grade"
            ])

        writer.writerow([
            datetime.utcnow(),
            symbol,
            side,
            entry,
            stop,
            target,
            qty,
            grade
        ])