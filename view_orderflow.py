"""Generate ETH orderflow chart from SQLite database."""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

DB_PATH = 'data/open_interest.db'
PAIR = 'ETH/USDC:USDC'
START = '2026-01-01'
END = '2026-03-01'

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(
    "SELECT timestamp, taker_buy_volume, taker_sell_volume, funding_rate "
    "FROM orderflow_1h WHERE pair = ? AND timestamp >= ? AND timestamp <= ? "
    "ORDER BY timestamp",
    conn, params=(PAIR, START, END + ' 23:59:59')
)
conn.close()

df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
df = df.set_index('timestamp')

print(f"📊 {PAIR}: {len(df)} rows | {df.index[0]} → {df.index[-1]}")

# --- Chart ---
fig, axes = plt.subplots(2, 1, figsize=(18, 9), sharex=True,
                         gridspec_kw={'height_ratios': [2, 1]})
fig.suptitle(f'{PAIR}  —  Orderflow  ({START} → {END})', fontsize=15, fontweight='bold', color='white')

# 1) Taker Volume
ax = axes[0]
ax.bar(df.index, df['taker_buy_volume'], width=0.035, color='#26a69a', alpha=0.85, label='Taker Buy')
ax.bar(df.index, -df['taker_sell_volume'], width=0.035, color='#ef5350', alpha=0.85, label='Taker Sell')
ax.axhline(0, color='#555', linewidth=0.5)
ax.set_ylabel('Volume (contracts)', color='#ccc', fontsize=10)
ax.legend(loc='upper left', fontsize=9, facecolor='#1e1e2f', edgecolor='#333', labelcolor='#ccc')
ax.set_title('Taker Buy vs Sell Volume (1h)', fontsize=11, color='#ccc')
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:,.0f}'))

# 2) Funding Rate
ax = axes[1]
colors = ['#26a69a' if v >= 0 else '#ef5350' for v in df['funding_rate']]
ax.bar(df.index, df['funding_rate'] * 100, width=0.035, color=colors, alpha=0.85)
ax.axhline(0, color='#555', linewidth=0.5)
ax.set_ylabel('Funding Rate (%)', color='#ccc', fontsize=10)
ax.set_title('Funding Rate (1h)', fontsize=11, color='#ccc')
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.4f}%'))

# Styling
for ax in axes:
    ax.set_facecolor('#1a1a2e')
    ax.tick_params(colors='#999', labelsize=8)
    ax.grid(axis='y', alpha=0.12, color='#666')
    for spine in ax.spines.values():
        spine.set_color('#333')

axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
axes[-1].xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
axes[-1].xaxis.set_minor_locator(mdates.DayLocator())

fig.patch.set_facecolor('#12121e')
fig.autofmt_xdate(rotation=30)
plt.tight_layout()

out = 'output/eth_orderflow_chart.png'
plt.savefig(out, dpi=150, facecolor=fig.get_facecolor())
print(f"💾 Saved to {out}")
plt.show()
