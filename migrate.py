import sqlite3

db_path = r"data/open_interest.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

columns_to_add = [
    ("taker_buy_volume", "FLOAT"),
    ("taker_sell_volume", "FLOAT"),
    ("cvd", "FLOAT")
]

for col_name, col_type in columns_to_add:
    try:
        cursor.execute(f"ALTER TABLE oi_funding ADD COLUMN {col_name} {col_type}")
        print(f"Added column {col_name}")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print(f"Column {col_name} already exists")
        else:
            print(f"Error adding {col_name}: {e}")

conn.commit()
conn.close()
