# diagnose.py  — run this standalone: python diagnose.py
import pyodbc
import pandas as pd
from db_connector import DatabaseConnector
from csv_loader import CSVLoader
from data_cleaner import DataCleaner
from config import TABLE_CONFIG, SCHEMA

cfg = TABLE_CONFIG[0]

# 1. Connect
db = DatabaseConnector()
conn = db.connect()
cursor = conn.cursor()

# 2. Fetch actual DB columns
cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{cfg['table_name']}'
    ORDER BY ORDINAL_POSITION
""")
db_cols = cursor.fetchall()
print("=== DB COLUMNS ===")
db_col_names = set()
for row in db_cols:
    print(f"  {row[0]:30s} {row[1]:15s} len={row[2]}  nullable={row[3]}")
    db_col_names.add(row[0])

# 3. Compare with config columns
print("\n=== CONFIG COLUMNS vs DB ===")
for col in cfg["columns"]:
    status = "OK" if col in db_col_names else "!! MISSING IN DB"
    print(f"  {col:30s} {status}")

# 4. Load and clean one row
loader = CSVLoader(cfg["columns"])
df = loader.load_csv(cfg["csv_path"])
cleaner = DataCleaner(
    cfg["date_cols"], cfg["int_cols"],
    cfg["dec_18_2"], cfg["dec_10_2"],
    text_cols=cfg.get("text_cols", [])
)
df = cleaner.normalize_all_strings(df)
df = cleaner.normalize_dates(df)
df = cleaner.fill_blank_numerics(df)
df = cleaner.enforce_text_columns(df)
df = cleaner.convert_column_types(df)

# 5. Print first row types
print("\n=== FIRST ROW PYTHON TYPES ===")
row0 = df.iloc[0]
for col in cfg["columns"]:
    val = row0[col]
    print(f"  {col:30s} {type(val).__name__:15s} = {repr(val)[:60]}")

# 6. Try inserting just row 0, one column at a time
print("\n=== SINGLE-COLUMN INSERT TEST ===")
col_list = ", ".join(f"[{c}]" for c in cfg["columns"])
placeholders = ", ".join(["?"] * len(cfg["columns"]))
sql = f"INSERT INTO {SCHEMA}.[{cfg['table_name']}] ({col_list}) VALUES ({placeholders});"

row = [row0[c] for c in cfg["columns"]]
for i, (col, val) in enumerate(zip(cfg["columns"], row)):
    test = [None] * len(row)
    test[i] = val
    try:
        cursor.execute(sql, test)
        conn.rollback()
        print(f"  OK  [{i:02d}] {col:30s} = {repr(val)[:50]}")
    except Exception as e:
        print(f"  !!  [{i:02d}] {col:30s} = {repr(val)[:50]}  ERROR: {e}")
        conn.rollback()

db.close()