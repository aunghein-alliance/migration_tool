import pandas as pd

class SQLInserter:
    """Handles batch inserts."""

    def __init__(self, conn, schema):
        self.conn = conn
        self.schema = schema

    def insert_rows(self, table_name, columns, df, batch_size=10000):
        cursor = self.conn.cursor()
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {self.schema}.[{table_name}] ({col_list}) VALUES ({placeholders});"
        cursor.fast_executemany = True
        total = len(df)
        start = 0

        while start < total:
            end = min(start + batch_size, total)
            chunk = df.iloc[start:end]

            # ✅ Preserve real Python types
            records = chunk.where(pd.notna(chunk), None).values.tolist()

            cursor.executemany(sql, records)
            print(f"Inserted rows {start}..{end - 1}")
            start = end


        self.conn.commit()