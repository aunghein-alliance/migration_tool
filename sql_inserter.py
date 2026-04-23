import pandas as pd

class SQLInserter:
    """Handles batch inserts."""

    def __init__(self, conn, schema):
        self.conn = conn
        self.schema = schema

    def _clean_value(self, v):
        """Convert numpy scalars to native Python types; map blanks/NaN to None."""
        import numpy as np
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            f = float(v)
            return None if (f != f) else f
        if isinstance(v, float) and (v != v):
            return None
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    def insert_rows(self, table_name, columns, df, batch_size=1000):
        cursor = self.conn.cursor()
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {self.schema}.[{table_name}] ({col_list}) VALUES ({placeholders});"

        # ✅ NO fast_executemany — it type-infers from row 0 only and miscasts later rows
        cursor.fast_executemany = False

        total = len(df)
        start = 0

        while start < total:
            end = min(start + batch_size, total)
            chunk = df.iloc[start:end]

            records = [
                [self._clean_value(v) for v in row]
                for row in chunk.itertuples(index=False, name=None)
            ]

            cursor.executemany(sql, records)
            self.conn.commit()
            print(f"Inserted rows {start}..{end - 1}")
            start = end

        print("All rows inserted successfully.")