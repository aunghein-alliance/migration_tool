class VarcharHandler:
    """Reports and truncates overlong varchar fields."""

    def __init__(self, cursor):
        self.cursor = cursor

    def fetch_db_varchar_limits(self, table_name):
        sql = """
        SELECT c.name AS column_name, t.name AS data_type, c.max_length AS max_length_bytes
        FROM sys.columns c
        JOIN sys.types t ON t.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
          AND t.name IN ('varchar','char','nvarchar','nchar')
        """
        self.cursor.execute(sql, table_name)
        limits = {}
        for col_name, data_type, max_len in self.cursor.fetchall():
            if data_type in ("nvarchar","nchar"):
                limits[col_name] = max_len // 2
            else:
                limits[col_name] = max_len
        return limits

    def report_overlong_values(self, df, db_limits):
        from config import REPORT_TOP_N_OFFENDERS
        offenders = {}
        for col, maxlen in db_limits.items():
            if col in df.columns and isinstance(maxlen,int) and maxlen>0:
                lens = df[col].map(lambda x: len(x) if isinstance(x,str) else 0)
                over = df[lens>maxlen]
                if not over.empty:
                    examples = over[[col]].head(REPORT_TOP_N_OFFENDERS)[col].tolist()
                    offenders[col] = {"max_observed":int(lens.max()),"limit":int(maxlen),"count":int(over.shape[0]),"examples":examples}
        return offenders

    def truncate_to_db_limits(self, df, db_limits):
        for col, maxlen in db_limits.items():
            if col in df.columns and isinstance(maxlen,int) and maxlen>0:
                mask = df[col].map(lambda x: len(x) if isinstance(x,str) else 0) > maxlen
                if mask.any():
                    df.loc[mask, col] = df.loc[mask, col].map(lambda x: x[:maxlen] if isinstance(x,str) else x)
        return df
