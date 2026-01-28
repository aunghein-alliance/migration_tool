import pandas as pd
from config import FILL_BLANK_NUMERIC_WITH_ZERO

class DataCleaner:
    """Normalizes dates, numeric columns, and ensures text columns stay as strings with proper truncation."""

    def __init__(self, date_cols, int_cols, dec_18_2, dec_10_2, text_cols=None, text_col_limits=None):
        """
        text_cols: list of textual columns that must be strings
        text_col_limits: dict of column_name -> max_length (truncate to fit DB)
        """
        self.date_cols = date_cols
        self.int_cols = int_cols
        self.dec_18_2 = dec_18_2
        self.dec_10_2 = dec_10_2
        self.text_cols = text_cols or []
        self.text_col_limits = text_col_limits or {}

    def normalize_dates(self, df):
        """Normalize MIS_DAT, OPN_DAT, MAT_DAT to YYYY-MM-DD and fill blanks if required."""
        if "MIS_DAT" in df.columns:
            s = df["MIS_DAT"].str.replace(r"\s+", "", regex=True)
            dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
            df["MIS_DAT"] = dt.dt.strftime("%Y-%m-%d")

        for col in ["OPN_DAT", "MAT_DAT", "DISB_DAT"]:
            if col in df.columns:
                s = df[col].str.replace(r"\s+0:00$", "", regex=True)
                dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
                df[col] = dt.dt.strftime("%Y-%m-%d")

        for col in self.date_cols:
            if FILL_BLANK_NUMERIC_WITH_ZERO:
                mask = df[col].isna()
                if mask.any():
                    fallback = df["MIS_DAT"] if col != "MIS_DAT" and "MIS_DAT" in df.columns else pd.Timestamp.today().strftime("%Y-%m-%d")
                    df.loc[mask, col] = fallback
            else:
                df[col] = df[col].fillna("")
        return df

    def fill_blank_numerics(self, df):
        """Replace blank numeric columns with '0' to satisfy NOT NULL constraints."""
        if not FILL_BLANK_NUMERIC_WITH_ZERO:
            return df
        for col in list(self.int_cols) + list(self.dec_18_2) + list(self.dec_10_2):
            if col in df.columns:
                df[col] = df[col].map(lambda x: "0" if x == "" else x)
        return df



    def enforce_text_columns(self, df):
        """
        Ensure text columns remain strings and truncate to DB limits.
        Prevents SQL miscasts like 'FALSE' in numeric-looking varchar columns.
        """
        for col in self.text_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else "")
                # Truncate to max length if specified
                if col in self.text_col_limits:
                    max_len = self.text_col_limits[col]
                    df[col] = df[col].map(lambda x: x[:max_len] if len(x) > max_len else x)
        return df

    def convert_column_types(self, df):
        """
        Force correct Python types for numeric and date columns to avoid SQLBindParameter errors.
        Must be called after filling blanks and normalizing dates.
        """
        # INT
        for col in self.int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # DECIMAL
        for col in self.dec_18_2 + self.dec_10_2:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)

        # DATE columns - convert to datetime.date objects
        for col in self.date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        return df