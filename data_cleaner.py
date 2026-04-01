import pandas as pd
from config import FILL_BLANK_NUMERIC_WITH_ZERO

class DataCleaner:
    """Normalizes dates, numeric columns, and ensures text columns stay as strings with proper truncation."""

    BLANK_TOKENS = {"", "nan", "nat", "none", "null", "<na>"}
    DATE_TIME_SUFFIX_RE = r"\s+0{1,2}:0{2}(?::0{2})?$"

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

    def _normalize_scalar(self, value):
        if pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() in self.BLANK_TOKENS else text

    def normalize_all_strings(self, df):
        """Strip whitespace and normalize empty-like tokens across the whole CSV."""
        for col in df.columns:
            df[col] = df[col].map(self._normalize_scalar)
        return df

    def _parse_date_series(self, series):
        cleaned = series.map(self._normalize_scalar)
        cleaned = cleaned.str.replace(self.DATE_TIME_SUFFIX_RE, "", regex=True)

        parsed = pd.Series(pd.NaT, index=cleaned.index, dtype="datetime64[ns]")
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y"):
            parsed = parsed.fillna(pd.to_datetime(cleaned, errors="coerce", format=fmt))

        parsed = parsed.fillna(pd.to_datetime(cleaned, errors="coerce", format="mixed"))

        excel_mask = cleaned.str.fullmatch(r"\d+(?:\.\d+)?", na=False)
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                pd.to_numeric(cleaned[excel_mask], errors="coerce"),
                errors="coerce",
                unit="D",
                origin="1899-12-30",
            )
            parsed.loc[excel_mask] = parsed.loc[excel_mask].fillna(excel_dates)

        return parsed.dt.normalize()

    def _parse_numeric_series(self, series):
        cleaned = series.map(self._normalize_scalar)
        cleaned = cleaned.str.replace(",", "", regex=False)
        cleaned = cleaned.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)
        cleaned = cleaned.str.replace(r"\s+", "", regex=True)
        valid_mask = cleaned.str.fullmatch(r"[-+]?\d+(?:\.\d+)?", na=False)
        return pd.to_numeric(cleaned.where(valid_mask), errors="coerce")

    def normalize_dates(self, df):
        """Normalize MIS_DAT, OPN_DAT, MAT_DAT to YYYY-MM-DD and fill blanks if required."""
        mis_dates = self._parse_date_series(df["MIS_DAT"]) if "MIS_DAT" in df.columns else None
        today = pd.Timestamp.today().normalize()

        for col in self.date_cols:
            if col not in df.columns:
                continue

            parsed = mis_dates.copy() if col == "MIS_DAT" and mis_dates is not None else self._parse_date_series(df[col])
            if FILL_BLANK_NUMERIC_WITH_ZERO:
                if col == "MIS_DAT":
                    parsed = parsed.fillna(today)
                elif mis_dates is not None:
                    parsed = parsed.fillna(mis_dates).fillna(today)
                else:
                    parsed = parsed.fillna(today)

            df[col] = parsed.dt.strftime("%Y-%m-%d").fillna("")
        return df

    def fill_blank_numerics(self, df):
        """Replace blank numeric columns with '0' to satisfy NOT NULL constraints."""
        if not FILL_BLANK_NUMERIC_WITH_ZERO:
            return df
        for col in list(self.int_cols) + list(self.dec_18_2) + list(self.dec_10_2):
            if col in df.columns:
                df[col] = df[col].map(self._normalize_scalar).replace("", "0")
        return df



    def enforce_text_columns(self, df):
        """
        Ensure text columns remain strings and truncate to DB limits.
        Prevents SQL miscasts like 'FALSE' in numeric-looking varchar columns.
        """
        for col in self.text_cols:
            if col in df.columns:
                df[col] = df[col].map(self._normalize_scalar)
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
                df[col] = self._parse_numeric_series(df[col]).fillna(0).round().astype(int)

        # DECIMAL
        for col in self.dec_18_2 + self.dec_10_2:
            if col in df.columns:
                df[col] = self._parse_numeric_series(df[col]).fillna(0.0).astype(float)

        # DATE columns - convert to datetime.date objects
        for col in self.date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        return df
