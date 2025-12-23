import os
import pandas as pd
from config import COMMON_DELIMITERS

class CSVLoader:
    """Loads all columns as pure strings without any numeric conversion."""

    def __init__(self, columns):
        self.columns = columns

    def detect_best_delimiter(self, csv_path):
        best_delim, best_rows = None, -1
        for delim in COMMON_DELIMITERS:
            try:
                df = pd.read_csv(
                    csv_path,
                    header=None,
                    names=self.columns,
                    sep=delim,
                    dtype=str,
                    na_filter=False,
                    engine="python"
                )
                if df.shape[1] == len(self.columns) and df.shape[0] > best_rows:
                    best_delim, best_rows = delim, df.shape[0]
            except Exception:
                continue
        return best_delim

    def load_csv(self, csv_path):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        # Force ALL columns to remain STRING
        converters = {col: lambda x: "" if x is None else str(x).strip() for col in self.columns}

        delim = self.detect_best_delimiter(csv_path)
        if delim is None:
            raise ValueError("Could not detect suitable delimiter.")

        print(f"Using delimiter: {repr(delim)}")

        # NO dtype parameter → removes ALL warnings
        df = pd.read_csv(
            csv_path,
            header=None,
            names=self.columns,
            sep=delim,
            converters=converters,  # <-- THIS ENSURES RAW STRING
            na_filter=False,
            engine="python"
        )

        return df
