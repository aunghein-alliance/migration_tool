import sys
from db_connector import DatabaseConnector
from csv_loader import CSVLoader
from data_cleaner import DataCleaner
from varchar_handler import VarcharHandler
from sql_inserter import SQLInserter
from config import TABLE_CONFIG, SCHEMA, TRUNCATE_OVERLONG_STRINGS

class MigrationManager:
    """Orchestrates multiple CSVs → multiple tables migration."""

    def __init__(self, table_configs=TABLE_CONFIG):
        self.table_configs = table_configs
        self.db = DatabaseConnector()

    def run(self):
        self.db.connect()
        try:
            for cfg in self.table_configs:
                print(f"\n=== Processing CSV: {cfg['csv_path']} -> Table: {cfg['table_name']} ===")
                loader = CSVLoader(cfg["columns"])
                df = loader.load_csv(cfg["csv_path"])
                print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")

                cleaner = DataCleaner(
                    cfg["date_cols"],
                    cfg["int_cols"],
                    cfg["dec_18_2"],
                    cfg["dec_10_2"],
                    text_cols=cfg.get("text_cols", [])
                )
                df = cleaner.normalize_all_strings(df)
                df = cleaner.normalize_dates(df)
                df = cleaner.fill_blank_numerics(df)
                df = cleaner.enforce_text_columns(df)
                df = cleaner.convert_column_types(df)

                cursor = self.db.conn.cursor()
                varchar_handler = VarcharHandler(cursor)
                db_limits = varchar_handler.fetch_db_varchar_limits(f"{SCHEMA}.{cfg['table_name']}")
                offenders = varchar_handler.report_overlong_values(df, db_limits)
                if offenders and TRUNCATE_OVERLONG_STRINGS:
                    df = varchar_handler.truncate_to_db_limits(df, db_limits)
                    offenders2 = varchar_handler.report_overlong_values(df, db_limits)
                    if offenders2:
                        print("Still overlong values after truncation, aborting.")
                        sys.exit(4)
                elif offenders:
                    print("Overlong values detected, truncation disabled. Aborting.")
                    sys.exit(5)

                inserter = SQLInserter(self.db.conn, SCHEMA)
                inserter.insert_rows(cfg["table_name"], cfg["columns"], df)
                print(f"Completed migration for table {cfg['table_name']}.")
        finally:
            self.db.close()
