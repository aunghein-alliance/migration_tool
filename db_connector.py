import pyodbc
from config import SERVER, DATABASE, ENCRYPT, TRUST_CERT

class DatabaseConnector:
    """Handles SQL Server connection."""

    def __init__(self, server: str = SERVER, database: str = DATABASE):
        self.server = server
        self.database = database
        self.conn = None

    def connect(self):
        drivers = pyodbc.drivers()
        driver = "{ODBC Driver 18 for SQL Server}" if "{ODBC Driver 18 for SQL Server}" in drivers \
                 else "{ODBC Driver 17 for SQL Server}" if "{ODBC Driver 17 for SQL Server}" in drivers \
                 else next((d for d in drivers if "SQL Server" in d), "{SQL Server}")
        conn_str = (
            f"DRIVER={driver};SERVER={self.server};DATABASE={self.database};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"  # disable SSL if local
            "TrustServerCertificate=yes;"
        )
        self.conn = pyodbc.connect(conn_str, autocommit=False)
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
