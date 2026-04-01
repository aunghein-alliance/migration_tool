import pyodbc
from config import SERVER, DATABASE, ENCRYPT, TRUST_CERT

class DatabaseConnector:
    """Handles SQL Server connection."""

    def __init__(self, server: str = SERVER, database: str = DATABASE):
        self.server = server
        self.database = database
        self.conn = None

    def _choose_driver(self):
        drivers = pyodbc.drivers()
        preferred = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",
        ]
        for driver_name in preferred:
            if driver_name in drivers:
                return driver_name
        fallback = next((d for d in drivers if "SQL Server" in d), None)
        if fallback:
            return fallback
        raise RuntimeError("No SQL Server ODBC driver found on this machine.")

    def connect(self):
        driver = self._choose_driver()
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={self.server};DATABASE={self.database};"
            "Trusted_Connection=yes;"
            f"Encrypt={ENCRYPT};"
            f"TrustServerCertificate={TRUST_CERT};"
        )
        self.conn = pyodbc.connect(conn_str, autocommit=False)
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
