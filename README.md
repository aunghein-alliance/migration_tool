CSV to SQL Server Migration Tool
--------------------------------

Author: Aung Hein Thant
Version: 1.0
Date: 2025-12-09

Purpose:
--------
This tool migrates headerless CSV files into existing SQL Server tables efficiently
and safely. It supports multiple CSVs and tables, automatically detects delimiters,
normalizes date formats, validates and truncates overlong string fields, and performs
batch inserts using fast_executemany for high performance. Ideal for ETL and
analytical workflows.

Features:
---------
- Object-oriented design for maintainability and scalability
- Headerless CSV handling with predefined column mapping
- Automatic CSV delimiter detection
- Date normalization (dd-mm-yyyy, M/D/YYYY)
- Optional filling of blank numeric and date columns
- Overlong string detection and optional truncation
- Batch insertion with pyodbc.fast_executemany
- Supports multiple tables and multiple CSVs dynamically
- Optional table truncation before insert
- Detailed logging and progress reporting

Requirements:
-------------
- Python 3.9+
- Libraries: pandas, pyodbc
- SQL Server with ODBC Driver 17 or 18
- Windows Authentication for SQL Server

Installation:
-------------
1. Clone the repository:
   git clone https://github.com/username/migration_tool.git
   cd migration_tool

2. Create a virtual environment and install dependencies:
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt

Configuration:
--------------
- CSV and Table Mapping: Provide multiple CSVs and target tables via a JSON or Python dict
- Database Settings: SERVER, DATABASE, SCHEMA, ENCRYPT, TRUST_CERT
- Controls: TRUNCATE_BEFORE_INSERT, TRUNCATE_OVERLONG_STRINGS, FILL_BLANK_NUMERIC_WITH_ZERO

Usage:
------
Run the migration tool:
   python __main__.py

The tool will:
- Detect the CSV delimiter
- Read and clean CSV data
- Normalize date formats
- Validate and truncate string values
- Insert data in batches into SQL Server

Logging & Error Handling:
------------------------
- Warnings for date parsing, numeric blanks, and string truncation are printed
- SQL errors rollback transactions automatically
- Overlong strings that cannot be truncated (if truncation disabled) abort migration

Example Output:
---------------
=== Processing CSV: C:\path\file.csv → Table: MIS_LNACC ===
Using delimiter: ','
Loaded 92236 rows, 50 columns
Inserted rows 0..9999
Inserted rows 10000..19999
...
Migration complete.

Recommended Practices:
----------------------
- Ensure CSV column order matches the target table
- Use TRUNCATE_BEFORE_INSERT cautiously
- Verify SQL Server column types match expected CSV types
- Monitor progress for large CSVs

License:
--------
MIT License
