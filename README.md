# ETL-Project-Largest-Banks-by-Market-Capitalization
This project implements an ETL pipeline in Python to extract, transform, and load data about the world’s largest banks by market capitalization.  The data is extracted from an archived Wikipedia page, transformed by applying exchange rates, and then stored both in a CSV file and a SQLite database.

---

## Features

- **Logging**
Tracks progress and writes timestamps to code_log.txt.
- **Extraction**
Scrapes the “By market capitalization” table from Wikipedia into a Pandas DataFrame.
- **Transformation**
Converts market capitalization values from USD to GBP, EUR, and INR using an external exchange rate CSV.
- **Load to CSV**
Saves the transformed data into a CSV file (Largest_banks_data.csv).
- **Load to SQLite Database**
Stores the data in an SQLite database (Banks.db) in a table called Largest_banks.
- **Run Queries**
Supports executing SQL queries on the SQLite database.

---

## Project Structure

- `banks_project.py`- Main ETL script
- `Largest_banks_data.csv `- Output CSV (generated)
- `Banks.db`- SQLite database (generated)
- `code_log.txt `- Log file (generated)

---

##Technologies & Libraries

Python 3.x

`requests` – for fetching web content
`BeautifulSoup` – for parsing HTML tables
`andas` – for data manipulation
`numpy` – for numeric operations
`sqlite3` – for storing data in a database

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/banks_etl_project.git
cd banks_etl_project
```

### 2.Install dependencies:
```bash
pip install requests beautifulsoup4 pandas numpy
```

### 3.Run the ETL script
```bash
python banks_project.py
```

### 4.Check outputs:

`Largest_banks_data.csv` – CSV file with transformed data.
`Banks.db` – SQLite database containing the table Largest_banks.
`code_log.txt` – Log file with execution timestamps.
