# banks_project.py
# ETL Project - Largest Banks by Market Capitalization

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


# =========================
# Task 1: Logging Function
# =========================
def log_progress(message):
    """Logs progress messages with timestamps into code_log.txt"""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


# =========================
# Task 2: Extract Function
# =========================
def extract(url, table_attribs):
    """Extracts the 'By market capitalization' table from Wikipedia into a Pandas DataFrame"""
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table = soup.find("table", {"class": "wikitable"})
    df = pd.DataFrame(columns=table_attribs)
    rows = table.find_all("tr")
    for row in rows[1:]:  # Skip header
        cols = row.find_all("td")
        if len(cols) >= 3:
            name = cols[1].a.text.strip() if cols[1].a else cols[1].text.strip()
            mc_usd = cols[2].text.strip().replace(",", "").replace("\n", "")

            try:
                mc_usd = float(mc_usd)
            except:
                mc_usd = None

            df = pd.concat(
                [df, pd.DataFrame([{"Name": name, "MC_USD_Billion": mc_usd}])],
                ignore_index=True
            )

    return df


# =========================
# Task 3: Transform Function
# =========================
def transform(df, csv_path):
    """Transforms dataframe by adding GBP, EUR and INR values based on exchange rates"""
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index(exchange_df.columns[0])[exchange_df.columns[1]].to_dict()

    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]

    return df


# =========================
# Task 4: Load to CSV
# =========================
def load_to_csv(df, output_path):
    """Saves dataframe to CSV"""
    df.to_csv(output_path, index=False)


# =========================
# Task 5: Load to DB
# =========================
def load_to_db(df, sql_connection, table_name):
    """Saves dataframe to SQL database"""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


# =========================
# Task 6: Run Queries
# =========================
def run_query(query_statement, sql_connection):
    """Runs SQL query and prints results"""
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    for row in rows:
        print(row)


# =========================
# Task 7: Main ETL Pipeline
# =========================
if __name__ == "__main__":
    # Parameters
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    table_attribs = ["Name", "MC_USD_Billion"]
    table_attribs_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
    output_csv_path = "./Largest_banks_data.csv"
    db_name = "Banks.db"
    table_name = "Largest_banks"

    log_progress("ETL Job Started")

    # Extract
    df = extract(url, table_attribs)
    log_progress(f"Data extraction complete. Extracted {df.shape[0]} rows.")

    # Transform
    df = transform(df, csv_path)
    log_progress("Data transformation complete")

    # Load to CSV
    load_to_csv(df, output_csv_path)
    log_progress("Data saved to CSV at " + output_csv_path)

    # Load to Database
    conn = sqlite3.connect(db_name)
    load_to_db(df, conn, table_name)
    log_progress("Data loaded to Database table " + table_name)

    # Run a sample query
    print("\nSample query: First 5 rows from the database table")
    run_query(f"SELECT * FROM {table_name} LIMIT 5;", conn)

    conn.close()
    log_progress("ETL Job Finished")
