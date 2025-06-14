import sqlite3
import os

base = os.path.dirname(__file__)
sqlite_path = os.path.join(base, "Dataset", "olist.sqlite")

connection = sqlite3.connect(sqlite_path)

tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'")

with connection as conn:
    cursor = conn.cursor()

    query = "SELECT name FROM sqlite_master WHERE type='table'"

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    results = cursor.fetchall()
    print("Raw Query:", query)
    print("Results:", results)

