import sqlite3
import os


base = os.path.dirname(__file__)
sqlite_path = os.path.join(base, "Dataset", "olist.sqlite")

print(type(base))
print(type(sqlite_path))