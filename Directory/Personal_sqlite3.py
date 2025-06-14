import sqlite3 as sq3
import Directory.File_Manager as FileManager


# General SQL functions
# https://www.freecodecamp.org/news/work-with-sqlite-in-python-handbook/#heading-how-to-create-an-sqlite-database
# Required:
# 1. Create database
# 2. Create table
# 3. Drop data (by parameter)

sql_path = FileManager.get_file_paths()

class DB_Manager:
    @staticmethod
    def create_db():