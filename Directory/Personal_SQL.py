from sqlalchemy_utils import database_exists, create_database
import sqlalchemy as sql_al
from sqlalchemy.ext.automap import automap_base
import sqlalchemy_utils
import sqlite3

""""Personal SQL related module utilizing ORM and native sqlite support

ORM: SQLAlchemy and SQLAlchemy utils
Database creation and general database relationship configuration purpose

SQLITE: Base database format for ease of writing and storing large data
Will mainly be interfaced with using SQLAlchemy
"""


class sql_alchemy:
    @staticmethod
    def engine_creation(path, sql_type="0"):
        """Create engine for SQLAlchemy of desired database types

        Parameter:
        path = the url / file link to the sql location of the database
        sql_type = determine what sql_type to use ('0' = sqlite, '1' = mysql); default sql_type is '0'
        """

        # Short sql_types dict for selection purpose
        sql_types = {
            "0": "sqlite",
            "1": "mysql",
        }

        # sql_types selection
        sql_type = sql_types.get(sql_type)

        # path cleaning
        path = path.replace('\\', '/')

        if path.endswith(".db"):
            path = path

        elif not (path.endswith(".db")):
            path = path + '.db'

        # Parametrized path; practicing parametrizing for SQL query
        full_path = f'%s:///%s' % (sql_type, path)

        engine = sql_al.create_engine(full_path)

        return engine

    @staticmethod
    def database_exists_test(engine_path):
        if not database_exists(engine_path.url):
            create_database(engine_path.url)



# def main():
#     print('None')
#
#
# if __name__ == "__main__":
#     main()
