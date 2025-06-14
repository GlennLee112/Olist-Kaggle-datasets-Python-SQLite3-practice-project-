import os
import sqlite3
import sqlalchemy as sql_al
from sqlalchemy import *
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from Directory.Personal_SQL import sql_alchemy as p_sql_al
import pandas as pd

# Declarative base
Base = declarative_base()


# jhr_anm table class
class JhrAnm(Base):
    # AWB list of all overnight parcel of the day
    __tablename__ = "jhr_anm"
    ANM = sql_al.Column(sql_al.String)
    DP = sql_al.Column(sql_al.String, primary_key=True)
    DP_Name = sql_al.Column(sql_al.String)
    Feishu_Tag = sql_al.Column(sql_al.String)


# overnight_awb table class
class OvernightAwb(Base):
    # AWB list of all overnight parcel of the day
    __tablename__ = "overnight_awb"
    # Index = sql_al.Column(sql_al.Integer, primary_key=True, autoincrement=True)
    AWB = sql_al.Column(sql_al.String, primary_key=True)
    Pick_Up_DP = sql_al.Column(sql_al.String)
    Arrival_Time = sql_al.Column(sql_al.Date)
    Scanning_Type_Last = sql_al.Column(sql_al.String)
    Scanning_DP_Last = sql_al.Column(sql_al.String)
    Scanning_Time_Last = sql_al.Column(sql_al.String)
    COD = sql_al.Column(sql_al.String)
    Return_Parcel = sql_al.Column(sql_al.String)
    T_category = sql_al.Column(sql_al.String)
    Data_Date = sql_al.Column(sql_al.Date)
    # ANM = relationship('ANM', foreign_keys='overnight_awb.Scanning_DP_Last')


class TempOvernightAwb(Base):
    # AWB list of all overnight parcel of the day
    __tablename__ = "temp_overnight_awb"
    # Index = sql_al.Column(sql_al.Integer, primary_key=True, autoincrement=True)
    AWB = sql_al.Column(sql_al.String, primary_key=True)
    Pick_Up_DP = sql_al.Column(sql_al.String)
    Arrival_Time = sql_al.Column(sql_al.Date)
    Scanning_Type_Last = sql_al.Column(sql_al.String)
    Scanning_DP_Last = sql_al.Column(sql_al.String)
    Scanning_Time_Last = sql_al.Column(sql_al.String)
    COD = sql_al.Column(sql_al.String)
    Return_Parcel = sql_al.Column(sql_al.String)
    T_category = sql_al.Column(sql_al.String)
    Data_Date = sql_al.Column(sql_al.Date)


# jnt_tables
def overnight_awb_read(date, engine_path):
    """Read data from sql database; parameters:

    1. Date: the date of desired data
    2. engine path: path of the engine in string format; recommend using os.path.join to build engine path

    """
    use_engine = p_sql_al.engine_creation(engine_path)
    # Create if db doesn't exist
    p_sql_al.database_exists_test(use_engine)

    # Query without anm join
    query = (
        select(
            OvernightAwb,  # Select all columns from overnight_awb
            # jhr_anm.ANM.label("ANM")  # Add ANM as a new column in results
        )
        .where(
            OvernightAwb.Data_Date == date  # Filter by date first
        )
    )

    # query with ANM join
    # query = (
    #     select(
    #         overnight_awb,  # Select all columns from overnight_awb
    #         jhr_anm.ANM.label("ANM")  # Add ANM as a new column in results
    #     )
    #     .join(
    #         jhr_anm,
    #         overnight_awb.Scanning_DP_Last == jhr_anm.DP  # Join condition
    #     )
    #     .where(
    #         overnight_awb.Data_Date == date  # Filter by date first
    #     )
    # )

    with use_engine.connect() as conn:
        result = pd.read_sql(query, conn)

    return result


def overnight_awb_update(df, engine_path, col_to_use="Data_Date"):
    """update AWB to sql database:

    Utilize sqlalchemy to establish connection with current database and write to sql database

    To avoid duplicate, test query will be done with to compare the input dataframe with the sql entries/rows to filter
    out any entries/rows with identical values; only data not present in current database will be updated

    Steps of function are as followed:
    # 1. Engine Creation
    # 2. Test table existence
    # 3. Filter and write to temporary table
    # 4. Write to database (optional if dataframe is zero)
    # 5. Drop temporary table

    reference use in making of this function:

    1. https://stackoverflow.com/questions/60523645/how-to-avoid-duplicates-on-copying-data-from-python-pandas-dataframe-to-sql-data
    2. https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy

    Parameters are as followed:
    1. df - dataframe of table to be writen
    2. engine - the sqlalchemy engine to establish to the target database
    3. col_to_use - which column of the 'df' to be used; get unique date to remove

    """
    # 1. Engine creation
    engine = p_sql_al.engine_creation(engine_path)
    # Create if db doesn't exist
    p_sql_al.database_exists_test(engine)

    # 2. Test table exist or not
    # unique value from column; remove date that is same as in df (implementation, optional)

    test_result = sql_al.inspect(engine).has_table("overnight_awb") # Test table exist; return TRUE or FALSE
    print(test_result)
    with engine.begin() as conn:
        if not test_result:
            df.to_sql("overnight_awb", con=conn, if_exists="append", index=False)
            return None

    # 3. Write to temporary table
    # Create and hold data to temporary table for
        try:
            df.to_sql("temp_overnight_awb", con=conn, if_exists="replace", index=False)
            print('data to temporary overnight table complete')
        except ValueError:
            print('sql update failed')

        query_old_and_new_overnight = ((select
                                        (TempOvernightAwb)
                                        )
                                       .except_(select(OvernightAwb))
                                       )  # filtering condition based on current and new data

        # 4. Write to database (optional if dataframe is zero)
    with engine.begin() as conn:
        final_out = pd.read_sql(query_old_and_new_overnight, con=conn)
        print('SUCCESS: querying old and new data is complete')
        if len(final_out) > 0:
            final_out.to_sql('overnight_awb',
                             con=conn,
                             if_exists="append",
                             index=False,
                             method="multi",
                             chunksize=1000)
            print('SUCCESS: Final df appended to sql database')
        else:
            print('No new records to append')

    # 5. Drop temporary sql table
    # Drop temporary table inside database after all steps has been completed
    # https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy
    TempOvernightAwb.__table__.drop(engine)

    temp_test_result = sql_al.inspect(engine).has_table("temp_overnight_awb")
    if temp_test_result:
        print('FAILED: temp table has not been deleted')
    else:
        print('SUCCESS: temp table has been deleted')
