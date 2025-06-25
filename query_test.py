import sqlite3
import os
from functools import reduce

import pandas as pd
import Directory.File_Manager as p_fm
import Directory.Personal_Pandas as p_pd
from datetime import datetime

base:str = os.path.dirname(__file__)
out_path:str = os.path.join(base,"Out")
sqlite_path:str = os.path.join(base, "Dataset", "olist.sqlite")

connection = sqlite3.connect(sqlite_path)

# data from: https://www.kaggle.com/datasets/terencicp/e-commerce-dataset-by-olist-as-an-sqlite-database

# 0. print tables inside sqlite database


min_max_order_date = """SELECT os.order_purchase_timestamp
                        FROM orders os;"""


# 1. Query function
def max_min_sales_date(conn_use:str | sqlite3.Connection)->pd.DataFrame:
    max_min_sales_date_query =  """SELECT 
                                        MAX(os.order_purchase_timestamp) as max_min_date
                                    FROM orders os
                                    UNION ALL
                                    SELECT 
                                        MIN(os.order_purchase_timestamp)
                                    FROM orders os
                                    ;"""

    if conn_use is str:
        conn_use = sqlite3.connect(conn_use)

    elif conn_use is sqlite3.Connection:
        conn_use = conn_use


    with conn_use:
        result = pd.read_sql_query(max_min_sales_date_query, con=conn_use)

    result['max_min_date'] = pd.to_datetime(result['max_min_date'])
    result = result.sort_values(by=['max_min_date'], ignore_index=True)
    # print(result.dtypes)
    # print(result)
    result['max_min_date'] = result['max_min_date'].dt.date

    return result

def seller_query_func(conn_use:sqlite3.Connection)->pd.DataFrame:

    seller_query = """SELECT 
                        s.seller_id,
                        s.seller_city,
                        s.seller_state,
                        s.seller_zip_code_prefix,
                        g.geolocation_city
                    FROM 
                        sellers s
                    JOIN 
                        geolocation g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
                    GROUP BY
                        s.seller_id"""

    with conn_use:
        result = pd.read_sql_query(seller_query, con=conn_use)

    return result

def order_query_func(conn_use:sqlite3.Connection,
                     max_date:str,
                     min_date:str)->pd.DataFrame:

    """New order query function:

    Query orders and join with review score"""

    orders_query = """SELECT
                        o.order_id,
                        o.customer_id,
                        o.order_status,
                        o.order_purchase_timestamp,
                        o.order_approved_at,
                        o.order_delivered_carrier_date,
                        o.order_delivered_customer_date,
                        o.order_estimated_delivery_date
                    FROM orders o
                    WHERE 
                         o.order_purchase_timestamp BETWEEN ? AND ?;"""

    order_items_query = """SELECT
                            oi.order_id,
                            oi.seller_id
                        FROM 
                            order_items oi
                        JOIN orders o ON oi.order_id = o.order_id
                        WHERE
                            o.order_purchase_timestamp BETWEEN ? AND ?
                        GROUP BY
                            oi.order_id;"""

    order_reviews_query = """SELECT
                            ors.order_id,
                            ors.review_score
                        FROM 
                            order_reviews ors
                        JOIN orders o ON ors.order_id = o.order_id
                        WHERE
                            o.order_purchase_timestamp BETWEEN ? AND ?;"""

    with conn_use:
        order_result = pd.read_sql_query(orders_query, con=conn_use, params=[min_date, max_date])
        order_items_result = pd.read_sql_query(order_items_query, con=conn_use, params=[min_date, max_date])
        order_reviews_result = pd.read_sql_query(order_reviews_query, con=conn_use, params=[min_date, max_date])

    df_list_to_use = [order_result, order_items_result, order_reviews_result]

    final_df = p_pd.df_batch_merge(df_list_to_use, "order_id", "3")

    return final_df


# # 1. query
# with connection as conn:
#     cursor = conn.cursor()
#
#     # 0. print tables inside sqlite database
#     base_query = "SELECT name FROM sqlite_master WHERE type='table'"
#
#     cursor.execute(base_query)
#     results = cursor.fetchall()
#     print("Raw Query:", base_query)
#     print("Results:", results)
#     # New order query, include:
#     # 1. all columns from orders
#     # 2. order reviews (Join) - order_id, review_score
#     # 3. seller
#
#     # create a date subquery to determine the max and min date of purchase
#     df_min_max_date = pd.read_sql_query(min_max_order_date, con=conn)
#     df_min_max_date["order_purchase_timestamp"] = df_min_max_date["order_purchase_timestamp"].astype(str)
#
#     order_items_query = """SELECT
#                             oi.order_id,
#                             oi.seller_id
#                         FROM
#                             order_items oi
#                         GROUP BY
#                             oi.order_id;"""
#
#     orders_query = """SELECT
#                         o.order_id,
#                         o.customer_id,
#                         o.order_status,
#                         o.order_purchase_timestamp,
#                         o.order_approved_at,
#                         o.order_delivered_carrier_date,
#                         o.order_delivered_customer_date,
#                         o.order_estimated_delivery_date
#                     FROM orders o
#                     WHERE
#                          o.order_purchase_timestamp BETWEEN ? AND ?;"""
#
#     df_order_items = pd.read_sql_query(order_items_query, con=conn)
#
#     df_order_and_seller = df_order.merge(df_order_items, on=["order_id"])
#
#     print(df_order_and_seller)
#
#     df_order_and_seller_col = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
#                                'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date',
#                                'order_estimated_delivery_date', 'seller_id']
#
#     p_pd.csv_out(df_order_and_seller, "order and seller", out_path, column_use=df_order_and_seller_col)
#
#     # 3. get completed sales by seller
#
#     # sale_by_seller = ("SELECT"
#     #                         "s.seller_id,"
#     #                         "COUNT(DISTINCT o.order_id) AS delivered_order_count"
#     #                   "FROM"
#     #                         " sellers s"
#     #                   "LEFT JOIN order_items oi ON s.seller_id = oi.seller_id"
#     #                   "LEFT JOIN orders o ON oi.order_id = o.order_id"
#     #                   "WHERE"
#     #                         "o.order_delivered_customer_date IS NOT NULL"
#     #                   "GROUP BY"
#     #                         "s.seller_id"
#     #                   "ORDER BY"
#     #                     "delivered_order_count DESC;")
#
#     sale_by_seller_query = """SELECT
#                             s.seller_id,
#                             s.seller_city,
#                             s.seller_zip_code_prefix,
#                             COUNT(DISTINCT (o.order_id || '_' || product_id)) AS delivered_order_count
#                         FROM
#                             sellers s
#                         JOIN order_items oi ON s.seller_id = oi.seller_id
#                         JOIN orders o ON oi.order_id = o.order_id
#                         WHERE
#                             o.order_delivered_customer_date IS NOT NULL
#                         GROUP BY
#                             s.seller_id
#                         ORDER BY
#                             delivered_order_count DESC;
#                         """
#
#     # explanation:
#         # 1. Start from sellers: to ensure all sellers are included, even those without any sales (can change to INNER JOIN if you only want active sellers).
#         #
#         # 2. Join to order_items: to link sellers to their order IDs.
#         #
#         # 3. Join to orders: to access the order_delivered_customer_date column.
#         #
#         # 4. Filter WHERE order_delivered_customer_date IS NOT NULL: to count only delivered orders.
#         #
#         # 5. Use COUNT(DISTINCT o.order_id): in case a seller is associated with multiple items in the same order, to avoid double-counting.
#
#     sale_by_seller_debug = """SELECT *
#                             FROM sellers s
#                             JOIN order_items oi ON s.seller_id = oi.seller_id
#                             JOIN orders o ON oi.order_id = o.order_id
#                             LIMIT ?;"""
#
#
#     df_sales_by_seller = pd.read_sql_query(sale_by_seller_query, con=conn)
#     df_sales_by_seller_debug = pd.read_sql_query(sale_by_seller_debug, con=conn, params=[15])
#
#     # 4. Sales by seller and years
#     # SQLITE3 doesn't support year func for date value, find alternative to this solution
#     # sale_by_seller_and_date_query = """SELECT
#     #                         s.seller_id,
#     #                         s.seller_city,
#     #                         COUNT(DISTINCT o.order_id) AS delivered_order_count
#     #                     FROM
#     #                         sellers s
#     #                     JOIN order_items oi ON s.seller_id = oi.seller_id
#     #                     JOIN orders o ON oi.order_id = o.order_id
#     #                     WHERE
#     #                         o.order_delivered_customer_date IS NOT NULL
#     #                         AND
#     #                         year(o.order_delivered_customer_date) between ? AND ?
#     #                     GROUP BY
#     #                         s.seller_id
#     #                     ORDER BY
#     #                         delivered_order_count DESC;
#     #                     """
#     #
#     # df_sales_by_seller_and_year = pd.read_sql_query(sale_by_seller_and_date_query, con=conn, params=[2016,2017])

# if __name__ == "__main__":
#     csv_path = os.path.join(base,"Out")
#     data_path = os.path.join(base, "Data")
#
#     p_fm.file_dir_create(csv_path)
#
#     # df output test
#     df_seller.to_csv(os.path.join(csv_path, "seller list.csv"), index=False)
#     print(df_seller.columns)
#     df_sales_by_seller.to_csv(os.path.join(csv_path, "seller sales.csv"), index=False)
#
#
#     # df_order.to_csv(os.path.join(csv_path, "order list.csv"), index=False)
#     # print(df_order.columns)
#     # delivered_date = df_order["order_delivered_customer_date"] #.dropna()
#     # print(len(delivered_date))
#
#     unique_seller = df_seller["seller_id"].unique()
#
#
#     print(unique_seller)
#     print(len(unique_seller))
#
#
#     print(df_sales_by_seller_debug)
#     print(df_sales_by_seller)
#     # print(df_sales_by_seller_and_year)

    # data output for use by Tableau