import sqlite3
import os
import pandas as pd
import Directory.File_Manager as p_fm

base:str = os.path.dirname(__file__)
sqlite_path:str = os.path.join(base, "Dataset", "olist.sqlite")

connection = sqlite3.connect(sqlite_path)

# data from: https://www.kaggle.com/datasets/terencicp/e-commerce-dataset-by-olist-as-an-sqlite-database

# 0. print tables inside sqlite database

# 1. query
with connection as conn:
    cursor = conn.cursor()

    # 0. print tables inside sqlite database
    base_query = "SELECT name FROM sqlite_master WHERE type='table'"

    cursor.execute(base_query)
    results = cursor.fetchall()
    print("Raw Query:", base_query)
    print("Results:", results)

    # 1. get sellers table inside sql database
    # Include the following columns:
    # 1) s.seller_id, 2) s.seller_zip_code_prefix, 3) s.seller_city, 4) s.seller_state, 5) g.geolocation_lat,
    # 6) g.geolocation_lng, 7)g.geolocation_city

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

    df_seller = pd.read_sql_query(seller_query, con=conn)

    # 2. get orders table inside sqlite database
    orders_query = "SELECT * FROM orders"

    df_order = pd.read_sql_query(orders_query, con=conn)

    # 3. get completed sales by seller

    # sale_by_seller = ("SELECT"
    #                         "s.seller_id,"
    #                         "COUNT(DISTINCT o.order_id) AS delivered_order_count"
    #                   "FROM"
    #                         " sellers s"
    #                   "LEFT JOIN order_items oi ON s.seller_id = oi.seller_id"
    #                   "LEFT JOIN orders o ON oi.order_id = o.order_id"
    #                   "WHERE"
    #                         "o.order_delivered_customer_date IS NOT NULL"
    #                   "GROUP BY"
    #                         "s.seller_id"
    #                   "ORDER BY"
    #                     "delivered_order_count DESC;")

    sale_by_seller_query = """SELECT
                            s.seller_id,
                            s.seller_city,
                            s.seller_zip_code_prefix,
                            COUNT(DISTINCT o.order_id) AS delivered_order_count
                        FROM
                            sellers s
                        JOIN order_items oi ON s.seller_id = oi.seller_id
                        JOIN orders o ON oi.order_id = o.order_id
                        WHERE
                            o.order_delivered_customer_date IS NOT NULL
                        GROUP BY
                            s.seller_id
                        ORDER BY
                            delivered_order_count DESC;
                        """

    # explanation:
        # 1. Start from sellers: to ensure all sellers are included, even those without any sales (can change to INNER JOIN if you only want active sellers).
        #
        # 2. Join to order_items: to link sellers to their order IDs.
        #
        # 3. Join to orders: to access the order_delivered_customer_date column.
        #
        # 4. Filter WHERE order_delivered_customer_date IS NOT NULL: to count only delivered orders.
        #
        # 5. Use COUNT(DISTINCT o.order_id): in case a seller is associated with multiple items in the same order, to avoid double-counting.

    sale_by_seller_debug = """SELECT * 
                            FROM sellers s
                            JOIN order_items oi ON s.seller_id = oi.seller_id
                            JOIN orders o ON oi.order_id = o.order_id
                            LIMIT ?;"""


    df_sales_by_seller = pd.read_sql_query(sale_by_seller_query, con=conn)
    df_sales_by_seller_debug = pd.read_sql_query(sale_by_seller_debug, con=conn, params=[15])

    # 4. Sales by seller and years
    # SQLITE3 doesn't support year func for date value, find alternative to this solution
    # sale_by_seller_and_date_query = """SELECT
    #                         s.seller_id,
    #                         s.seller_city,
    #                         COUNT(DISTINCT o.order_id) AS delivered_order_count
    #                     FROM
    #                         sellers s
    #                     JOIN order_items oi ON s.seller_id = oi.seller_id
    #                     JOIN orders o ON oi.order_id = o.order_id
    #                     WHERE
    #                         o.order_delivered_customer_date IS NOT NULL
    #                         AND
    #                         year(o.order_delivered_customer_date) between ? AND ?
    #                     GROUP BY
    #                         s.seller_id
    #                     ORDER BY
    #                         delivered_order_count DESC;
    #                     """
    #
    # df_sales_by_seller_and_year = pd.read_sql_query(sale_by_seller_and_date_query, con=conn, params=[2016,2017])

csv_path = os.path.join(base,"Out")

p_fm.file_dir_create(csv_path)

# df output
df_seller.to_csv(os.path.join(csv_path, "seller list.csv"), index=False)
df_order.to_csv(os.path.join(csv_path, "order list.csv"), index=False)
df_sales_by_seller.to_csv(os.path.join(csv_path, "seller sales.csv"), index=False)

print(df_seller.columns)
print(df_order.columns)

unique_seller = df_seller["seller_id"].unique()
delivered_date = df_order["order_delivered_customer_date"] #.dropna()

print(unique_seller)
print(len(unique_seller))

print(len(delivered_date))
print(df_sales_by_seller_debug)
print(df_sales_by_seller)
# print(df_sales_by_seller_and_year)

