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

# product_list = super()

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

    # Method 1. raw query


    # Method 2. Pandas
    df_seller = pd.read_sql_query(seller_query, con=conn)

    # New order query, include:
    # 1. all columns from orders
    # 2. order reviews (Join) - order_id, review_score
    # 3. seller

    # create a date subquery to determine the max and min date of purchase
    min_max_order_date = """SELECT os.order_purchase_timestamp
                            FROM orders os;"""

    df_min_max_date = pd.read_sql_query(min_max_order_date, con=conn)
    df_min_max_date["order_purchase_timestamp"] = df_min_max_date["order_purchase_timestamp"].astype(str)
    min_date = df_min_max_date["order_purchase_timestamp"].min()
    max_date = df_min_max_date["order_purchase_timestamp"].max()
    print(f'min date is:{min_date}, max date is: {max_date}')
    print(df_min_max_date)
    # print(datetime.strptime(min_date.iloc[0],"%Y-%m-%d %H:%M:%S"))
    # print(datetime.strptime(max_date.iloc[0],"%Y-%m-%d %H:%M:%S"))
    # print()

    orders_query = """SELECT
                        os.order_id,
                        os.customer_id,
                        os.order_status,
                        os.order_purchase_timestamp,
                        os.order_approved_at,
                        os.order_delivered_carrier_date,
                        os.order_delivered_customer_date,
                        os.order_estimated_delivery_date
                    FROM orders os
                    WHERE 
                         os.order_purchase_timestamp BETWEEN ? AND ?;"""

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

    df_order = pd.read_sql_query(orders_query, con=conn, params=[min_date,max_date])
    df_order_items = pd.read_sql_query(order_items_query, con=conn, params=[min_date,max_date])
    df_order_reviews = pd.read_sql_query(order_reviews_query, con=conn, params=[min_date,max_date])

    print(df_order)
    print(df_order_items)
    print(df_order_reviews)

    # Batch merge the above 3 dataframe into one
    def df_batch_merge(df_list: list,
                       col_use: str|list,
                       merge_type: str = "0") -> pd.DataFrame:
        """Merge multiple dataframes into one"""
        merge_type_dict = {"0": "inner",
                           "1": "left",
                           "2": "right",
                           "3": "outer"}

        merge_use = merge_type_dict[merge_type]
        print(merge_use)

        # https://stackoverflow.com/questions/44327999/how-to-merge-multiple-dataframes

        df_merged = reduce(lambda left, right: pd.merge(left, right, on=[col_use], how=merge_use), df_list)

        return df_merged

    df_list_use = [df_order, df_order_items, df_order_reviews]

    df_order_review_items = df_batch_merge(df_list_use, "order_id", "3")

    print(df_order_review_items)

    p_pd.csv_out(df_order_review_items, "order seller & review", out_path)



    df_order_and_seller_col = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                               'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date',
                               'order_estimated_delivery_date', 'seller_id']

    # p_pd.csv_out(df_order_and_seller, "order and seller", out_path, column_use=df_order_and_seller_col)

    # 3. get completed sales by seller

    sale_by_seller_query = """SELECT
                            s.seller_id,
                            s.seller_city,
                            s.seller_zip_code_prefix,
                            COUNT(DISTINCT (o.order_id || '_' || product_id)) AS delivered_order_count
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

csv_path = os.path.join(base,"Out")

p_fm.file_dir_create(csv_path)

# df output test
df_seller.to_csv(os.path.join(csv_path, "seller list.csv"), index=False)
print(df_seller.columns)
df_sales_by_seller.to_csv(os.path.join(csv_path, "seller sales.csv"), index=False)


# df_order.to_csv(os.path.join(csv_path, "order list.csv"), index=False)
# print(df_order.columns)
# delivered_date = df_order["order_delivered_customer_date"] #.dropna()
# print(len(delivered_date))

unique_seller = df_seller["seller_id"].unique()


print(unique_seller)
print(len(unique_seller))


print(df_sales_by_seller_debug)
print(df_sales_by_seller)
# print(df_sales_by_seller_and_year)

# data output for use by Tableau
