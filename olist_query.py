import sqlite3
import os
from functools import reduce

import pandas as pd
from openpyxl.styles.builtins import output

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

def query_out(result_in:pd.DataFrame,
              base_path:str,
              file_name:str,
              extension_type:int=0,         # Output type is csv (0) by default
              output_index:bool=False):     # Index disabled by default

    # Dict holding extension possible
    extension_dict = {0:".csv",
                      1:".xlsx"}

    # Select extension using extension_type
    extension_used = extension_dict[extension_type]

    # output_path fabrication / joining
    output_path = os.path.join(base_path, file_name + extension_used)

    # output type by extension type
    if extension_type == 0:
        result_in.to_csv(output_path, index = output_index)

    elif extension_type == 1:
        result_in.to_excel(output_path, index = output_index)


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

def seller_query_func(conn_use:sqlite3.Connection)-> None | pd.DataFrame:

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
                     min_date:str,
                     max_date: str,
                     output_path:str,
                     data_only:bool=True) -> None | pd.DataFrame:

    """New order query function:

    Query orders and join with review score and items information to get the overall order information"""

    # Query to use
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

    order_seller_query = """SELECT
                            oi.order_id,
                            oi.seller_id,
                            oi.price
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

    order_products_query = """SELECT
                            o.order_id,
                            ps.product_id,
                            ps.product_category_name,
                            ps.product_weight_g
                        FROM 
                            orders o
                        JOIN order_items oi ON oi.order_id = o.order_id
                        JOIN products ps ON oi.product_id = ps.product_id
                        WHERE
                            o.order_purchase_timestamp BETWEEN ? AND ?;"""

    # Test query only; disable when in actual use
    # order_items_query = """SELECT
    #                         oi.order_id,
    #                         oi.order_item_id
    #                     FROM
    #                         order_items oi
    #                     JOIN products ps ON oi.product_id = ps.product_id
    #                     JOIN orders o ON oi.order_id = o.order_id
    #                     WHERE
    #                         o.order_purchase_timestamp BETWEEN ? AND ?;"""
    #
    # review_comment_query = """SELECT
    #                             ors.order_id,
    #                             ors.review_comment_title,
    #                             ors.review_comment_message
    #                         FROM
    #                             order_reviews ors
    #                         JOIN orders o ON o.order_id = ors.order_id
    #                         WHERE
    #                             o.order_purchase_timestamp BETWEEN ? AND ?;"""

    product_cat_translation_query = """SELECT
                                        pcnt.product_category_name,
                                        pcnt.product_category_name_english
                                    FROM
                                        product_category_name_translation pcnt
                                    JOIN products ps ON pcnt.product_category_name = ps.product_category_name
                                    JOIN order_items oi ON ps.product_id = oi.product_id
                                    JOIN orders o ON oi.order_id = o.order_id
                                    WHERE
                                        o.order_purchase_timestamp BETWEEN ? AND ?;"""

    # Connect & query
    with conn_use:
        order_result = pd.read_sql_query(orders_query, con=conn_use, params=[min_date, max_date])
        order_seller_result = pd.read_sql_query(order_seller_query, con=conn_use, params=[min_date, max_date])
        order_reviews_result = pd.read_sql_query(order_reviews_query, con=conn_use, params=[min_date, max_date])
        order_products_result = pd.read_sql_query(order_products_query, con=conn_use, params=[min_date, max_date])
        # order_items_result = pd.read_sql_query(order_items_query, con=conn_use, params=[min_date, max_date]) # testing only; disable or enable as required
        # review_comment_result = pd.read_sql_query(review_comment_query, con=conn_use, params=[min_date, max_date]) # testing only; disable or enable as required
        product_translation_result = pd.read_sql_query(product_cat_translation_query, con=conn_use, params=[min_date, max_date])

    # joining
    # list of df to join
    df_list_to_use = [order_result, order_seller_result, order_reviews_result, order_products_result]

    # join 1 -- join review and
    final_df = p_pd.df_batch_merge(df_list_to_use, "order_id", "3")
    # test query order items

    # grouping
    product_translation_result_final = product_translation_result.drop_duplicates()

    # Query_out if not data_only (i.e.: data_only=False)
    if not data_only:
        query_out(final_df, output_path, "order overall")
        query_out(product_translation_result_final, output_path, "product cat translation")
        # query_out(order_items_result, output_path, "order_items") # testing only; disable or enable as required
        # query_out(review_comment_result, output_path, "review comment") # testing only; disable or enable as required

    # Return final_df
    return final_df

# def product_category_en(conn_use:sqlite3.Connection,
#                      min_date:str,
#                      max_date: str,
#                      output_path:str,
#                      data_only:bool=True) -> None | pd.DataFrame:
#

# data output for use by Tableau