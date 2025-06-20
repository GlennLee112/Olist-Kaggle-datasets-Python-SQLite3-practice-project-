import os
import pandas as pd

from Directory.Personal_Pandas import convert_to_datetime

base = os.path.dirname(__file__)
file_path = os.path.join(base,"Out","order seller & review.csv")

df_raw = pd.read_csv(file_path, date_format="%Y-%m-%d")
print(df_raw)
print(df_raw.info())

# guide:
# https://medium.com/@nomannayeem/clustering-with-confidence-a-practical-guide-to-data-clustering-in-python-15d82d8a7bfb

# Step 0. preprocessing
# Add delivery time column (order_delivered_customer_date - order_purchase_timestamp) & estimation dif (order_delivered_customer_date - order_estimated_delivery_date)
df_new = df_raw
col_to_convert = ["order_delivered_customer_date", "order_purchase_timestamp"]
print(df_new.info())
df_new["Delivery Time Taken"] = df_new["order_delivered_customer_date"] - df_new["order_purchase_timestamp"]

print(df_new["Delivery Time Taken"])

# Step 1. basic EDA (exploratory data analysis)

