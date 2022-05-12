# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %%
import pandas as pd
import numpy as np
import datetime as dt
import glob
import json


# %%
# Customers

query_pc = 'SELECT Conv_Date as "date", Conv_ID as "conv_id", Customer_ID as "customer_id", Order_Num AS "order_num", SUM(Revenue) AS "revenue" FROM conversions_backend cb WHERE Account = "Paula" AND Market IN ("DE","AT") AND Conv_Date >= "2019-04-01" GROUP BY 1,2,3,4 ORDER BY 1 ASC'


# %%
import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='attribution-pc.ccjslhbmclgf.eu-central-1.rds.amazonaws.com',
                                         database='etl_site_traffic',
                                         user='fsg',
                                         password='Attribution3.0')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Your connected to database: ", record)
        df_og = pd.read_sql(query_pc,con=connection)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


# %%
df = df_og.copy()


# %%
df.shape

# %%
df.info()

# %%
get_ipython().run_line_magic('run', '../cohorts_pipeline_pc.py')



# %%
df = first_order(df)

# %%
## GSW Week Analysis
new_gsw_apr_19_users = df[(df.order_date>="2019-04-06") & (df.order_date<="2019-04-14") & (df.customer_type=="New")].customer_id.unique()

new_gsw_oct_19_users = df[(df.order_date>="2019-10-05") & (df.order_date<="2019-10-13") & (df.customer_type=="New")].customer_id.unique()

new_gsw_apr_20_users = df[(df.order_date>="2020-04-03") & (df.order_date<="2020-04-12") & (df.customer_type=="New")].customer_id.unique()

new_gsw_oct_20_users = df[(df.order_date>="2020-10-02") & (df.order_date<="2020-10-11") & (df.customer_type=="New")].customer_id.unique()

# Datasets
new_gsw_apr_19 = df[df.customer_id.isin(new_gsw_apr_19_users)]
new_gsw_oct_19 = df[df.customer_id.isin(new_gsw_oct_19_users)]
new_gsw_apr_20 = df[df.customer_id.isin(new_gsw_apr_20_users)]
new_gsw_oct_20 = df[df.customer_id.isin(new_gsw_oct_20_users)]


# %%
new_non_gsw = df[(~df.customer_id.isin(new_gsw_apr_19_users)) & (~df.customer_id.isin(new_gsw_oct_19_users)) & (~df.customer_id.isin(new_gsw_apr_20_users)) & (~df.customer_id.isin(new_gsw_oct_20_users))]


# %%
transactions_apr19, cohorts_apr19 = cohorts_pipeline(new_gsw_apr_19)
transactions_oct19, cohorts_oct19 = cohorts_pipeline(new_gsw_oct_19)
transactions_apr20, cohorts_apr20 = cohorts_pipeline(new_gsw_apr_20)
transactions_oct20, cohorts_oct20 = cohorts_pipeline(new_gsw_oct_20)
transactions_nongsw, cohorts_nongsw = cohorts_pipeline(new_non_gsw)


# %%
transactions_apr19 = transactions_apr19.astype(int)
transactions_oct19 = transactions_oct19.astype(int)
transactions_apr20 = transactions_apr20.astype(int)
transactions_oct20 = transactions_oct20.astype(int)
transactions_nongsw = transactions_nongsw.astype(int)


# %%
cohorts_apr19 = cohorts_apr19.astype(str)
cohorts_oct19 = cohorts_oct19.astype(str)
cohorts_apr20 = cohorts_apr20.astype(str)
cohorts_oct20 = cohorts_oct20.astype(str)
cohorts_nongsw = cohorts_nongsw.astype(str)


# %%
cohorts_apr19 = cohorts_apr19.apply(lambda x: x.str.replace('.',','))
cohorts_apr19 = cohorts_apr19.apply(lambda x: x.str.replace('nan',''))


cohorts_oct19 = cohorts_oct19.apply(lambda x: x.str.replace('.',','))
cohorts_oct19 = cohorts_oct19.apply(lambda x: x.str.replace('nan',''))

cohorts_apr20 = cohorts_apr20.apply(lambda x: x.str.replace('.',','))
cohorts_apr20 = cohorts_apr20.apply(lambda x: x.str.replace('nan',''))

cohorts_oct20 = cohorts_oct20.apply(lambda x: x.str.replace('.',','))
cohorts_oct20 = cohorts_oct20.apply(lambda x: x.str.replace('nan',''))

cohorts_nongsw = cohorts_nongsw.apply(lambda x: x.str.replace('.',','))
cohorts_nongsw = cohorts_nongsw.apply(lambda x: x.str.replace('nan',''))


# %%
from gspread_pandas import Spread, Client


# %%
pc_cohorts_data_dump = Spread('1ir4Z6ZbP4JWUg9loRkB22l9ty0IJIoUFyrVAZ3jKNsk')


# %%
# Push Transactions
pc_cohorts_data_dump.df_to_sheet(transactions_apr19, index=True, sheet='transactions_apr19', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(cohorts_apr19, index=True, sheet='cohorts_apr19', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(transactions_oct19, index=True, sheet='transactions_oct19', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(cohorts_oct19, index=True, sheet='cohorts_oct19', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(transactions_apr20, index=True, sheet='transactions_apr20', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(cohorts_apr20, index=True, sheet='cohorts_apr20', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(transactions_oct20, index=True, sheet='transactions_oct20', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(cohorts_oct20, index=True, sheet='cohorts_oct20', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(transactions_nongsw, index=True, sheet='transactions_nongsw', start='A1', replace=True)
pc_cohorts_data_dump.df_to_sheet(cohorts_nongsw, index=True, sheet='cohorts_nongsw', start='A1', replace=True)




# %%
