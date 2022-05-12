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
get_ipython().run_line_magic('run', './cohorts_pipeline_pc.py')

# %%
df = first_order(df)

# %%
## GSW Week Analysis New 
new_gsw_apr_19_users = df[(df.order_date>="2019-04-06") & (df.order_date<="2019-04-14") & (df.customer_type=="New")].customer_id.unique()
new_gsw_oct_19_users = df[(df.order_date>="2019-10-05") & (df.order_date<="2019-10-13") & (df.customer_type=="New")].customer_id.unique()
new_gsw_apr_20_users = df[(df.order_date>="2020-04-03") & (df.order_date<="2020-04-12") & (df.customer_type=="New")].customer_id.unique()
new_gsw_oct_20_users = df[(df.order_date>="2020-10-02") & (df.order_date<="2020-10-11") & (df.customer_type=="New")].customer_id.unique()
new_gsw_apr_21_users = df[(df.order_date>="2021-04-01") & (df.order_date<="2021-04-11") & (df.customer_type=="New")].customer_id.unique()

# Datasets
new_gsw_apr_19 = df[df.customer_id.isin(new_gsw_apr_19_users)]
new_gsw_oct_19 = df[df.customer_id.isin(new_gsw_oct_19_users)]
new_gsw_apr_20 = df[df.customer_id.isin(new_gsw_apr_20_users)]
new_gsw_oct_20 = df[df.customer_id.isin(new_gsw_oct_20_users)]
new_gsw_apr_21 = df[df.customer_id.isin(new_gsw_apr_21_users)]

# %%
## GSW Week Analysis Ret
ret_gsw_apr_19_users = df[(df.order_date>="2019-04-06") & (df.order_date<="2019-04-14") & (df.customer_type=="Returning")].customer_id.unique()
ret_gsw_oct_19_users = df[(df.order_date>="2019-10-05") & (df.order_date<="2019-10-13") & (df.customer_type=="Returning")].customer_id.unique()
ret_gsw_apr_20_users = df[(df.order_date>="2020-04-03") & (df.order_date<="2020-04-12") & (df.customer_type=="Returning")].customer_id.unique()
ret_gsw_oct_20_users = df[(df.order_date>="2020-10-02") & (df.order_date<="2020-10-11") & (df.customer_type=="Returning")].customer_id.unique()
ret_gsw_apr_21_users = df[(df.order_date>="2021-04-01") & (df.order_date<="2021-04-11") & (df.customer_type=="Returning")].customer_id.unique()

# Datasets
ret_gsw_apr_19 = df[df.customer_id.isin(ret_gsw_apr_19_users)]
ret_gsw_oct_19 = df[df.customer_id.isin(ret_gsw_oct_19_users)]
ret_gsw_apr_20 = df[df.customer_id.isin(ret_gsw_apr_20_users)]
ret_gsw_oct_20 = df[df.customer_id.isin(ret_gsw_oct_20_users)]
ret_gsw_apr_21 = df[df.customer_id.isin(ret_gsw_apr_21_users)]

# %%
## GSW Week Analysis All
gsw_apr_19_users = df[(df.order_date>="2019-04-06") & (df.order_date<="2019-04-14")].customer_id.unique()
gsw_oct_19_users = df[(df.order_date>="2019-10-05") & (df.order_date<="2019-10-13")].customer_id.unique()
gsw_apr_20_users = df[(df.order_date>="2020-04-03") & (df.order_date<="2020-04-12")].customer_id.unique()
gsw_oct_20_users = df[(df.order_date>="2020-10-02") & (df.order_date<="2020-10-11")].customer_id.unique()
gsw_apr_21_users = df[(df.order_date>="2021-04-01") & (df.order_date<="2021-04-11")].customer_id.unique()

# Datasets
gsw_apr_19 = df[df.customer_id.isin(gsw_apr_19_users)]
gsw_oct_19 = df[df.customer_id.isin(gsw_oct_19_users)]
gsw_apr_20 = df[df.customer_id.isin(gsw_apr_20_users)]
gsw_oct_20 = df[df.customer_id.isin(gsw_oct_20_users)]
gsw_apr_21 = df[df.customer_id.isin(gsw_apr_21_users)]

# %%
new_gsw_apr_21.shape, ret_gsw_apr_21.shape, gsw_apr_21.shape

# %%
"""
April 2021:
20% off
Free shipping from 20€
1 CYG trial size from 75€
1 CYG full size from 100€
 
October 2020:
20% off
Free shipping from 20€
BHA trial size from 75€
1 CYG full size from 100€

March/April 2020:
20% off
Free shipping from 0€
Hydrating mask trial size from 75€
Toner full size from 100€
"""
gsw_apr_20['aov_bucket'] = ""
gsw_oct_20['aov_bucket'] = ""
gsw_apr_21['aov_bucket'] = ""


# %%
def bucket_20(col):
    if (col <= 20.0):
        return '< 20'
    elif ((col > 20) & (col <= 75)):
        return '21 - 75'
    elif ((col > 75) & (col <= 100)):
        return '75 - 100'    
    else:
        return '> 100'
# %%
def bucket_0(col):
    if (col < 75):
        return '< 75'
    elif ((col >=75) & (col <= 100)):
        return '75 - 100'  
    else:
        return '> 100'

# %%
gsw_apr_20.loc[:,['aov_bucket']] = gsw_apr_20['revenue'].apply(lambda x: bucket_0(x))


# %%
gsw_oct_20.loc[:,['aov_bucket']] = gsw_oct_20['revenue'].apply(lambda x: bucket_20(x))


# %%
gsw_apr_21.loc[:,['aov_bucket']] = gsw_apr_21['revenue'].apply(lambda x: bucket_20(x))


# %%
gsw_apr_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#gsw_apr_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index()

# %%
gsw_oct_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#gsw_oct_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index()

# %%
gsw_apr_21.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#gsw_apr_21.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index()

# %%
new_gsw_apr_20['aov_bucket'] = ""
new_gsw_oct_20['aov_bucket'] = ""
new_gsw_apr_21['aov_bucket'] = ""
ret_gsw_apr_20['aov_bucket'] = ""
ret_gsw_oct_20['aov_bucket'] = ""
ret_gsw_apr_21['aov_bucket'] = ""


#%%
new_gsw_apr_20.loc[:,['aov_bucket']] = new_gsw_apr_20['revenue'].apply(lambda x: bucket_0(x))
new_gsw_oct_20.loc[:,['aov_bucket']] = new_gsw_oct_20['revenue'].apply(lambda x: bucket_20(x))
new_gsw_apr_21.loc[:,['aov_bucket']] = new_gsw_apr_21['revenue'].apply(lambda x: bucket_20(x))

#%%
new_gsw_apr_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#%%
new_gsw_oct_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#%%
new_gsw_apr_21.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
# %%

#%%
ret_gsw_apr_20.loc[:,['aov_bucket']] = ret_gsw_apr_20['revenue'].apply(lambda x: bucket_0(x))
ret_gsw_oct_20.loc[:,['aov_bucket']] = ret_gsw_oct_20['revenue'].apply(lambda x: bucket_20(x))
ret_gsw_apr_21.loc[:,['aov_bucket']] = ret_gsw_apr_21['revenue'].apply(lambda x: bucket_20(x))

#%%
ret_gsw_apr_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#%%
ret_gsw_oct_20.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
#%%
ret_gsw_apr_21.groupby('aov_bucket').agg({'conv_id':pd.Series.nunique}).reset_index().to_clipboard(index=False)
# %%
