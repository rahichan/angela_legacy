#%%[1]
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import chart_studio.plotly as py
import plotly.offline as pyoff
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import mysql.connector
from mysql.connector import Error
import datetime as dt

pd.set_option('display.max_columns', None)

#%%[2]
df_og = pd.read_csv('./Data/KOI_export_082021.csv')
df_orders = df_og.copy()
df_orders.head()


#%%[3]
df_orders['Financial Status'].unique()
# %%
df_orders.isna().sum()
# %%
df_orders.shape
# %%
df_orders.columns

# %%
df_orders.groupby(['Name', 'Email']).agg({'Id':pd.Series.count})


# %%
df_orders.rename(columns={'Date': 'date','Name':'cus_id','Total':'total', 'Financial Status':'status','Email':'email', 'Subtotal':'subtotal', 'Shipping':'shipping',
       'Taxes':'taxes', 'Discount Amount':'discount_amount', 'Refunded Amount':'refunded_amount', 'Billing Country': 'billing_country'}, inplace=True)


# %%
df_order