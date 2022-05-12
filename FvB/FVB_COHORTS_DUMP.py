#%%
import numpy as np
from numpy.core.numeric import NaN
import pandas as pd
import datetime as dt
#pd.options.mode.chained_assignment = None
from cohorts_pipeline_fvb_v3 import df_cleaning_source # cohort builder function
from cohorts_pipeline_fvb_v3 import cohort_period # for the purchase behaviour analysis
from cohorts_pipeline_fvb_v3 import first_order 
from cohorts_pipeline_fvb_v3 import cohorts

# %%
df_og = pd.read_csv('./Data/orders2016-2021.csv', sep=';', decimal=',')
df_og_AT = pd.read_csv('./Data/orders-jan-june-21AT.csv', sep=';', decimal=',') #DOWNLOAD AUSTRIA FROM 2016
#df_og_16 = pd.read_csv('./Data/orders2016-2018.csv', sep=';', decimal=',')
#%%
df_og.shape, df_og_AT.shape

#%%
#df =df_og.copy()
#df2 = df_og_16.copy()
df = pd.concat([df_og, df_og_AT])
df.head()

# %%
df_og_AT.Locale.unique()
#%%
df = df[df.Status != 'Geannuleerd']
df = df.iloc[:,[0,1,2,3,4,5,6,8]]
df.columns = ['Merchant_Reference','Creation_Date','First_Name','Last_Name','Email', 'Payment_Method','Revenue','Market']
#%%
df['Revenue'] = df['Revenue'].str.replace(',','')
df['Revenue'] = df['Revenue'].astype(float)
#df.Revenue.sum()
#%%
df['Creation_Date'] = pd.to_datetime(df['Creation_Date'], format='%Y-%m-%d %H:%M:%S')

# %%
df_og_16.head()
# %%
df_og_16.columns
# %%
df_og_16.Orderdatum.min()
# %%

# %%
df3 = pd.concat([df2, df])
df3.head()
# %%
df.Orderdatum.min()
# %%
df.Orderdatum.max()

#%%
df3.columns
# %%
#df = df_cleaning_source(df)
# %%
df.head()
# %%
df = first_order(df)
# %%
df.shape, df_og.shape
#df_og_16.shape
# %%
df.isna().sum()
#df[df.First_Order.isna()]

#%%
df.groupby(by='Creation_Date_YM').agg('Revenue').sum()

#%%
df.dropna(inplace=True)

#%%
df.groupby(by='Creation_Date_YM').agg('Revenue').sum()
# %% OBTAIN TWO TABLES: ONE FOR WITH THE NEW AND RET ORDERS, OTHER WITH THE COHORTS
reorder_rates, transactions = cohorts(df)
# %%
df= cohort_period(df)
# %%
df.head()

# %%
transactions
# %%
reorder_rates
# %%
transactions.to_clipboard(index=True)

# %%

# %% Change all data to integers
transactions = transactions.astype(int)
# %% Change data to str to replace null values and dot to commas...
reorder_rates = reorder_rates.astype(str)
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('.',','))
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('nan',''))
#%% Review that all months are in the table
reorder_rates.index.unique()

# %% CONNECTION TO THE GOOGLE SHEET TO DUMP THE RESULTS
from gspread_pandas import Spread, Client
fvb_cohorts_data_dump = Spread('FVB_COHORTS_DUMP')
# %%
# %% Push Transactions
fvb_cohorts_data_dump.df_to_sheet(transactions, index=True, sheet='TRANSACTIONS', start='A1', replace=True) # Review the sheets name 

#%%
fvb_cohorts_data_dump.df_to_sheet(reorder_rates, index=True, sheet='REORDER_RATES', start='A1', replace=True)

# %%
df3.columns
# %%
TO_per_year = df.groupby(['Creation_Date_YM']).agg({'Merchant_Reference':pd.Series.nunique, 'Revenue':pd.Series.sum}).reset_index()
# %%
TO_per_year
# %%
df_og.columns
# %%
df_og.Aflevermethode.unique()
# %% CHECK NOVEMBER
df_og2 = pd.read_csv('./Data/export_grid_082ba157-00ae-468d-9222-35f82c33f182.csv', sep=';')
df_og2.head()

# %%
nov_df = df_og2.copy()
#df_og2 = df_cleaning_source(df_og2)
#%%
nov_df = nov_df[nov_df.Status != 'Geannuleerd']
nov_df = nov_df.iloc[:,[0,1,2,3,4,5,6,8,10]]
nov_df.columns = ['Merchant_Reference','Creation_Date','First_Name','Last_Name','Email', 'Payment_Method','Revenue','Market', 'Status']
#%%
nov_df['Revenue'] = nov_df['Revenue'].str.replace(',','')
nov_df['Revenue'] = nov_df['Revenue'].astype(float)
nov_df.Revenue.sum()
#%%
nov_df['Creation_Date'] = pd.to_datetime(nov_df['Creation_Date'], format='%Y-%m-%d %H:%M:%S')


#%%
nov_df = first_order(nov_df)

#%%
reorder_rates, transactions = cohorts(nov_df)

# %%
df_og2 = first_order(df_og2)
# %%
df_og2.columns
# %%
df_og2.Revenue.sum()
# %%
df_og2.Market.unique()
# %%
reorder_rates, transactions = cohorts(df_og2)
# %%
df_og2.isna().sum()
# %%
df_og2.dropna(inplace=True)
# %%
transactions
# %%
df_og2.head()
# %%
df_og.head()
# %%
