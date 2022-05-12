# %%
import numpy as np
import pandas as pd
import datetime as dt
import cohorts_pipeline_pom_v3
# %%
df_og = pd.read_csv('./Data/orders_export_1.csv', sep=',', decimal=',')

#%%
#df_og.shape
#len(df_og[df_og['Billing Country'] == 'DE'].Name.unique())
df_og[['Subtotal', 'Shipping', 'Taxes', 'Total', 'Discount Amount']].dtypes

# %%
df = cohorts_pipeline_pom_v3.df_cleaning(df_og)
# %%
df.head()

#%%
months = df['First_Order_YM'].unique()

#%%
len(df['Email'].unique())
#%%
output_dfs = {p: df[df['First_Order_YM'] == p] for p in months}

#%%
output_dfs['2021-03']


#%%
x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Order_ID':pd.Series.nunique})
x_orders

#%%
df.groupby(level=0)['Creation_Date'].min()


#%%
def cohort_period(df):
    """
    Creates a `CohortPeriod` column, which is the Nth period based on the user's first purchase.

    Example
    -------
    Say you want to get the 3rd month for every user:
        df.sort(['UserId', 'OrderTime', inplace=True)
        df = df.groupby('UserId').apply(cohort_period)
        df[df.CohortPeriod == 3]
    """
    df['CohortPeriod'] = np.arange(len(df))
    return df


cohort_orders = pd.DataFrame()
cohort_customers = pd.DataFrame()
cohort_values = pd.DataFrame()
trans_summary =  pd.DataFrame()
projections = pd.DataFrame()
rev_summary = pd.DataFrame()

for p in output_dfs:

        dates = pd.DataFrame()

        dates['dates'] = pd.date_range(start=output_dfs[p]['Creation_Date'].min(), end=output_dfs[p]['Creation_Date'].max(), freq="D")
        dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))

        x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Order_ID':pd.Series.nunique})
        x_users = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Email':pd.Series.nunique})
        x_values = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'ValueNOVAT':pd.Series.sum})

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
        x_values.reset_index(inplace=True)

        dates_od = dates['ym'].unique()
        dates_fo = output_dfs[p]['First_Order_YM'].unique()
        idx = pd.MultiIndex.from_product((dates_od,dates_fo), names=['Creation_Date_YM','First_Order_YM'])

        x_orders = x_orders.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
        x_users = x_users.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
        x_values = x_values.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)

        x_orders = x_orders.groupby(level=1).apply(cohort_period)
        x_users = x_users.groupby(level=1).apply(cohort_period)
        x_values = x_values.groupby(level=1).apply(cohort_period)

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
        x_values.reset_index(inplace=True)

        x_orders.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        x_users.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        x_values.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)

        ord_result = x_orders['Order_ID'].unstack(1)
        cust_result = x_users['Email'].unstack(1)
        value_result = x_values['ValueNOVAT'].unstack(1)

        cohort_orders = cohort_orders.append(ord_result)
        cohort_customers = cohort_customers.append(cust_result)
        cohort_values = cohort_values.append(value_result)

cohort_orders.head()

#%%
output_dfs['2021-07'].groupby(['Creation_Date_YM','First_Order_YM']).agg({'ValueNOVAT':pd.Series.sum})

#%%
output_dfs['2021-07'].columns

#%%
p = '2021-07'
dates = pd.DataFrame()

dates['dates'] = pd.date_range(start=output_dfs[p]['Creation_Date'].min(), end=output_dfs[p]['Creation_Date'].max(), freq="D")
dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))

x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Order_ID':pd.Series.nunique})
x_users = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Email':pd.Series.nunique})
x_values = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'ValueNOVAT':pd.Series.sum})

#%%
x_values

#%%
x_orders.reset_index(inplace=True)
x_users.reset_index(inplace=True)
x_values.reset_index(inplace=True)


#%%
dates_od = dates['ym'].unique()
dates_fo = output_dfs[p]['First_Order_YM'].unique()
idx = pd.MultiIndex.from_product((dates_od,dates_fo), names=['Creation_Date_YM','First_Order_YM'])
#%%
dates_od
#%%
x_orders = x_orders.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
x_users = x_users.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
x_values = x_values.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)

x_orders = x_orders.groupby(level=1).apply(cohort_period)
x_users = x_users.groupby(level=1).apply(cohort_period)
x_values = x_values.groupby(level=1).apply(cohort_period)

x_orders.reset_index(inplace=True)
x_users.reset_index(inplace=True)
x_values.reset_index(inplace=True)


# %% CONTINUATION
transactions, reorder_rates = cohorts_pipeline_pom_v3.cohorts_pipeline(df)
# %%
transactions.fillna(0, inplace=True)
transactions = transactions.astype(int)
# %%
reorder_rates = reorder_rates.astype(str)
# %%
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('.',','))
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('nan',''))

#%%
transactions.reset_index(inplace=True)
transactions
#%%
reorder_rates.reset_index(inplace=True)
reorder_rates

#%%
reorder_rates.reset_index(inplace=True, drop=True)
reorder_rates


# %% DUMP DATA ON GOOGLE SHEET
from gspread_pandas import Spread, Client
import gspread
gc = gspread.service_account()
g_sheet = '1pymRU0rhpfRQKlUzSFbD8sK_QkTB0WUctzgOuZ8W-Jc'
sheet_name = gc.open_by_key(g_sheet)

# %%
#wooof_cohorts_data_dump = Spread('POM_COHORTS_DUMP')
worksheet = sheet_name.worksheet('TRANSACTIONS') 
worksheet.update([transactions.columns.values.tolist()] + transactions.values.tolist())

# %%
worksheet = sheet_name.worksheet('REORDER_RATES') 
worksheet.update([reorder_rates.columns.values.tolist()] + reorder_rates.values.tolist())

# %%
# Push Transactions
#sheet_name.df_to_sheet(transactions, index=True, sheet='TRANSACTIONS', start='A1', replace=True)
#wooof_cohorts_data_dump.df_to_sheet(reorder_rates, index=True, sheet='REORDER_RATES', start='A1', replace=True)
# %%
