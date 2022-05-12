# %%
import numpy as np
import pandas as pd
import datetime as dt
import cohorts_pipeline_pom_v3
# %%
df_og = pd.read_csv('./Data/orders_export_14102021.csv', sep=',', decimal=',')

#%%
df_og.columns
#%%
#df_og.shape
#len(df_og[df_og['Billing Country'] == 'DE'].Name.unique())
df_og[['Subtotal', 'Shipping', 'Taxes', 'Total', 'Discount Amount']].dtypes

# %%
df = cohorts_pipeline_pom_v3.df_cleaning(df_og)
# %%
df.head()

#%%
df['Observation'] = df['Creation_Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
df.head(2)

#%%
summary = df[df['Observation']>= "2021-09-01"].groupby(['Observation','Customer_Type']).agg({'Order_ID':pd.Series.nunique, 'ValueNOVAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
summary
#%%
summary.rename(columns={'Order_ID':'All_Orders','ValueNOVAT':'All_Revenue'},inplace=True)
summary.set_index('Observation', inplace=True)
summary

#%%
summary.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
summary
#%%
#summary.unstack(1).fillna(0).reset_index(inplace=True, drop=True)
summary['Revenue_EXVAT'] = summary['New_Rev'] + summary['Ret_Rev']
summary['Transactions'] = summary['New_Trans'] + summary['Ret_Trans']
summary

#%%
gsw_2021_df = summary[['Revenue_EXVAT', 'Transactions', 'New_Rev', 'New_Trans', 'Ret_Rev', 'Ret_Trans']]
gsw_2021_df

#%%
gsw_2021_df.to_clipboard()

#%%
summary = df[df['Observation']>= "2021-03-01"][df['Observation']< "2021-04-12"].groupby(['Observation','Customer_Type']).agg({'Order_ID':pd.Series.nunique, 'ValueNOVAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
summary.set_index('Observation', inplace=True)
summary.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
summary['Revenue_EXVAT'] = summary['New_Rev'] + summary['Ret_Rev']
summary['Transactions'] = summary['New_Trans'] + summary['Ret_Trans']
gsw_2021_q1_df = summary[['Revenue_EXVAT', 'Transactions', 'New_Rev', 'New_Trans', 'Ret_Rev', 'Ret_Trans']]
gsw_2021_q1_df.to_clipboard()




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
transactions.tail()
# %%
# Glamour Shopping Week Oct 2021 2-10
df[df["Creation_Date"]>"2021-10-01"][df["Creation_Date"]<"2021-10-11"][df["Customer_Type"] == "New"]
# %%
New_custs = df[df["Creation_Date"]>"2021-09-30"][df["Creation_Date"]<"2021-10-12"][df["Customer_Type"] == "New"]
len(New_custs)

# %%
df["Creation_Date"].min()
# %%
# GSW Q3 2020 Oct. 2-11
# 
New_2020 = df[df["Creation_Date"]>"2020-10-01"][df["Creation_Date"]<"2020-10-12"][df["Customer_Type"] == "New"]
len(New_2020)
# %%
df.columns
# %%
df_og.columns
# %%
df_og["Accepts Marketing"].unique()
# %%
df_og[df_og["Accepts Marketing"]== "yes"].loc[:,['Email']]
# %%
accept_markt = df_og[df_og["Accepts Marketing"]== "yes"].loc[:,['Email']]
accept_markt.to_csv('./Outputs/accepts_markt_14102021.csv', index=False)
# %%
df.Creation_Date.max()
# %%
