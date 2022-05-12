# %%
import numpy as np
import pandas as pd
import datetime as dt
#from cohorts_pipeline_woof_v4 import df_cleaning
#from cohorts_pipeline_woof_v4 import cohorts_pipeline
import mysql.connector
from mysql.connector import Error
# %%
#df_og = pd.read_csv('./Data/orders.csv', sep=';', decimal=',')
query_orders = 'SELECT alo.Customer_ID, alo.Hashed_Email, alo.Conv_Date, alo.Conv_ID, CASE WHEN alo.Conv_Date = first_orders.first_date THEN "New" ELSE "Returning" END AS "Customer_Type", alo.Revenue, alo.Revenue_excl_VAT FROM api_shopware.api_shopware_orders alo JOIN ( SELECT Hashed_Email, MIN(Conv_Date) AS "first_date" FROM api_shopware.api_shopware_orders alo WHERE Account = "WOOOF" AND Conv_Status != "cancelled" GROUP BY 1) AS first_orders ON first_orders.Hashed_Email = alo.Hashed_Email WHERE Account = "WOOOF" AND Conv_Status != "cancelled" AND Shipping_Country = "Germany"'

#%% 
try:
    connection = mysql.connector.connect(host='attribution-system-fsg-new.cob86lv75rzo.eu-west-1.rds.amazonaws.com',
                                         database='api_lightspeed',
                                         user='fsg',
                                         password='Attribution3.0')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Your connected to database: ", record)
        df_og = pd.read_sql(query_orders,con=connection)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

#%%
#df_og.columns
df_og.head()

#%%
df_og.Conv_Date.max()

#%%
df = df_og.copy()

#%%
df.set_index('Customer_ID', inplace=True)

df['First_Order'] = df.groupby(level=0)['Conv_Date'].min()
df['First_Order_YM'] = df.groupby(level=0)['Conv_Date'].min().apply(lambda x: x.strftime('%Y-%m'))
df.reset_index(inplace=True)

df['Creation_Date_YM'] = df['Conv_Date'].apply(lambda x: x.strftime('%Y-%m'))

#%%
df['Year'] = df['Conv_Date'].dt.year
df['Week'] = df['Conv_Date'].dt.week
df['Year_Week'] = df['Conv_Date'].dt.strftime("%Y-%W")

#%%
df.head()


#%%
months = df['First_Order_YM'].unique()

output_dfs = {p: df[df['First_Order_YM'] == p] for p in months}

cohort_orders = pd.DataFrame()
cohort_customers = pd.DataFrame()
cohort_values = pd.DataFrame()
trans_summary =  pd.DataFrame()
projections = pd.DataFrame()
rev_summary = pd.DataFrame()

rev_summary['New'] = df[df['Customer_Type']=='New'].groupby('Creation_Date_YM')['ValueNOVAT'].sum()
rev_summary['Returning'] = df[df['Customer_Type']=='Returning'].groupby('Creation_Date_YM')['ValueNOVAT'].sum()

#%%
from cohorts_pipeline_woof_v4 import df_cleaning
from cohorts_pipeline_woof_v4 import cohorts_pipeline


# %%
df = df_cleaning(df_og)
# %%
df.head()
# %%
transactions, reorder_rates = cohorts_pipeline(df)

#%%
transactions = transactions.fillna(0)
transactions.head()

# %%
transactions = transactions.astype(int)
# %%
reorder_rates = reorder_rates.astype(str)
# %%
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('.',','))
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('nan',''))
# %%
from gspread_pandas import Spread, Client
# %%
wooof_cohorts_data_dump = Spread('WOOOF_COHORTS_DUMP')
# %%
# Push Transactions
wooof_cohorts_data_dump.df_to_sheet(transactions, index=True, sheet='TRANSACTIONS_JULY', start='A1', replace=True)
wooof_cohorts_data_dump.df_to_sheet(reorder_rates, index=True, sheet='REORDER_RATES_JULY', start='A1', replace=True)
# %%
transactions
# %%
