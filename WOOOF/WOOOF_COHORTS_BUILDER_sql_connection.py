# %% SCRIPT TO BUILD COHORTS AND GET THE PURCHASE TABLES
import numpy as np
from numpy.core.numeric import NaN
import pandas as pd
import datetime as dt
from cohorts_pipeline_woof_v4 import df_cleaning #function for obteining only relevant columns
from cohorts_pipeline_woof_v4 import cohorts_pipeline # cohort builder function
from cohorts_pipeline_woof_v4 import purchase_number # for the purchase behaviour analysis
import mysql.connector
from mysql.connector import Error

# %% BEFORE WE DID IT WITH A BACKEND FILE, NOW WE ESTABLISH A CONNECTION WITH THE DATABASE SO WE USE A QUERY FOR IT
query_orders = 'SELECT alo.Hashed_Email, alo.Conv_Date, alo.Conv_ID, CASE WHEN alo.Conv_Date = first_orders.first_date THEN "New" ELSE "Returning" END AS "Customer_Type", alo.Revenue, alo.Revenue_excl_VAT FROM api_shopware.api_shopware_orders alo JOIN ( SELECT Hashed_Email, MIN(Conv_Date) AS "first_date" FROM api_shopware.api_shopware_orders alo WHERE Account = "WOOOF" AND Conv_Status != "cancelled" GROUP BY 1) AS first_orders ON first_orders.Hashed_Email = alo.Hashed_Email WHERE Account = "WOOOF" AND Conv_Status != "cancelled" AND Shipping_Country = "Germany"'

# I took out the Customer_ID column since there are NULL Values on the table, but for Hashed_Email there isn't any missing values.


#%% CONNECT TO THE DATABASE AND FETCH THE DATA
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

#%% COPY THE ORIGINAL DATAFRAME TO AVOID QUERYING AGAIN
#df_og.columns
df = df_og.copy()
df_og.head()
# %% to check there are no missing values
df_og.isna().sum()

# %% CLEAN THE DATAFRAME AND GET NEW DATE COLUMNS
df = df_cleaning(df)
df.head()
# %% to check the new columns have no null values
df.isna().sum()

# %% OBTAIN TWO TABLES: ONE FOR WITH THE NEW AND RET ORDERS, OTHER WITH THE COHORTS
transactions, reorder_rates = cohorts_pipeline(df)

#%% Filling up the transactions table in case there are missing values
transactions = transactions.fillna(0)
transactions.head()
#%%
transactions.tail()
# %% Change all data to integers
transactions = transactions.astype(int)
# %% Change data to str to replace null values and dot to commas...
reorder_rates = reorder_rates.astype(str)
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('.',','))
reorder_rates = reorder_rates.apply(lambda x: x.str.replace('nan',''))
#%% Review that all months are in the table
reorder_rates.index.unique()
# there are no values from 2019-06 till 2020-02 !!

#%%
transactions.drop(['2016-01'], inplace=True)
transactions.head(3)
#%%
reorder_rates.drop(['2016-01'], inplace=True)
reorder_rates.head(3)

# %% CONNECTION TO THE GOOGLE SHEET TO DUMP THE RESULTS
from gspread_pandas import Spread, Client
wooof_cohorts_data_dump = Spread('WOOOF_COHORTS_DUMP')

# %% Push Transactions
wooof_cohorts_data_dump.df_to_sheet(transactions, index=True, sheet='TRANSACTIONS', start='A1', replace=True) # Review the sheets name 

wooof_cohorts_data_dump.df_to_sheet(reorder_rates, index=True, sheet='REORDER_RATES', start='A1', replace=True)

# %% Check columns
df.info()

# %% CREATE PURCHASE TABLE
temp0 = df.groupby(['Hashed_Email','Creation_Date']).agg({'Conv_ID':pd.Series.count})
temp0 = temp0.groupby(level=0).apply(purchase_number)
temp0.reset_index(inplace=True)
temp0.drop('Conv_ID',axis=1,inplace=True)
temp1 = df.groupby('Hashed_Email').agg({'Conv_ID':pd.Series.nunique})
temp1.rename(columns={'Conv_ID':'Total_Purchases'},inplace=True)
temp1.reset_index(inplace=True)
purchase_table = temp0.merge(temp1, on='Hashed_Email',how='left')
purchase_table['Days_Between'] = purchase_table.groupby('Hashed_Email')['Creation_Date'].diff().apply(lambda x: x.days)



# %% Fill missing values
purchase_table['Days_Between'] = purchase_table['Days_Between'].fillna(0)
purchase_table.head()

# %% CREATE TABLE OF THE DAYS DIFFERENCE
days_between_table = purchase_table.groupby(['Purchase_Number']).agg({'Days_Between': [pd.Series.mean, pd.Series.mode]}).reset_index()

# %% Rename columns
days_between_table.columns = ['Purchase_Number','Avg_Days_Between','Most_Common_Days_Between']
# %% Reduce the table to only the first 20 purchases
days_between_table = days_between_table[days_between_table.Purchase_Number<=20]
#%%
days_between_table['Most_Common_Days_Between'] = days_between_table['Most_Common_Days_Between'].apply(lambda x: x.mean())

days_between_table

#%%
days_between_table = days_between_table.astype(int)

#%% COPY TABLE TO ADD TO THE COHORTS FILE
days_between_table.to_clipboard(index=False)

# %%
purchase_table
#%% COPY TABLE TO ADD TO THE COHORTS FILE
# WE TAKE THE ONLY STATISTICS OF THE PURCHASE TABLE
purchase_table[(purchase_table.Purchase_Number<=20)].groupby(['Purchase_Number'])['Days_Between'].describe().to_clipboard( decimal=',')



# %%

