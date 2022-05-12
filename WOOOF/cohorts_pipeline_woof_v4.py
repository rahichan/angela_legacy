# version for the query from database

import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None

def df_cleaning(df):
    """
    Based on the specific column names
    """
    df['Creation_Date'] = pd.to_datetime(df['Conv_Date'])
    df.set_index('Hashed_Email', inplace=True)
    df['First_Order'] = df.groupby(level=0)['Creation_Date'].min()
    df['First_Order_YM'] = df.groupby(level=0)['Creation_Date'].min().apply(lambda x: x.strftime('%Y-%m'))
    df.reset_index(inplace=True)
    df['Creation_Date_YM'] = df['Creation_Date'].apply(lambda x: x.strftime('%Y-%m'))
    df['Year'] = df['Creation_Date'].dt.year
    df['Week'] = df['Creation_Date'].dt.week
    df['Year_Week'] = df['Creation_Date'].dt.strftime("%Y-%W")
    return df

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

def purchase_number(df):
    """
    Creates a `Purchase_Number` column, which is the Nth period based on the user's first purchase.

    Example
    -------
    Say you want to get the 3rd month for every user:
        df.sort(['UserId', 'OrderTime', inplace=True)
        df = df.groupby('UserId').apply(cohort_period)
        df[df.CohortPeriod == 3]
    """
    df['Purchase_Number'] = np.arange(len(df)) + 1
    return df

def cohorts_pipeline(df):
    """
    Creates the returning rates and transactions tables

    """
    months = df['First_Order_YM'].unique()

    output_dfs = {p: df[df['First_Order_YM'] == p] for p in months}

    cohort_orders = pd.DataFrame()
    cohort_customers = pd.DataFrame()

    for p in output_dfs:

        dates = pd.DataFrame()
        try:
            dates['dates'] = pd.date_range(start=output_dfs[p]['Conv_Date'].min(), end=output_dfs[p]['Conv_Date'].max(), freq="D")
            dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))
        except:
            ValueError

        x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Conv_ID':pd.Series.nunique})
        x_users = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Hashed_Email':pd.Series.nunique})
       
        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
        
        dates_od = dates['ym'].unique()
        dates_fo = output_dfs[p]['First_Order_YM'].unique()
        idx = pd.MultiIndex.from_product((dates_od,dates_fo), names=['Creation_Date_YM','First_Order_YM'])

        x_orders = x_orders.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
        x_users = x_users.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
       

        x_orders = x_orders.groupby(level=1).apply(cohort_period)
        x_users = x_users.groupby(level=1).apply(cohort_period)
        

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
       

        x_orders.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        x_users.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        

        ord_result = x_orders['Conv_ID'].unstack(1)
        cust_result = x_users['Hashed_Email'].unstack(1)
      

        cohort_orders = cohort_orders.append(ord_result)
        cohort_customers = cohort_customers.append(cust_result)
      

    ret_rate = cohort_orders.divide(cohort_customers[0],axis=0)

    ret_rate[0] = ret_rate[0] - 1

    ret_rate = ret_rate.sort_values(by='First_Order_YM')


    
    trans_month = df.groupby(['Creation_Date_YM','Customer_Type']).agg({'Conv_ID':pd.Series.nunique, 'Revenue_excl_VAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
    trans_month.rename(columns={'Conv_ID':'All_Orders','Revenue_excl_VAT':'All_Revenue'},inplace=True)
    trans_month.set_index('Creation_Date_YM', inplace=True)
    try:
        trans_month.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']
        trans_month['AOV_Ret'] = trans_month['Ret_Rev']/trans_month['Ret_Trans']
    except ValueError:
        trans_month.columns = ['New_Trans','New_Rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']

    # for col in ret_rate.columns:
    #     s = np.array(ret_rate[col])
    #     s = s[np.logical_not(np.isnan(s))]
    #     means = np.array([])
    #     for i in range(6):
    #         t = s[-6:]
    #         m = t.mean()
    #         s = np.append(s,m)
    #         means = np.append(means,m)
    #     projections[col] = means

    return  trans_month, ret_rate
