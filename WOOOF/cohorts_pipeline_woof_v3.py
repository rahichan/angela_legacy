import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None

def df_cleaning(df):
    df = df.loc[:,['Order','Order_ID','Added','Status','Payment','Price_total_ex','E-mail','Customer','Firstname','Lastname', 'Product_title']]
    df['Product_title'] = df['Product_title'].replace('WOOOF Lamm Kartoffel  ','WOOOF Lamm & Kartoffel')
    df['Product_title'] = df['Product_title'].replace('WOOOF  Lamm & Kartoffel  ','WOOOF Lamm & Kartoffel')
    df['Product_title'] = df['Product_title'].replace('WOOOF Hundefutter -  Lamm & Kartoffel  ','WOOOF Lamm & Kartoffel')
    df['Product_title'] = df['Product_title'].replace('Palette WOOOF','WOOOF Palette')
    df['Product_title'] = df['Product_title'].replace('WOOOF Palette  - 10% Rabatt ','WOOOF Palette')
    df['Product_title'] = df['Product_title'].replace('WOOOF Palette  - 10% Rabatt','WOOOF Palette')
    df['Product_title'] = df['Product_title'].replace('WOOOF Palette  - 7% Rabatt','WOOOF Palette')
    df['Product_title'] = df['Product_title'].replace('WOOOF glutenfrei Lamm & Kartoffel  ','WOOOF GF Lamm & Kartoffel')
    df['Creation_Date'] = pd.to_datetime(df['Added'].str.split('@', expand=True)[0].apply(lambda x: x.strip()), format='%d-%m-%Y')
    df.rename(columns={'Price_total_ex':'ValueNOVAT','Customer':'Customer_ID'}, inplace=True)
    df.drop('Added',axis=1,inplace=True)
    df['Creation_Date'] = pd.to_datetime(df['Creation_Date'])
    df = df[~df['Status'].isin(['Cancelled'])]
    df.set_index('Customer_ID', inplace=True)
    df['First_Order'] = df.groupby(level=0)['Creation_Date'].min()
    df['First_Order_YM'] = df.groupby(level=0)['Creation_Date'].min().apply(lambda x: x.strftime('%Y-%m'))
    df.reset_index(inplace=True)
    df['Creation_Date_YM'] = df['Creation_Date'].apply(lambda x: x.strftime('%Y-%m'))
    first_orders = df.sort_values('Creation_Date').groupby('E-mail')['Order'].first().values
    df['Customer_Type'] = np.where(df['Order'].isin(first_orders),'New','Returning')
    df['Payment_Type'] = np.where(df['Payment'] == 'Payment Amazon','Amazon','Webshop')
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

def cohorts_pipeline(df):
    """
    Creates the returning rates and transactions tables

    """
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

    for p in output_dfs:

        dates = pd.DataFrame()

        dates['dates'] = pd.date_range(start=output_dfs[p]['Creation_Date'].min(), end=output_dfs[p]['Creation_Date'].max(), freq="D")
        dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))

        x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Order':pd.Series.nunique})
        x_users = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'E-mail':pd.Series.nunique})
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

        ord_result = x_orders['Order'].unstack(1)
        cust_result = x_users['E-mail'].unstack(1)
        value_result = x_values['ValueNOVAT'].unstack(1)

        cohort_orders = cohort_orders.append(ord_result)
        cohort_customers = cohort_customers.append(cust_result)
        cohort_values = cohort_values.append(value_result)

    ret_rate = cohort_orders.divide(cohort_customers[0],axis=0)

    ret_rate[0] = ret_rate[0] - 1

    ret_rate = ret_rate.sort_values(by='First_Order_YM')

    # trans_summary['New_Orders']  = cohort_customers[0]
    # trans_summary['Ret_Orders'] = np.subtract(cohort_orders[0],cohort_customers[0],dtype=float)+cohort_orders.iloc[:,1:].sum(axis=1,skipna=True)
    # trans_summary['New_Revenue']  = cohort_values[0]
    # trans_summary['Ret_Revenue'] = cohort_values.iloc[:,1:].sum(axis=1,skipna=True)
    # trans_summary['AOV_New'] = np.divide(trans_summary['New_Revenue'],trans_summary['New_Orders'])
    # trans_summary['AOV_Ret'] = np.divide(trans_summary['Ret_Revenue'],trans_summary['Ret_Orders'])


    #Transaction Summary Calcualtion
    #Transaction Summary Calcualtion
    trans_summary =  pd.DataFrame()
    trans_month = df.groupby(['Creation_Date_YM','Customer_Type']).agg({'Order':pd.Series.nunique, 'ValueNOVAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
    trans_month.rename(columns={'Merchant_Reference':'All_Orders','ValueNOVAT':'All_Revenue'},inplace=True)
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
