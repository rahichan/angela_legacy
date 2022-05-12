import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None

def df_cleaning_source(df):
    df.drop(['Unnamed: 9'],axis=1,inplace=True)
    df = df[df.Status != 'Geannuleerd']
    df = df[df.Aflevermethode == 'Thuisbezorgd']
    df = df[df.Locale.isin(['de_DE','de_AT'])]
    df = df.iloc[:,[0,1,2,3,4,6,8]]
    df.columns = ['Merchant_Reference','Creation_Date','First_Name','Last_Name','Email','Revenue','Market']
    df['Creation_Date'] = pd.to_datetime(df['Creation_Date'], format='%Y-%m-%d %H:%M:%S')
    df['Revenue'] = df['Revenue'].str.replace(',','')
    df['Revenue'] = df['Revenue'].astype(float)
    return df

def first_order(df):
    df['Email'] = df['Email'].str.lower()
    df['Creation_Date_YM'] = df['Creation_Date'].apply(lambda x: x.strftime('%Y-%m'))
    df.set_index('Email', inplace=True)
    df['First_Order'] = df.groupby(level=0)['Creation_Date'].min()
    df['First_Order_YM'] = df.groupby(level=0)['Creation_Date'].min().apply(lambda x: x.strftime('%Y-%m'))
    first_orders = df.sort_values('Creation_Date').groupby('Email')['Merchant_Reference'].first().values
    df['Customer_Type'] = np.where(df['Merchant_Reference'].isin(first_orders),'New','Returning')
    df.reset_index(inplace=True)
    return df

def Sale_Label(col):
    if col in pd.date_range(start='2016-03-07', end='2016-03-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-03-07', end='2017-03-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-03-07', end='2018-03-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-03-07', end='2019-03-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2016-04-01', end='2016-04-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-04-01', end='2017-04-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-04-01', end='2018-04-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-04-01', end='2019-04-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2016-05-01', end='2016-05-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-05-01', end='2017-05-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-05-01', end='2018-05-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-05-01', end='2019-05-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2016-09-07', end='2016-09-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-09-07', end='2017-09-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-09-07', end='2018-09-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-09-07', end='2019-09-30'):
        return "Non Sale"
    elif col in pd.date_range(start='2016-10-01', end='2016-10-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-10-01', end='2017-10-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-10-01', end='2018-10-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-10-01', end='2019-10-31'):
        return "Non Sale"
    elif col in pd.date_range(start='2016-11-01', end='2016-11-20'):
        return "Non Sale"
    elif col in pd.date_range(start='2017-11-01', end='2017-11-20'):
        return "Non Sale"
    elif col in pd.date_range(start='2018-11-01', end='2018-11-20'):
        return "Non Sale"
    elif col in pd.date_range(start='2019-11-01', end='2019-11-20'):
        return "Non Sale"
    else:
        return "Sale"

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

def cohorts(df):

    months = df['First_Order_YM'].unique()

    output_dfs = {p: df[df['First_Order_YM'] == p] for p in months}

    cohort_orders = pd.DataFrame()
    cohort_customers = pd.DataFrame()
    # cohort_values = pd.DataFrame()
    trans_summary =  pd.DataFrame()
    rev_summary = pd.DataFrame()

    for p in output_dfs:

        dates = pd.DataFrame()

        dates['dates'] = pd.date_range(start=output_dfs[p]['Creation_Date'].min(), end=output_dfs[p]['Creation_Date'].max(), freq="D")
        dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))

        x_orders = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Merchant_Reference':pd.Series.nunique})
        x_users = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'Email':pd.Series.nunique})
        # x_values = output_dfs[p].groupby(['Creation_Date_YM','First_Order_YM']).agg({'ValueNOVAT':pd.Series.sum})

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
        # x_values.reset_index(inplace=True)

        dates_od = dates['ym'].unique()
        dates_fo = output_dfs[p]['First_Order_YM'].unique()
        idx = pd.MultiIndex.from_product((dates_od,dates_fo), names=['Creation_Date_YM','First_Order_YM'])

        x_orders = x_orders.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
        x_users = x_users.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)
        # x_values = x_values.set_index(['Creation_Date_YM','First_Order_YM']).reindex(idx, fill_value=0)

        x_orders = x_orders.groupby(level=1).apply(cohort_period)
        x_users = x_users.groupby(level=1).apply(cohort_period)
        # x_values = x_values.groupby(level=1).apply(cohort_period)

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)
        # x_values.reset_index(inplace=True)

        x_orders.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        x_users.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)
        # x_values.set_index(['First_Order_YM', 'CohortPeriod'], inplace=True)

        ord_result = x_orders['Merchant_Reference'].unstack(1)
        cust_result = x_users['Email'].unstack(1)
        # value_result = x_values['ValueNOVAT'].unstack(1)

        cohort_orders = cohort_orders.append(ord_result)
        cohort_customers = cohort_customers.append(cust_result)
        # cohort_values = cohort_values.append(value_result)

    ret_rate = cohort_orders.divide(cohort_customers[0],axis=0)

    ret_rate[0] = ret_rate[0] - 1

    ret_rate = ret_rate.sort_values(by='First_Order_YM')

    # #Transaction Summary Calcualtion
    # trans_month = df.groupby(['Creation_Date_YM','Customer_Type']).agg({'Merchant_Reference':pd.Series.nunique, 'ValueNOVAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
    # trans_month.rename(columns={'Merchant_Reference':'All_Orders','ValueNOVAT':'All_Revenue'},inplace=True)

    #Transaction Calcualtion
    trans_month = df.groupby(['Creation_Date_YM','Customer_Type']).agg({'Merchant_Reference':pd.Series.nunique, 'Revenue':pd.Series.sum}).unstack(1).fillna(0).reset_index()
    trans_month.rename(columns={'Merchant_Reference':'All_Orders','Revenue':'All_Revenue'},inplace=True)
    trans_month.set_index('Creation_Date_YM', inplace=True)
    try:
        trans_month.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']
        trans_month['AOV_Ret'] = trans_month['Ret_Rev']/trans_month['Ret_Trans']
    except ValueError:
        trans_month.columns = ['new_trans','new_rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']

    return  ret_rate, trans_month
