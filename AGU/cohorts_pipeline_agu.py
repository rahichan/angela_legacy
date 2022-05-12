import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None

# Clean dataset and add first order column
def first_order(df):

    df = df.iloc[:,[0,2,3,5,6,7,11,13,14]]
    df.columns = ['order_id','date','name','total_base','total_purchased','status','email','subtotal','shipping']
    df =  df[df.status.isin(['Processing','Closed','Complete'])]
    df['date'] = pd.to_datetime(df['date'])

    df['order_date'] = df['date'].dt.date
    df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')

    df = df.sort_values(by='date')

    df.set_index('email', inplace=True)
    df['first_order'] = df.groupby(level=0)['date'].min()
    df['first_order_ym'] = df.groupby(level=0)['date'].min().apply(lambda x: x.strftime('%Y-%m'))
    df.reset_index(inplace=True)
    df['date_ym'] = df['date'].apply(lambda x: x.strftime('%Y-%m'))

    first_orders = df.sort_values('date').groupby('email')['order_id'].first().values

    df['customer_type'] = np.where(df['order_id'].isin(first_orders),'New','Returning')
    df['days_between'] = df.groupby('email')['date'].diff().apply(lambda x: x.days)
    df['days_between'].fillna(0,inplace=True)

    return df

# Function to add the cohort period
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
    df['cohort_period'] = np.arange(len(df))
    return df

# Function to add the purchase number of each client/transactions
def purchase_number(df):
    """
    Creates a `CohortPeriod` column, which is the Nth period based on the user's first purchase.

    Example
    -------
    Say you want to get the 3rd month for every user:
        df.sort(['UserId', 'OrderTime', inplace=True)
        df = df.groupby('UserId').apply(cohort_period)
        df[df.CohortPeriod == 3]
    """
    df['purchase_number'] = np.arange(len(df)) + 1
    return df

# Function to create the cohorts
def cohorts_pipeline(df):
    """
    Creates the returning rates and transactions tables

    """
    months = df['first_order_ym'].unique()
    output_dfs = {p: df[df['first_order_ym'] == p] for p in months}
    cohort_orders = pd.DataFrame()
    cohort_customers = pd.DataFrame()

    for p in output_dfs:

        dates = pd.DataFrame()

        dates['dates'] = pd.date_range(start=output_dfs[p]['order_date'].min(), end=output_dfs[p]['order_date'].max(), freq="D")
        dates['ym'] = dates['dates'].apply(lambda x: x.strftime('%Y-%m'))

        x_orders = output_dfs[p].groupby(['date_ym','first_order_ym']).agg({'order_id':pd.Series.nunique})
        x_users = output_dfs[p].groupby(['date_ym','first_order_ym']).agg({'email':pd.Series.nunique})

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)

        dates_od = dates['ym'].unique()
        dates_fo = output_dfs[p]['first_order_ym'].unique()
        idx = pd.MultiIndex.from_product((dates_od,dates_fo), names=['date_ym','first_order_ym'])

        x_orders = x_orders.set_index(['date_ym','first_order_ym']).reindex(idx, fill_value=0)
        x_users = x_users.set_index(['date_ym','first_order_ym']).reindex(idx, fill_value=0)

        x_orders = x_orders.groupby(level=1).apply(cohort_period)
        x_users = x_users.groupby(level=1).apply(cohort_period)

        x_orders.reset_index(inplace=True)
        x_users.reset_index(inplace=True)

        x_orders.set_index(['first_order_ym', 'cohort_period'], inplace=True)
        x_users.set_index(['first_order_ym', 'cohort_period'], inplace=True)

        ord_result = x_orders['order_id'].unstack(1)
        cust_result = x_users['email'].unstack(1)

        cohort_orders = cohort_orders.append(ord_result)
        cohort_customers = cohort_customers.append(cust_result)

    ret_rate = cohort_orders.divide(cohort_customers[0],axis=0)
    ret_rate[0] = ret_rate[0] - 1
    ret_rate = ret_rate.sort_values(by='first_order_ym')

    #Transaction Summary Calcualtion
    trans_summary =  pd.DataFrame()
    trans_month = df.groupby(['date_ym','customer_type']).agg({'order_id':pd.Series.nunique, 'subtotal':pd.Series.sum}).unstack(1).fillna(0).reset_index()
    trans_month.set_index('date_ym', inplace=True)
    try:
        trans_month.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']
        trans_month['AOV_Ret'] = trans_month['Ret_Rev']/trans_month['Ret_Trans']
    except ValueError:
        trans_month.columns = ['New_Trans','New_Rev']
        trans_month['AOV_New'] = trans_month['New_Rev']/trans_month['New_Trans']

    return  trans_month, ret_rate
