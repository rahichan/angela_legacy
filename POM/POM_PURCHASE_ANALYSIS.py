# %%
import numpy as np
import pandas as pd
import datetime as dt
import cohorts_pipeline_pom_v3
# %%
df_og = pd.read_csv('./Data/orders_export_14102021.csv', sep=',', decimal=',')
#pd.read_csv('./Data/orders_export_1.csv', sep=',', decimal=',')
# %%
df = cohorts_pipeline_pom_v3.df_cleaning(df_og)
# %%
df.info()
# %%
temp0 = df.groupby(['Email','Creation_Date']).agg({'Order_ID':pd.Series.count})
temp0 = temp0.groupby(level=0).apply(cohorts_pipeline_pom_v3.cohort_period)
temp0.reset_index(inplace=True)
temp0.drop('Order_ID',axis=1,inplace=True)
temp1 = df.groupby('Email').agg({'Order_ID':pd.Series.nunique})
temp1.rename(columns={'Order_ID':'Total_Purchases'},inplace=True)
temp1.reset_index(inplace=True)
purchase_table = temp0.merge(temp1, on='Email',how='left')
purchase_table['Days_Between'] = purchase_table.groupby('Email')['Creation_Date'].diff().apply(lambda x: x.days)
# %%
purchase_table['Days_Between'] = purchase_table['Days_Between'].fillna(0)
# %%
purchase_table.head()
# %%
#days_between_table = purchase_table.groupby(['Purchase_Number']).agg({'Days_Between': [pd.Series.mean, pd.Series.mode]}).reset_index()

days_between_table = purchase_table.groupby(['CohortPeriod']).agg({'Days_Between': [pd.Series.mean, pd.Series.mode]}).reset_index()
# %%

days_between_table.columns = ['Purchase_Number','Avg_Days_Between','Most_Common_Days_Between']
# %%
days_between_table = days_between_table[days_between_table.Purchase_Number<=20]

#%%
days_between_table['Most_Common_Days_Between'] = days_between_table['Most_Common_Days_Between'].apply(lambda x: np.mean(x))

#%%
days_between_table = days_between_table.astype(int)
#%%
days_between_table.to_clipboard(decimal=',', index=False)
#%%
#purchase_table[(purchase_table.Purchase_Number<=20)].groupby(['Purchase_Number'])['Days_Between'].describe().to_clipboard( decimal=',')
purchase_table[(purchase_table.CohortPeriod<=20)].groupby(['CohortPeriod'])['Days_Between'].describe().to_clipboard( decimal=',')

# %%
np.mean(days_between_table['Most_Common_Days_Between'][1])
# %%
#%%
df['Observation'] = df['Creation_Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
df.head(2)
# %%
ret_df = df[['Observation', 'Creation_Date', 'Email', 'Customer_Type', 'Order_ID', 'ValueNOVAT']]

#%%
ret_df = pd.merge(ret_df, purchase_table)
ret_df
#%%
ret_purch = ret_df[ret_df['Observation']>= "2021-03-01"][ret_df['Observation']< "2021-04-12"][ret_df['Customer_Type'] == "Returning"].groupby(['CohortPeriod']).agg({'Order_ID': [pd.Series.count]}).reset_index()
ret_purch

#%%
ret_purch.to_clipboard()


#%%
summary = ret_df[ret_df['Observation']>= "2021-03-01"][ret_df['Observation']< "2021-04-12"].groupby(['Observation','Customer_Type']).agg({'Order_ID':pd.Series.nunique, 'ValueNOVAT':pd.Series.sum}).unstack(1).fillna(0).reset_index()
summary.set_index('Observation', inplace=True)
summary.columns = ['New_Trans','Ret_Trans','New_Rev','Ret_Rev']
summary['Revenue_EXVAT'] = summary['New_Rev'] + summary['Ret_Rev']
summary['Transactions'] = summary['New_Trans'] + summary['Ret_Trans']
gsw_2021_q1_df = summary[['Revenue_EXVAT', 'Transactions', 'New_Rev', 'New_Trans', 'Ret_Rev', 'Ret_Trans']]