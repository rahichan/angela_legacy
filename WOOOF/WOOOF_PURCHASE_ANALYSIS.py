# %%
import numpy as np
import pandas as pd
import datetime as dt
import cohorts_pipeline_woof_v3
# %%
df_og = pd.read_csv('./Data/orders.csv', sep=';', decimal=',')
# %%
df = cohorts_pipeline_woof_v3.df_cleaning(df_og)
# %%
df.info()
# %%
temp0 = df.groupby(['E-mail','Creation_Date']).agg({'Order_ID':pd.Series.count})
temp0 = temp0.groupby(level=0).apply(cohorts_pipeline_woof_v3.purchase_number)
temp0.reset_index(inplace=True)
temp0.drop('Order_ID',axis=1,inplace=True)
temp1 = df.groupby('E-mail').agg({'Order_ID':pd.Series.nunique})
temp1.rename(columns={'Order_ID':'Total_Purchases'},inplace=True)
temp1.reset_index(inplace=True)
purchase_table = temp0.merge(temp1, on='E-mail',how='left')
purchase_table['Days_Between'] = purchase_table.groupby('E-mail')['Creation_Date'].diff().apply(lambda x: x.days)
# %%
purchase_table['Days_Between'] = purchase_table['Days_Between'].fillna(0)
# %%
purchase_table.head()
# %%
days_between_table = purchase_table.groupby(['Purchase_Number']).agg({'Days_Between': [pd.Series.mean, pd.Series.mode]}).reset_index()
# %%
days_between_table.columns = ['Purchase_Number','Avg_Days_Between','Most_Common_Days_Between']
# %%
days_between_table = days_between_table[days_between_table.Purchase_Number<=20]
#%%
days_between_table = days_between_table.astype(int)
#%%
days_between_table.to_clipboard(index=False)
#%%
purchase_table[(purchase_table.Purchase_Number<=20)].groupby(['Purchase_Number'])['Days_Between'].describe().to_clipboard( decimal=',')


# %%
