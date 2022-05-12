#%%
import pandas as pd
#data_2021 = pd.read_csv('./statistics_from_2021-01-01_to_2021-12-31.csv', sep=',',index_col=False )
data_2021 = pd.read_csv('./data_2021.csv', sep=',',index_col=False )
data_2021.head()
# %%
# %%
data_2021.shape
# %%
oct_dec = data_2021[data_2021['Date']>= "2021-10-01"][data_2021['Date']<"2021-12-29"]
oct_dec.Date.min()
# %%
oct_dec.Date.max()
# %%
oct_dec.Date.unique()
# %%
data_2021['Date'] = pd.to_datetime(data_2021['Date'])
data_2021['year_month'] = data_2021.Date.dt.strftime("%Y-%m")
data_2021.year_month.unique()
# %%
data_2021.Date[0]
# %%
oct_dec.shape
# %%
oct_dec.Sales.sum()
# %%
oct_dec.groupby(by=['year_month']).agg({'Sales':'sum','Order value':'sum', 'Clicks':'sum'})

# %%
oct_dec['Order value'].sum()
# %%
data_2022 = pd.read_csv('./data_2022.csv', sep=',',index_col=False )
data_2022.head()
# %%
data_2022['Date'] = pd.to_datetime(data_2022['Date'])
data_2022['year_month'] = data_2022.Date.dt.strftime("%Y-%m")
data_2022.year_month.unique()
# %%
data_2022.max()
#%%
data_2022.shape
# %%
data_2022.Date.max()
# %%
data_2022.groupby(by=['year_month']).agg({'Sales':'sum','Order value':'sum', 'Clicks':'sum'})
# %%
jan_mar = data_2022[data_2022['Date']>= "2022-01-01"][data_2022['Date']<"2022-03-22"]
# %%
jan_mar.groupby(by=['year_month']).agg({'Sales':'sum','Order value':'sum', 'Clicks':'sum'})
# %%
