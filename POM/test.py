#%%
test = df_og[df_og['Billing Country'] == 'DE']
test.shape

#%%
test = test.loc[:,['Name', 'Email', 'Financial Status', 'Created at', 'Currency', 'Subtotal', 'Shipping', 'Taxes', 'Total', 'Discount Code', 'Discount Amount', 'Lineitem name', 'Lineitem sku', 'Billing Name']]

#%%
test.rename(columns={'Name':'Order_ID','Financial Status':'Status' ,'Created at':'Creation_Date', 'Lineitem name':'Product Name', 'Lineitem name':'Product_ID', 'Billing Name': 'Customer_Name'}, inplace=True)

#%%
test['Creation_Date'] = pd.to_datetime(test['Creation_Date'], errors='coerce')

#%%
#test = test[~test['Status'].isin(['Cancelled'])]
#test = test.fiilna(0)
#%%
test.set_index('Order_ID', inplace=True)

#%%
#test.Customer_Name.isnull().sum()
df_og['Paid at'].isnull().sum()

#%%
test['First_Order'] = test.groupby(level=0)['Creation_Date'].min()
test['First_Order_YM'] = test.groupby(level=0)['Creation_Date'].min().apply(lambda x: x.strftime('%Y-%m'))

#%%
test.reset_index(inplace=True)
test['Creation_Date_YM'] = test['Creation_Date'].apply(lambda x: x.strftime('%Y-%m'))
first_orders = test.sort_values('Creation_Date').groupby('Email')['Order_ID'].first().values
test['Customer_Type'] = np.where(test['Order_ID'].isin(first_orders),'New','Returning')

#%%
test['Creation_Date'] = pd.to_datetime(test['Creation_Date'], errors='ignore', utc=True)
test['Year'] = test['Creation_Date'].dt.year


#%%
test['Week'] = test['Creation_Date'].dt.isocalendar().week
test['Year_Week'] = test['Creation_Date'].dt.strftime("%Y-%W")

#%%
test = test.astype({'Subtotal':'float64', 'Shipping':'float64', 'Taxes':'float64', 'Total':'float64', 'Discount Amount':'float64'})
test['ValueNOVAT'] = test.Total - test.Taxes
test




#%%
test = test[~test['Status'].isin(['Cancelled'])]
test.set_index('Order_ID', inplace=True)
test['First_Order'] = test.groupby(level=0)['Creation_Date'].min()
test['First_Order_YM'] = test.groupby(level=0)['Creation_Date'].min().apply(lambda x: x.strftime('%Y-%m'))
test.reset_index(inplace=True)
test['Creation_Date_YM'] = test['Creation_Date'].apply(lambda x: x.strftime('%Y-%m'))
first_orders = test.sort_values('Creation_Date').groupby('E-mail')['Order'].first().values
test['Customer_Type'] = np.where(test['Order'].isin(first_orders),'New','Returning')
test['Payment_Type'] = np.where(test['Payment'] == 'Payment Amazon','Amazon','Webshop')
test['Year'] = test['Creation_Date'].dt.year
test['Week'] = test['Creation_Date'].dt.week
test['Year_Week'] = test['Creation_Date'].dt.strftime("%Y-%W")
