# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
import datetime as dt
import glob
import mysql.connector
from mysql.connector import Error


# %%
try:
    connection = mysql.connector.connect(host='attribution-pc.ccjslhbmclgf.eu-central-1.rds.amazonaws.com',
                                         database='etl_site_traffic',
                                         user='fsg',
                                         password='Attribution3.0')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Your connected to database: ", record)
        historic = pd.read_sql('SELECT cb.Conv_ID, cb.Customer_ID, cb.Conv_Date, cb.Order_Num, cb.Revenue_excl_VAT as "Revenue", cb.Days_Since_Last FROM conversions_backend cb WHERE Account = "Paula" AND Market in  ("DE","AT")',con=connection)
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

# %% [markdown]
# # AOVs

# %%
historic.head()


# %%
historic = historic[historic['Revenue']>0.0]


# %%
historic['Conv_Date'] = pd.to_datetime(historic['Conv_Date'], format="%Y-%m-%d")


# %%
new_c = (historic['Order_Num']==0)
ret_c = (historic['Order_Num']>0)


# %%
historic['Customer_Type'] = ""


# %%
historic.loc[new_c,['Customer_Type']] = "New"
historic.loc[ret_c,['Customer_Type']] = "Returning"


# %%
historic_apr19 = historic[historic['Conv_Date']<= '2019-04-14']
historic_oct19 = historic[historic['Conv_Date']<= '2019-10-13']
historic_apr20 = historic[historic['Conv_Date']<= '2020-04-12']
historic_oct20 = historic[historic['Conv_Date']<= '2020-10-12']
historic_apr21 = historic[historic['Conv_Date']<= '2021-04-12']
historic_oct21 = historic[historic['Conv_Date']<= '2021-10-11']

# %%
time_diff_apr19 = historic_apr19.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()
time_diff_oct19 = historic_oct19.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()
time_diff_apr20 = historic_apr20.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()
time_diff_oct20 = historic_oct20.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()
time_diff_apr21 = historic_apr21.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()
time_diff_oct21 = historic_oct21.groupby('Customer_ID')['Conv_Date'].agg(['max','min']).reset_index()


# %%
time_diff_apr19['Tenure'] = (time_diff_apr19['max']-time_diff_apr19['min']).dt.days
time_diff_oct19['Tenure'] = (time_diff_oct19['max']-time_diff_oct19['min']).dt.days
time_diff_apr20['Tenure'] = (time_diff_apr20['max']-time_diff_apr20['min']).dt.days
time_diff_oct20['Tenure'] = (time_diff_oct20['max']-time_diff_oct20['min']).dt.days
time_diff_apr21['Tenure'] = (time_diff_apr21['max']-time_diff_apr21['min']).dt.days
time_diff_oct21['Tenure'] = (time_diff_oct21['max']-time_diff_oct21['min']).dt.days


# %%
time_diff_apr19 = time_diff_apr19.loc[:,['Customer_ID','Tenure']]
time_diff_oct19 = time_diff_oct19.loc[:,['Customer_ID','Tenure']]
time_diff_apr20 = time_diff_apr20.loc[:,['Customer_ID','Tenure']]
time_diff_oct20 = time_diff_oct20.loc[:,['Customer_ID','Tenure']]
time_diff_apr21 = time_diff_apr21.loc[:,['Customer_ID','Tenure']]
time_diff_oct21 = time_diff_oct21.loc[:,['Customer_ID','Tenure']]

# %%
historic_apr19 = historic_apr19.merge(time_diff_apr19, on='Customer_ID',how='inner')
historic_oct19 = historic_oct19.merge(time_diff_oct19, on='Customer_ID',how='inner')
historic_apr20 = historic_apr20.merge(time_diff_apr20, on='Customer_ID',how='inner')
historic_oct20 = historic_oct20.merge(time_diff_oct20, on='Customer_ID',how='inner')
historic_apr21 = historic_apr21.merge(time_diff_apr21, on='Customer_ID',how='inner')
historic_oct21 = historic_oct21.merge(time_diff_oct21, on='Customer_ID',how='inner')


# %%
historic_apr19['Tenure_Bucket'] = ""
historic_oct19['Tenure_Bucket'] = ""
historic_apr20['Tenure_Bucket'] = ""
historic_oct20['Tenure_Bucket'] = ""
historic_apr21['Tenure_Bucket'] = ""
historic_oct21['Tenure_Bucket'] = ""

# %%
def tenure_bucket(col):
    if (col <= 30) :
        return 'Less than 1 Month'
    elif ((col > 30) & (col <= 90)):
        return 'Between 1 and 3 Months'
    elif ((col > 90) & (col <= 180)):
        return 'Between 3 and 6 Months'    
    elif ((col > 180) & (col <= 365)):
        return 'Between 6 and 12 Months'
    else:
        return 'Over 12 Months'


# %%
historic_apr19.loc[:,['Tenure_Bucket']] = historic_apr19['Tenure'].apply(lambda x: tenure_bucket(x))
historic_oct19.loc[:,['Tenure_Bucket']] = historic_oct19['Tenure'].apply(lambda x: tenure_bucket(x))
historic_apr20.loc[:,['Tenure_Bucket']] = historic_apr20['Tenure'].apply(lambda x: tenure_bucket(x))
historic_oct20.loc[:,['Tenure_Bucket']] = historic_oct20['Tenure'].apply(lambda x: tenure_bucket(x))
historic_apr21.loc[:,['Tenure_Bucket']] = historic_apr21['Tenure'].apply(lambda x: tenure_bucket(x))
historic_oct21.loc[:,['Tenure_Bucket']] = historic_oct21['Tenure'].apply(lambda x: tenure_bucket(x))

# %%
gsw_apr19_all = historic_apr19[(historic_apr19['Conv_Date']>= '2019-04-06') & (historic_apr19['Conv_Date']<= '2019-04-14')]
gsw_oct19_all = historic_oct19[(historic_oct19['Conv_Date']>= '2019-10-05') & (historic_oct19['Conv_Date']<= '2019-10-13')]
gsw_apr20_all = historic_apr20[(historic_apr20['Conv_Date']>= '2020-04-03') & (historic_apr20['Conv_Date']<= '2020-04-12')]
gsw_oct20_all = historic_oct20[(historic_oct20['Conv_Date']>= '2020-10-02') & (historic_oct20['Conv_Date']<= '2020-10-11')]
gsw_apr21_all = historic_apr21[(historic_apr21['Conv_Date']>= '2021-04-03') & (historic_apr21['Conv_Date']<= '2021-04-12')]
gsw_oct21_all = historic_oct21[(historic_oct21['Conv_Date']>= '2021-10-02') & (historic_oct21['Conv_Date']<= '2021-10-10')]

# %%
gsw_apr19_ret = gsw_apr19_all[gsw_apr19_all['Customer_Type']=='Returning']
gsw_oct19_ret = gsw_oct19_all[gsw_oct19_all['Customer_Type']=='Returning']
gsw_apr20_ret = gsw_apr20_all[gsw_apr20_all['Customer_Type']=='Returning']
gsw_oct20_ret = gsw_oct20_all[gsw_oct20_all['Customer_Type']=='Returning']
gsw_apr21_ret = gsw_apr21_all[gsw_apr21_all['Customer_Type']=='Returning']
gsw_oct21_ret = gsw_oct21_all[gsw_oct21_all['Customer_Type']=='Returning']

# %%
gsw_apr19_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)
# this is to copy the table...

# %%
gsw_oct19_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)

# %%
gsw_apr20_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)

# %%
#gsw_oct20_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)

# %%
#gsw_apr21_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)

# %%
#gsw_oct21_ret.groupby('Tenure_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)

# %%
# New Customers


# %%
new_cust = historic[historic['Customer_Type']=='New']


# %%
gsw_apr19 = new_cust[(new_cust['Conv_Date']>= '2019-04-06') & (new_cust['Conv_Date']<= '2019-04-14')]
gsw_oct19 = new_cust[(new_cust['Conv_Date']>= '2019-10-05') & (new_cust['Conv_Date']<= '2019-10-13')]
gsw_apr20 = new_cust[(new_cust['Conv_Date']>= '2020-04-03') & (new_cust['Conv_Date']<= '2020-04-12')]
gsw_oct20 = new_cust[(new_cust['Conv_Date']>= '2020-10-02') & (new_cust['Conv_Date']<= '2020-10-11')]
gsw_apr21 = new_cust[(new_cust['Conv_Date']>= '2021-04-03') & (new_cust['Conv_Date']<= '2021-04-12')]
gsw_oct21 = new_cust[(new_cust['Conv_Date']>= '2021-10-02') & (new_cust['Conv_Date']<= '2021-10-10')]


# %%
gsw_apr19['AOV_Bucket'] = ""
gsw_oct19['AOV_Bucket'] = ""
gsw_apr20['AOV_Bucket'] = ""
gsw_oct20['AOV_Bucket'] = ""
gsw_apr21['AOV_Bucket'] = ""
gsw_oct21['AOV_Bucket'] = ""

# %%
def aov_bucket(col):
    if (col <= 20.0) :
        return '< 20'
    elif ((col > 20) & (col <= 40)):
        return '21 - 40'
    elif ((col > 40) & (col <= 60)):
        return '41 - 60'    
    elif ((col > 60) & (col <= 80)):
        return '61 - 80'
    elif ((col > 80) & (col <= 100)):
        return '81 - 100'
    else:
        return '> 100'


# %%
gsw_apr19.loc[:,['AOV_Bucket']] = gsw_apr19['Revenue'].apply(lambda x: aov_bucket(x))


# %%
gsw_oct19.loc[:,['AOV_Bucket']] = gsw_oct19['Revenue'].apply(lambda x: aov_bucket(x))


# %%
gsw_apr20.loc[:,['AOV_Bucket']] = gsw_apr20['Revenue'].apply(lambda x: aov_bucket(x))


# %%
gsw_apr19.groupby('AOV_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
gsw_oct19.groupby('AOV_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
gsw_apr20.groupby('AOV_Bucket').agg({'Conv_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
# gsw_apr19_all = historic[(historic['Conv_Date']>= '2019-04-06') & (historic['Conv_Date']<= '2019-04-14')]
# gsw_oct19_all = historic[(historic['Conv_Date']>= '2019-10-05') & (historic['Conv_Date']<= '2019-10-13')]
# gsw_apr20_all = historic[(historic['Conv_Date']>= '2020-04-03') & (historic['Conv_Date']<= '2020-04-12')]


# %%
# gsw_apr19_ret = gsw_apr19_all[gsw_apr19_all['Customer_Type']=='Returning']
# gsw_oct19_ret = gsw_oct19_all[gsw_oct19_all['Customer_Type']=='Returning']
# gsw_apr20_ret = gsw_apr20_all[gsw_apr20_all['Customer_Type']=='Returning']


# %%
gsw_apr19_ret.head()


# %%
rfm_apr19 = gsw_apr19_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()
rfm_oct19 = gsw_oct19_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()
rfm_apr20 = gsw_apr20_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()
rfm_oct20 = gsw_oct20_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()
rfm_apr21 = gsw_apr21_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()
rfm_oct21 = gsw_oct21_ret.groupby(['Customer_ID']).agg({'Order_Num':pd.Series.max,'Days_Since_Last':pd.Series.min,'Revenue':pd.Series.mean}).reset_index()

# %%
# Purchase Frequency
purchase_frequency_apr19=sum(rfm_apr19['Order_Num'])/rfm_apr19.shape[0]
purchase_frequency_oct19=sum(rfm_oct19['Order_Num'])/rfm_oct19.shape[0]
purchase_frequency_apr20=sum(rfm_apr20['Order_Num'])/rfm_apr20.shape[0]
purchase_frequency_oct20=sum(rfm_oct20['Order_Num'])/rfm_oct20.shape[0]
purchase_frequency_apr21=sum(rfm_apr21['Order_Num'])/rfm_apr21.shape[0]
purchase_frequency_oct21=sum(rfm_oct21['Order_Num'])/rfm_oct21.shape[0]

#%%
purchase_frequency_oct21

#%%
gsw_oct21_ret.head()

# %%
# Repeat Rate
repeat_rate_apr19=rfm_apr19[rfm_apr19.Order_Num > 1].shape[0]/rfm_apr19.shape[0]
repeat_rate_oct19=rfm_oct19[rfm_oct19.Order_Num > 1].shape[0]/rfm_oct19.shape[0]
repeat_rate_apr20=rfm_apr20[rfm_apr20.Order_Num > 1].shape[0]/rfm_apr20.shape[0]
repeat_rate_oct20=rfm_oct20[rfm_oct20.Order_Num > 1].shape[0]/rfm_oct20.shape[0]
repeat_rate_apr21=rfm_apr21[rfm_apr21.Order_Num > 1].shape[0]/rfm_apr21.shape[0]
repeat_rate_oct21=rfm_oct21[rfm_oct21.Order_Num > 1].shape[0]/rfm_oct21.shape[0]


# %%
# Churn Rate
churn_rate_apr19=1-repeat_rate_apr19
churn_rate_oct19=1-repeat_rate_oct19
churn_rate_apr20=1-repeat_rate_apr20
churn_rate_oct20=1-repeat_rate_oct20
churn_rate_apr21=1-repeat_rate_apr21
churn_rate_oct21=1-repeat_rate_oct21

# %%
# Customer Value
rfm_apr19['CLV']=(rfm_apr19['Revenue']*purchase_frequency_apr19)/churn_rate_apr19
rfm_oct19['CLV']=(rfm_oct19['Revenue']*purchase_frequency_oct19)/churn_rate_oct19
rfm_apr20['CLV']=(rfm_apr20['Revenue']*purchase_frequency_apr20)/churn_rate_apr20
rfm_oct20['CLV']=(rfm_oct20['Revenue']*purchase_frequency_oct20)/churn_rate_oct20
rfm_apr21['CLV']=(rfm_apr21['Revenue']*purchase_frequency_apr21)/churn_rate_apr21
rfm_oct21['CLV']=(rfm_oct21['Revenue']*purchase_frequency_oct21)/churn_rate_oct21

# %%
# Define scoring functions for segmentation

def recency_core(x,p,df):
    if x <= df[p][0.20]:
        return 5
    elif x <= df[p][0.4]:
        return 4
    elif x <= df[p][0.6]: 
        return 3
    elif x <= df[p][0.8]: 
        return 2
    else:
        return 1
    
def freq_monetary_score(x,p,df):
    if x <= df[p][0.20]:
        return 1
    elif x <= df[p][0.40]:
        return 2
    elif x <= df[p][0.60]: 
        return 3
    elif x <= df[p][0.80]: 
        return 4
    else:
        return 5


# %%
# Split metrics into segments is by using quartiles
quantiles_apr19 = rfm_apr19.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_apr19 = quantiles_apr19.to_dict()

quantiles_oct19 = rfm_oct19.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_oct19 = quantiles_oct19.to_dict()

quantiles_apr20 = rfm_apr20.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_apr20 = quantiles_apr20.to_dict()

quantiles_oct20 = rfm_oct20.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_oct20 = quantiles_oct20.to_dict()

quantiles_apr21 = rfm_apr21.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_apr21 = quantiles_apr21.to_dict()

quantiles_oct21 = rfm_oct21.quantile(q=[0.2,0.4,0.6,0.8])
quantiles_oct21 = quantiles_oct21.to_dict()

# %%
quantiles_apr19


# %%
quantiles_oct19


# %%
quantiles_apr20


# %%
# Apply the scoring functions on the segmentated table
rfm_apr19['R_quartile'] = rfm_apr19['Days_Since_Last'].apply(recency_core, args=('Days_Since_Last',quantiles_apr19,))
rfm_apr19['F_quartile'] = rfm_apr19['Order_Num'].apply(freq_monetary_score, args=('Order_Num',quantiles_apr19,))
rfm_apr19['M_quartile'] = rfm_apr19['Revenue'].apply(freq_monetary_score, args=('Revenue',quantiles_apr19,))

rfm_oct19['R_quartile'] = rfm_oct19['Days_Since_Last'].apply(recency_core, args=('Days_Since_Last',quantiles_oct19,))
rfm_oct19['F_quartile'] = rfm_oct19['Order_Num'].apply(freq_monetary_score, args=('Order_Num',quantiles_oct19,))
rfm_oct19['M_quartile'] = rfm_oct19['Revenue'].apply(freq_monetary_score, args=('Revenue',quantiles_oct19,))

rfm_apr20['R_quartile'] = rfm_apr20['Days_Since_Last'].apply(recency_core, args=('Days_Since_Last',quantiles_apr20,))
rfm_apr20['F_quartile'] = rfm_apr20['Order_Num'].apply(freq_monetary_score, args=('Order_Num',quantiles_apr20,))
rfm_apr20['M_quartile'] = rfm_apr20['Revenue'].apply(freq_monetary_score, args=('Revenue',quantiles_apr20,))


# %%
# Create the RFMScore column by concatenating the individual scores
rfm_apr19['RFMScore'] = rfm_apr19.R_quartile.map(str) + rfm_apr19.F_quartile.map(str) + rfm_apr19.M_quartile.map(str)
rfm_oct19['RFMScore'] = rfm_oct19.R_quartile.map(str) + rfm_oct19.F_quartile.map(str) + rfm_oct19.M_quartile.map(str)
rfm_apr20['RFMScore'] = rfm_apr20.R_quartile.map(str) + rfm_apr20.F_quartile.map(str) + rfm_apr20.M_quartile.map(str)


# %%
# Calculate the average to define the customer segmentation
rfm_apr19['RFM_AVG'] = rfm_apr19[['R_quartile','F_quartile','M_quartile']].mean(axis=1)
rfm_oct19['RFM_AVG'] = rfm_oct19[['R_quartile','F_quartile','M_quartile']].mean(axis=1)
rfm_apr20['RFM_AVG'] = rfm_apr20[['R_quartile','F_quartile','M_quartile']].mean(axis=1)


# %%
# Define the labeling funtion
def customer_label(col):
    col = str(col)
    if col in ['555','554','544','545','454','455','445']:
        return 'Champions'
    if col in ['543','444','435','355','354','345','344','335']:
        return 'Loyal'
    if col in ['553','551','552','541','542','533','532','531','452','451','442','441','431','453','433','432','423','353','352','351','342','341','333','323']:
        return 'Potential Loyalist'
    if col in ['512','511','422','421', '412','411','311']:
        return 'Second/Third Time Buyers'
    if col in ['525','524','523','522','521','515','514','513','425','424','413','414','415','315','314','313']:
        return 'Promising'
    if col in ['535','534','443','434','343','334','325','324']:
        return 'Need Attention'
    if col in ['331','321','312','221','213','231','241','251']:
        return 'About To Sleep'
    if col in ['255','254','245','244','253','252','243','242','235','234','225','224','153','152','145','143','142','135','134','133','125','124']:
        return 'At Risk'
    if col in ['155','154','144','214','215','115','114','113']:
        return 'Cannot Lose Them'
    if col in ['332','322','231','241','251','233','232','223','222','132','123','122','212','211']:
        return 'Hibernating customers'
    if col in ['111','112','121','131','141','151']:
        return 'Lost'
    else:
        return 'What Happened?'


# %%
# Create the CustomerLabel column by applying the customer_label function
rfm_apr19['CustomerLabel'] = rfm_apr19['RFMScore'].apply(customer_label)
rfm_oct19['CustomerLabel'] = rfm_oct19['RFMScore'].apply(customer_label)
rfm_apr20['CustomerLabel'] = rfm_apr20['RFMScore'].apply(customer_label)


# %%
rfm_apr19.groupby('CustomerLabel').agg({'Customer_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
rfm_oct19.groupby('CustomerLabel').agg({'Customer_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
rfm_apr20.groupby('CustomerLabel').agg({'Customer_ID':pd.Series.nunique}).reset_index().to_clipboard(index=False)


# %%
historic.head()

# %% [markdown]
# # BHA

# %%
data = pd.read_csv('./Data/Netsuite_Backend.csv')


# %%
data.head()


# %%
data['Date'] = pd.to_datetime(data['Date'], format="%d/%m/%Y")


# %%



