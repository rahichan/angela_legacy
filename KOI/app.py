#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import glob
import mysql.connector
from mysql.connector import Error


# ### Data Load

# In[2]:


#query_analytics = 'SELECT * FROM analytics_perf ap WHERE Account = "KOI"'
query_analytics = 'SELECT * FROM google_analytics_split ap WHERE Account = "KOI"'
query_adwords = 'SELECT aaas.Observation, aac.Name as "campaign_name", SUM(aaas.Impr) AS "impressions", SUM(aaas.Clicks) AS "clicks", SUM(aaas.Cost) AS "cost" FROM api_adwords.api_adwords_ag_stats aaas JOIN api_adwords.api_adwords_adgroups aaa on aaa.ID = aaas.Adgroup_ID JOIN api_adwords.api_adwords_campaigns aac on aac.ID = aaa.Campaign_ID JOIN api_adwords.api_adwords_accounts aaa2 on aaa2.ID = aaa.Account_ID WHERE aaa2.ID = "4957305842" GROUP BY 1,2;'
query_facebook = 'SELECT * FROM api_facebook.campaign_report WHERE Account_ID = "465444381281126"'


# In[3]:


try:
    connection = mysql.connector.connect(host='attribution-system-fsg-new.cob86lv75rzo.eu-west-1.rds.amazonaws.com',
                                         database='api_analytics',
                                         user='fsg',
                                         password='Attribution3.0')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Your connected to database: ", record)
        df_ga = pd.read_sql(query_analytics,con=connection)
        df_aw = pd.read_sql(query_adwords,con=connection)
        df_fb = pd.read_sql(query_facebook,con=connection)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


# ### Transformations

# In[4]:


def channel_fb(col):
    if ('| 01 |' not in col) & ('Post:' not in col) & ('Beitrag:' not in col):
        return "Social Paid"
    elif '| 01 |' in col:
        return "Social Remarketing"
    elif ('Post:' in col) | ('Beitrag:' in col):
        return "Social Organic"
    else:
        return "Unknown"
    
def channel_aw(col):
    if ('Brand' in col) | ('Generic Produkte' in col) & ('NB' not in col) & ('GSC' not in col):
        return "SEA - Brand"
    elif ('Brand' not in col) & ('NB' in col) & ('GSC' not in col):
        return "SEA - Non Brand"    
    elif ('Brand' in col) & ('NB' not in col) & ('GSC' in col):
        return "Shopping - Brand"   
    elif ('Brand' not in col) & ('NB' in col) & ('GSC' in col):
        return "Shopping - Non Brand"  
    elif 'Display' in col:
        return "Display"
    elif 'Remarketing' in col:
        return "Google Remarketing"
    elif 'YT' in col:
        return "Video Marketing"
    else:
        return "Unknown"
        


# In[ ]:


"""
df_ga.columns = df_ga.columns.str.lower()
df_ga['observation'] = pd.to_datetime(df_ga['observation'])
df_ga.channelgrouping = df_ga.channelgrouping.str.replace('SEA - Non-Brand\t','SEA - Non Brand')
df_ga.channelgrouping = df_ga.channelgrouping.str.replace('SEA - Non-Brand','SEA - Non Brand')
df_ga.channelgrouping = df_ga.channelgrouping.str.replace('Shopping - Non-Brand','Shopping - Non Brand')
df_ga.channelgrouping = df_ga.channelgrouping.str.replace('(','').str.replace(')','').str.replace('Other','Unknown')
df_ga.channelgrouping = df_ga.channelgrouping.str.replace('Video Paid','Video Marketing')
df_ga['year_month'] = df_ga.observation.dt.strftime("%Y-%m")
df_ga = df_ga.rename(columns={'channelgrouping':'channel'})
"""


# In[5]:


df_ga.columns = df_ga.columns.str.lower()
df_ga['observation'] = pd.to_datetime(df_ga['observation'])
df_ga.channel = df_ga.channel.str.replace('SEA - Non-Brand\t','SEA - Non Brand')
df_ga.channel = df_ga.channel.str.replace('SEA - Non-Brand','SEA - Non Brand')
df_ga.channel = df_ga.channel.str.replace('Shopping - Non-Brand','Shopping - Non Brand')
df_ga.channel = df_ga.channel.str.replace('(','').str.replace(')','').str.replace('Other','Unknown')
df_ga.channel = df_ga.channel.str.replace('Video Paid','Video Marketing')
df_ga['year_month'] = df_ga.observation.dt.strftime("%Y-%m")
#df_ga = df_ga.rename(columns={'channelgrouping':'channel'})




# In[7]:


# Adwords Transformation
df_aw.columns = df_aw.columns.str.lower()
df_aw['observation'] = pd.to_datetime(df_aw['observation'])
df_aw['channel'] = df_aw['campaign_name'].apply(channel_aw)
df_aw['year_month'] = df_aw.observation.dt.strftime("%Y-%m")


# In[8]:


# Facebook Transformation
df_fb.columns = df_fb.columns.str.lower()
df_fb['observation'] = pd.to_datetime(df_fb['observation'])
df_fb['channel'] = df_fb['campaign_name'].apply(channel_fb)
df_fb['year_month'] = df_fb.observation.dt.strftime("%Y-%m")
df_fb = df_fb.rename(columns={'impr':'impressions'})


# ### Aggregations

# In[ ]:


"""ga_summary = df_ga.groupby(['year_month','channel']).agg({
    
    'transactionrevenue':pd.Series.sum,
    'sessions':pd.Series.sum,
    'sessionduration':pd.Series.mean,
    'transactions':pd.Series.sum,
    'productaddstocart':pd.Series.sum,
    'bounces':pd.Series.sum, 
    'pageviews':pd.Series.sum,
    'newusers':pd.Series.sum,
    'goal1completions':pd.Series.sum,
    'goal2completions':pd.Series.sum, 
    'goal3completions':pd.Series.sum,
    'goal4completions':pd.Series.sum, 
    'goal5completions':pd.Series.sum,
    'goal6completions':pd.Series.sum,
    'goal7completions':pd.Series.sum,
    'goal8completions':pd.Series.sum,
    'goal9completions':pd.Series.sum,
    'goal10completions':pd.Series.sum, 
    'goal11completions':pd.Series.sum,
    'goal12completions':pd.Series.sum, 
    'goal13completions':pd.Series.sum,
    'goal14completions':pd.Series.sum,
    'goal15completions':pd.Series.sum,
    'goal16completions':pd.Series.sum,
    'goal17completions':pd.Series.sum, 
    'goal19completions':pd.Series.sum,
    'goal20completions':pd.Series.sum
    
}).add_suffix('_ga').reset_index()
"""


# In[9]:


ga_summary = df_ga.groupby(['year_month','channel']).agg({
    
    'revenue':pd.Series.sum,
    'sessions':pd.Series.sum,
    'session_duration':pd.Series.mean,
    'transactions':pd.Series.sum,
    'bounces':pd.Series.sum, 
    'pageviews':pd.Series.sum,
    'new_users':pd.Series.sum,
    'goal_4_pages_views':pd.Series.sum,
    'users':pd.Series.sum
    
}).add_suffix('_ga').reset_index()


# In[10]:


aw_summary = df_aw.groupby(['year_month','channel']).agg({
    
    'impressions':pd.Series.sum,
    'clicks':pd.Series.sum,
    'cost':pd.Series.sum
    
}).add_suffix('_aw')


# In[11]:


fb_summary = df_fb.groupby(['year_month','channel']).agg({
    
    'impressions':pd.Series.sum,
    'reach':pd.Series.sum,
    'frequency':pd.Series.mean,
    'page_likes':pd.Series.sum,
    'clicks':pd.Series.sum,
    'website_clicks':pd.Series.sum, 
    'cost':pd.Series.sum,
    'fb_conv':pd.Series.sum,
    'total_revenue':pd.Series.sum,
    'website_revenue':pd.Series.sum, 
    'unique_clicks':pd.Series.sum,
    'add_to_cart':pd.Series.sum, 
    'post_engagement':pd.Series.sum,
    'landing_page_views':pd.Series.sum
    
}).add_suffix('_fb')


# ### Reindexing

# In[12]:


ym_idx = ga_summary.year_month.unique()
channel_idx = ga_summary.channel.unique()


# In[13]:


join_idx = pd.MultiIndex.from_product([ym_idx,channel_idx], names = ['year_month','channel'])


# In[14]:


ga_summary = ga_summary.set_index(['year_month','channel'])
aw_summary = aw_summary.reindex(join_idx,fill_value=0)
fb_summary = fb_summary.reindex(join_idx,fill_value=0)


# In[15]:


dataframes = [ga_summary, aw_summary, fb_summary]


# In[16]:


from functools import reduce
combined_summary = reduce(lambda left,right: pd.merge(left, right, on = ['year_month','channel'], how='outer'), dataframes).fillna(0)


# In[17]:


combined_summary['total_impression'] = combined_summary['impressions_aw'] + combined_summary['impressions_fb']
combined_summary['total_clicks'] = combined_summary['clicks_aw'] + combined_summary['clicks_fb']
combined_summary['total_costs'] = combined_summary['cost_aw'] + combined_summary['cost_fb']
combined_summary['bounce_rate'] = np.divide(combined_summary['bounces_ga'],combined_summary['sessions_ga']).replace(np.inf, 0)*100


# In[18]:


combined_summary = combined_summary.astype(str)


# In[19]:


combined_summary = combined_summary.applymap(lambda x: str(x.replace('.',',')))


# In[20]:


combined_summary = combined_summary.reset_index()


# ### Data Export

# In[21]:


from gspread_pandas import Spread, Client


# In[23]:


koi_sheet = Spread('1C4Q_8tXrSfyOcf_dX-Pt_pH6O740gTGRcMpbVTFMFEg')


# In[24]:


koi_sheet.df_to_sheet(combined_summary, index=False, sheet='ACTUALS_DUMP', start='A1', replace=True)


# In[ ]:




