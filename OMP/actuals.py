#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pandas as pd
import numpy as np
import datetime as dt
import glob
import mysql.connector
from mysql.connector import Error
import gspread


# In[2]:
# ### Data Load
query_analytics = 'SELECT * FROM api_analytics.google_analytics_split ap WHERE Account = "OMP" AND Market = "DE"'
query_adwords = 'SELECT aaas.Observation, aac.Name as "campaign_name", SUM(aaas.Impr) AS "impressions", SUM(aaas.Clicks) AS "clicks", SUM(aaas.Cost) AS "cost" FROM api_adwords.api_adwords_ag_stats aaas JOIN api_adwords.api_adwords_adgroups aaa on aaa.ID = aaas.Adgroup_ID JOIN api_adwords.api_adwords_campaigns aac on aac.ID = aaa.Campaign_ID JOIN api_adwords.api_adwords_accounts aaa2 on aaa2.ID = aaa.Account_ID WHERE aaa2.ID = "3171662987" GROUP BY 1,2;'
query_facebook = 'SELECT * FROM api_facebook.campaign_report WHERE Account_ID = "66828992" AND Market = "DE"'
query_ihc = 'SELECT * FROM channels_attribution WHERE Account = "OMP" AND Market = "DE"'


# In[3]:
try:
    connection = mysql.connector.connect(host='attribution-system-fsg-new.cob86lv75rzo.eu-west-1.rds.amazonaws.com',
                                         database='attribution_dashboard_omp',
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
        df_ihc = pd.read_sql(query_ihc,con=connection)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


# In[51]:
#df_ihc.head()
#df_ihc.columns.unique()

# In[4]:
# ### Transformations
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

# In[52]:
df_ihc.columns = df_ihc.columns.str.lower()
df_ihc['observation'] = pd.to_datetime(df_ihc['observation'])
df_ihc['year_month'] = df_ihc.observation.dt.strftime("%Y-%m")
df_ihc.head()

# In[53]:
#df_ihc.year_month.unique()

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


# In[9]:
# ### Aggregations
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


# In[53]:
ihc_summary = df_ihc.groupby(['year_month','channel']).agg({

    'ihc_rev':pd.Series.sum,
    'ihc_conv':pd.Series.sum,
    'ihc_conv_new':pd.Series.sum,
    'ihc_rev_new':pd.Series.sum,
    'aff_conv':pd.Series.sum,
    'aff_conv_new':pd.Series.sum,

}).reset_index()
# In[12]:
# ### Reindexing
ym_idx = ihc_summary.year_month.unique()
channel_idx = ihc_summary.channel.unique()

# In[13]:
join_idx = pd.MultiIndex.from_product([ym_idx,channel_idx], names = ['year_month','channel'])
join_idx
# In[14]:

ihc_summary = ihc_summary.set_index(['year_month','channel'])
ga_summary = ga_summary.set_index(['year_month','channel'])
aw_summary = aw_summary.reindex(join_idx,fill_value=0)
fb_summary = fb_summary.reindex(join_idx,fill_value=0)

# In[]:
#ihc_summary = ihc_summary.reindex(join_idx,fill_value=0)

# In[]:
#ihc_summary.year_month
#ga_summary.max()
# In[141]:
#ihc_summary.head()
# In[142]:
#fb_summary.head()
# In[143]:
#ga_summary.head()
# In[141]:
#aw_summary.head()


# In[15]:
dataframes = [ga_summary, aw_summary, fb_summary, ihc_summary]

# In[16]:
from functools import reduce
combined_summary = reduce(lambda left,right: pd.merge(left, right, on = ['year_month','channel'], how='outer'), dataframes).fillna(0)


# In[17]:
combined_summary['total_impression'] = combined_summary['impressions_aw'] + combined_summary['impressions_fb']
combined_summary['total_clicks'] = combined_summary['clicks_aw'] + combined_summary['clicks_fb']
combined_summary['total_costs'] = combined_summary['cost_aw'] + combined_summary['cost_fb']
combined_summary['bounce_rate'] = np.divide(combined_summary['bounces_ga'],combined_summary['sessions_ga']).replace(np.inf, 0)*100

combined_summary = combined_summary.fillna(0)

combined_summary = combined_summary.astype(str)

combined_summary = combined_summary.applymap(lambda x: str(x.replace('.',',')))

#combined_summary = combined_summary.reset_index(drop=True)
combined_summary = combined_summary.reset_index()


# In[18]:
combined_summary.head()
#ihc_summary


# In[18]:
# ### Data Export
import gspread
#from gspread_pandas import Spread, Client
#omp_sheet = Spread('1FUeI8nL8Wl7OlIoEelPuaGyO4RqWBFnUdmM4zYFfalw')
#omp_sheet.df_to_sheet(combined_summary, index=False, sheet='ACTUALS_DUMP', start='A1', replace=True)

gc = gspread.service_account()
sht1 = gc.open_by_key('1FUeI8nL8Wl7OlIoEelPuaGyO4RqWBFnUdmM4zYFfalw')
wrksht= sht1.worksheet("ACTUALS_DUMP")
#worksheet.update('B1', 'Bingo!')

# In[19]:
wrksht.update([combined_summary.columns.values.tolist()] + combined_summary.values.tolist())
# %%
