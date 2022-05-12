# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 16:23:08 2020

@author: Adrian
"""
import pandas as pd
import numpy as np
import gspread
from gspread_pandas import Spread, Client

def convert_fill(data):
    values = data.get_all_values()
    headers = values.pop(0)
    df = pd.DataFrame(values, columns=headers)
    df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y")
    cols = df.iloc[:,2:].columns
    for col in cols:
        df[col] = df[col].str.replace('.','').str.replace(',','.').apply(pd.to_numeric, downcast='integer', errors='coerce').fillna(0)
        df[col] = df[col].astype(int)
    return df

def convert_fill_business(data):
    values = data.get_all_values()
    headers = values.pop(0)
    df = pd.DataFrame(values, columns=headers)
    return df

def convert_fill_creditcards(data):
    values = data.get_all_values()
    headers=['Date','Tx_Reference','Amount','Description','Comment']
    df = pd.DataFrame(values, columns=headers)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Amount'] = df['Amount'].str.replace('.','').str.replace(',','.').str.replace(' ','').apply(pd.to_numeric, downcast='integer', errors='coerce').fillna(0)
    return df

def export_converter(df, cols):
    for col in cols:
        df[col] = df[col].astype(int)
    return df


# Categorize CC Descriptions
def cc_categorizer(col):

    fb_pc_class = ['EY2','DY2']
    fb_agu_class = ['VN2','UN2']
    fb_fvb_class = ['B42','A42','AS2','9S2']
    fb_wooof_class = ['2V2','J2V','ZU2']
    fb_auping_class = ['2E2','2E3','ZD2','C22','D22']
    fb_tbag_class = ['CP2','DP2']
    aw_pc_class = ['9059','3462']
    aw_agu_class = ['9284']
    aw_fvb_class = ['2572','7602']
    aw_wooof_class = ['1942','7340']
    aw_auping_class = ['0889','8780']
    aw_tbag_class = ['8419']
    aw_fsg_class = ['8419']

# Linked In
    if 'LINKEDIN' in col:
        return "LinkedIn", "FSG"
# Facebook
    elif any(string in col for string in fb_pc_class):
        return "Facebook", "PC"
    elif any(string in col for string in fb_fvb_class):
        return "Facebook", "FvB"
    elif any(string in col for string in fb_agu_class):
        return "Facebook", "AGU"
    elif any(string in col for string in fb_wooof_class):
        return "Facebook", "WOOOF"
    elif any(string in col for string in fb_auping_class):
        return "Facebook", "Auping"
    elif any(string in col for string in fb_tbag_class):
        return "Facebook", "TBag"
# Adwords
    elif any(string in col for string in aw_pc_class):
        return "Adwords", "PC"
    elif any(string in col for string in aw_fvb_class):
        return "Adwords", "FvB"
    elif any(string in col for string in aw_agu_class):
        return "Adwords", "AGU"
    elif any(string in col for string in aw_wooof_class):
        return "Adwords", "WOOOF"
    elif any(string in col for string in aw_auping_class):
        return "Adwords", "Auping"
    elif any(string in col for string in aw_tbag_class):
        return "Adwords", "TBag"
    elif any(string in col for string in aw_fsg_class):
        return "Adwords", "FSG"
# Bing
    elif 'MICROSOFT*ADVERTISING' in col:
        return "Bing", "FSG"
# Else
    else:
        return "Unknown", "FSG"


# Date Format Change
def datestr_converter(df):
    if ((df["date"].dtype == '<M8[ns]')|(df["date"].dtype == 'datetime64[ns]')):
        df["date"] = df["date"].dt.strftime('%Y-%m-%d')
        return df
    elif (df["date"].dtype == 'O'):
        return df
    else:
        pass
