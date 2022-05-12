import pandas as pd
import numpy as np
import gspread
from gspread_pandas import Spread, Client
from functions import convert_fill, export_converter, datestr_converter
import time

# Connect to API Service
gc = gspread.service_account()

# Open Cost Control Sheet
fsg_cost_control = gc.open("FSG_Cost_Control")


""""

TURNOVER


"""

# Download PC TO Data
pc_to_df = convert_fill(fsg_cost_control.worksheet('PC_Turnover'))
pc_to_df['Total_Revenue_EXVAT'] = pc_to_df.iloc[:,2:].sum(axis=1)
time.sleep(100)

# Download FvB TO Data
fvb_to_df = convert_fill(fsg_cost_control.worksheet('FvB_Turnover'))
fvb_to_df['Total_Revenue_EXVAT'] = fvb_to_df.iloc[:,2]
time.sleep(100)

# Download KnK TO Data
knk_to_df = convert_fill(fsg_cost_control.worksheet('KnK_Turnover'))
knk_to_df['Total_Revenue_EXVAT'] = knk_to_df.iloc[:,2]
time.sleep(100)

# Download WOOOF TO Data
wooof_to_df = convert_fill(fsg_cost_control.worksheet('WOOOF_Turnover'))
wooof_to_df['Total_Revenue_EXVAT'] = wooof_to_df.iloc[:,5:].sum(axis=1)
time.sleep(100)

# Download Auping TO Data
auping_to_df = convert_fill(fsg_cost_control.worksheet('Auping_Turnover'))
auping_to_df['Total_Revenue_EXVAT'] = auping_to_df.iloc[:,2:].sum(axis=1)
time.sleep(100)

# Download AGU TO Data
agu_to_df = convert_fill(fsg_cost_control.worksheet('AGU_Turnover'))
agu_to_df['Total_Revenue_EXVAT'] = agu_to_df.iloc[:,3]
time.sleep(100)

# Download POM TO Data
pom_to_df = convert_fill(fsg_cost_control.worksheet('POM_Turnover'))
pom_to_df['Total_Revenue_EXVAT'] = pom_to_df.iloc[:,2:].sum(axis=1)
time.sleep(100)

# Download P4L TO Data
passion_to_df = convert_fill(fsg_cost_control.worksheet('P4L_Turnover'))
passion_to_df['Total_Revenue_EXVAT'] = passion_to_df.iloc[:,2:].sum(axis=1)
time.sleep(100)

# Summarized TO
frames = [pc_to_df,fvb_to_df,knk_to_df,wooof_to_df,auping_to_df,agu_to_df, pom_to_df, passion_to_df]
summarized_to = pd.concat(frames, ignore_index=True, sort=False).fillna(0)

# Google Sheets export formatting
summarized_to.columns = summarized_to.columns.str.lower()
summarized_to = export_converter(summarized_to, summarized_to.columns[2:])
summarized_to = datestr_converter(summarized_to)


""""

MEDIA COSTS


"""

# Download PC Costs Data
pc_perf_df = convert_fill(fsg_cost_control.worksheet('PC_Performance'))
time.sleep(100)
pc_br_df = convert_fill(fsg_cost_control.worksheet('PC_Brand'))
time.sleep(100)
pc_rep_df = convert_fill(fsg_cost_control.worksheet('PC_Reporting'))
time.sleep(100)

# Create Aggregated Columns
pc_perf_df['Total_Perf_Spend'] = pc_perf_df.loc[:, pc_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
pc_perf_df['Total_Perf_Planned'] = pc_perf_df.loc[:, pc_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

pc_br_df['Total_Brand_Spend'] = pc_br_df.loc[:, pc_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
pc_br_df['Total_Brand_Planned'] = pc_br_df.loc[:, pc_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

pc_rep_df['Total_Rep_Spend'] = pc_rep_df.loc[:, pc_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
pc_rep_df['Total_Rep_Planned'] = pc_rep_df.loc[:, pc_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download FvB Costs Data
fvb_perf_df = convert_fill(fsg_cost_control.worksheet('FvB_Performance'))
time.sleep(100)
fvb_br_df = convert_fill(fsg_cost_control.worksheet('FvB_Brand'))
time.sleep(100)
fvb_rep_df = convert_fill(fsg_cost_control.worksheet('FvB_Reporting'))
time.sleep(100)

# Create Aggregated Columns
fvb_perf_df['Total_Perf_Spend'] = fvb_perf_df.loc[:, fvb_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
fvb_perf_df['Total_Perf_Planned'] = fvb_perf_df.loc[:, fvb_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

fvb_br_df['Total_Brand_Spend'] = fvb_br_df.loc[:, fvb_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
fvb_br_df['Total_Brand_Planned'] = fvb_br_df.loc[:, fvb_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

fvb_rep_df['Total_Rep_Spend'] = fvb_rep_df.loc[:, fvb_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
fvb_rep_df['Total_Rep_Planned'] = fvb_rep_df.loc[:, fvb_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download KnK Costs Data
knk_perf_df = convert_fill(fsg_cost_control.worksheet('KnK_Performance'))
time.sleep(100)
knk_br_df = convert_fill(fsg_cost_control.worksheet('KnK_Brand'))
time.sleep(100)
knk_rep_df = convert_fill(fsg_cost_control.worksheet('KnK_Reporting'))
time.sleep(100)

# Create Aggregated Columns
knk_perf_df['Total_Perf_Spend'] = knk_perf_df.loc[:, knk_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
knk_perf_df['Total_Perf_Planned'] = knk_perf_df.loc[:, knk_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

knk_br_df['Total_Brand_Spend'] = knk_br_df.loc[:, knk_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
knk_br_df['Total_Brand_Planned'] = knk_br_df.loc[:, knk_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

knk_rep_df['Total_Rep_Spend'] = knk_rep_df.loc[:, knk_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
knk_rep_df['Total_Rep_Planned'] = knk_rep_df.loc[:, knk_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download WOOOF Costs Data
wooof_perf_df = convert_fill(fsg_cost_control.worksheet('WOOOF_Performance'))
time.sleep(100)
wooof_br_df = convert_fill(fsg_cost_control.worksheet('WOOOF_Brand'))
time.sleep(100)

# Create Aggregated Columns
wooof_perf_df['Total_Perf_Spend'] = wooof_perf_df.loc[:, wooof_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
wooof_perf_df['Total_Perf_Planned'] = wooof_perf_df.loc[:, wooof_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

wooof_br_df['Total_Brand_Spend'] = wooof_br_df.loc[:, wooof_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
wooof_br_df['Total_Brand_Planned'] = wooof_br_df.loc[:, wooof_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download Auping Costs Data
auping_perf_df = convert_fill(fsg_cost_control.worksheet('Auping_Performance'))
time.sleep(100)
auping_br_df = convert_fill(fsg_cost_control.worksheet('Auping_Brand'))
time.sleep(100)

# Create Aggregated Columns
auping_perf_df['Total_Perf_Spend'] = auping_perf_df.loc[:, auping_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
auping_perf_df['Total_Perf_Planned'] = auping_perf_df.loc[:, auping_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

auping_br_df['Total_Brand_Spend'] = auping_br_df.loc[:, auping_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
auping_br_df['Total_Brand_Planned'] = auping_br_df.loc[:, auping_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download AGU Costs Data
agu_perf_df = convert_fill(fsg_cost_control.worksheet('AGU_Performance'))
time.sleep(100)
agu_br_df = convert_fill(fsg_cost_control.worksheet('AGU_Brand'))
time.sleep(100)
agu_rep_df = convert_fill(fsg_cost_control.worksheet('AGU_Reporting'))
time.sleep(100)

# Create Aggregated Columns
agu_perf_df['Total_Perf_Spend'] = agu_perf_df.loc[:, agu_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
agu_perf_df['Total_Perf_Planned'] = agu_perf_df.loc[:, agu_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

agu_br_df['Total_Brand_Spend'] = agu_br_df.loc[:, agu_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
agu_br_df['Total_Brand_Planned'] = agu_br_df.loc[:, agu_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

agu_rep_df['Total_Rep_Spend'] = agu_rep_df.loc[:, agu_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
agu_rep_df['Total_Rep_Planned'] = agu_rep_df.loc[:, agu_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download POM Costs Data
pom_perf_df = convert_fill(fsg_cost_control.worksheet('POM_Performance'))
time.sleep(100)
pom_br_df = convert_fill(fsg_cost_control.worksheet('POM_Brand'))
time.sleep(100)
pom_rep_df = convert_fill(fsg_cost_control.worksheet('POM_Reporting'))
time.sleep(100)

# Create Aggregated Columns
pom_perf_df['Total_Perf_Spend'] = pom_perf_df.loc[:, pom_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
pom_perf_df['Total_Perf_Planned'] = pom_perf_df.loc[:, pom_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

pom_br_df['Total_Brand_Spend'] = pom_br_df.loc[:, pom_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
pom_br_df['Total_Brand_Planned'] = pom_br_df.loc[:, pom_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

pom_rep_df['Total_Rep_Spend'] = pom_rep_df.loc[:, pom_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
pom_rep_df['Total_Rep_Planned'] = pom_rep_df.loc[:, pom_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)


# Download P4L Costs Data
passion_perf_df = convert_fill(fsg_cost_control.worksheet('P4L_Performance'))
time.sleep(100)
passion_br_df = convert_fill(fsg_cost_control.worksheet('P4L_Brand'))
time.sleep(100)
passion_rep_df = convert_fill(fsg_cost_control.worksheet('P4L_Reporting'))
time.sleep(100)

# Create Aggregated Columns
passion_perf_df['Total_Perf_Spend'] = passion_perf_df.loc[:, passion_perf_df.columns.str.contains('Spend')].sum(axis=1).astype(int)
passion_perf_df['Total_Perf_Planned'] = passion_perf_df.loc[:, passion_perf_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

passion_br_df['Total_Brand_Spend'] = passion_br_df.loc[:, passion_br_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
passion_br_df['Total_Brand_Planned'] = passion_br_df.loc[:, passion_br_df.columns.str.contains('Planned')].sum(axis=1).astype(int)

passion_rep_df['Total_Rep_Spend'] = passion_rep_df.loc[:, passion_rep_df.columns.str.contains('Actual')].sum(axis=1).astype(int)
passion_rep_df['Total_Rep_Planned'] = passion_rep_df.loc[:, passion_rep_df.columns.str.contains('Planned')].sum(axis=1).astype(int)



# Summarized Performance
frames_perf = [pc_perf_df,fvb_perf_df,knk_perf_df,wooof_perf_df,auping_perf_df,agu_perf_df, pom_perf_df, passion_perf_df]
summarized_perf = pd.concat(frames_perf, ignore_index=True, sort=False)

# Summarized Brand
frames_br = [pc_br_df,fvb_br_df,knk_br_df,wooof_br_df,auping_br_df,agu_br_df, pom_br_df, passion_br_df]
summarized_br = pd.concat(frames_br, ignore_index=True, sort=False)

# Summarized Reporting
frames_rep = [pc_rep_df,fvb_rep_df,knk_rep_df,agu_rep_df, pom_rep_df, passion_rep_df]
summarized_rep = pd.concat(frames_rep, ignore_index=True, sort=False)


# Summarized Marketing Costs
summarized_media_cost = summarized_perf.loc[:,['Date','Client','Total_Perf_Spend','Total_Perf_Planned']]
summarized_media_cost['Total_Brand_Spend'] = summarized_br['Total_Brand_Spend']
summarized_media_cost['Total_Brand_Planned'] = summarized_br['Total_Brand_Planned']
summarized_media_cost['Total_Rep_Spend'] = summarized_rep['Total_Rep_Spend']
summarized_media_cost['Total_Rep_Planned'] = summarized_rep['Total_Rep_Planned']
summarized_media_cost = summarized_media_cost.fillna(0)

# Aggregate Total Variables
summarized_media_cost['Total_Spend'] = summarized_media_cost.loc[:, summarized_media_cost.columns.str.contains('Spend')].sum(axis=1).astype(int)
summarized_media_cost['Total_Planned'] = summarized_media_cost.loc[:, summarized_media_cost.columns.str.contains('Planned')].sum(axis=1).astype(int)

# Google Sheets export formatting
summarized_media_cost.columns = summarized_media_cost.columns.str.lower()
summarized_media_cost = export_converter(summarized_media_cost, summarized_media_cost.columns[2:])
summarized_media_cost = datestr_converter(summarized_media_cost)

"""


GOOGLE SHEETS EXPORTING


"""

# Output sheet connection
fsg_dashboard = Spread('FSG - Finance Dashboard Input')

# Push TO
fsg_dashboard.df_to_sheet(summarized_to, index=False, sheet='Summarized TO', start='A1', replace=True)
time.sleep(50)
# Push Costs
fsg_dashboard.df_to_sheet(summarized_media_cost, index=False, sheet='Summarized Media Costs', start='A1', replace=True)
time.sleep(50)
