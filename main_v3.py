# restructuring the table to: symbol, year, eps_estimate, revenue_estimate
# by filtering stock that is in the range of 0.5 to 2 pct difference

import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from io import StringIO
import re

def preprocess_numeric_value(value):
    if pd.isna(value) or value == 0.0:
        return np.nan
    str_value = str(value)
    if 'T' in str_value.upper():
        return float(str_value.upper().replace('T', '')) * 1e12
    elif 'B' in str_value.upper():
        return float(str_value.upper().replace('B', '')) * 1e9
    elif 'M' in str_value.upper():
        return float(str_value.upper().replace('M', '')) * 1e6
    elif 'K' in str_value.upper():
        return float(str_value.upper().replace('K', '')) * 1e3
    else:
        return float(value)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

key_data = supabase.table("idx_company_profile").select("symbol","sub_sector_id").execute()
key_df = pd.DataFrame(key_data.data).sort_values(['symbol'])

symbols = key_df['symbol'].to_list()

headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0'}

all_list = {
    'revenue_year_ago' : [],
    'avg_estimate_earnings_current_year': [],
    'avg_estimate_earnings_next_year': [],
    'avg_estimate_revenue_current_year': [],
    'avg_estimate_revenue_next_year': []
}

years = []

for symbol in symbols:
    try:
        url = f'https://finance.yahoo.com/quote/{symbol}/analysis?p={symbol}'
        html_content = requests.get(url, headers=headers).text
        source_df = pd.read_html(StringIO(html_content))

        earnings_df = source_df[0]
        revenue_df = source_df[1]
        overall_growth_df = source_df[5]

        avg_estimate_earnings_current_year = earnings_df.loc[earnings_df['Earnings Estimate'] == 'Avg. Estimate'].iloc[:, 3].values[0]
        avg_estimate_earnings_next_year = earnings_df.loc[earnings_df['Earnings Estimate'] == 'Avg. Estimate'].iloc[:, 4].values[0]
        avg_estimate_revenue_current_year = revenue_df.loc[revenue_df['Revenue Estimate'] == 'Avg. Estimate'].iloc[:, 3].values[0]
        year_ago = revenue_df.loc[revenue_df['Revenue Estimate'] == 'Year Ago Sales'].iloc[:, 3].values[0]
        avg_estimate_revenue_next_year = revenue_df.loc[revenue_df['Revenue Estimate'] == 'Avg. Estimate'].iloc[:, 4].values[0]

        all_list['avg_estimate_earnings_current_year'].append(avg_estimate_earnings_current_year)
        all_list['avg_estimate_earnings_next_year'].append(avg_estimate_earnings_next_year)
        all_list['avg_estimate_revenue_current_year'].append(avg_estimate_revenue_current_year)
        all_list['avg_estimate_revenue_next_year'].append(avg_estimate_revenue_next_year)
        all_list['revenue_year_ago'].append(year_ago)

        print(f"{symbol} data processed")

        if len(years) != 2:
            try:
                years_column = [source_df[0].columns[3], source_df[0].columns[4]]
                for year in years_column:
                    match = re.search(r'\((\d+)\)', year)
                    if match:
                        year = match.group(1)
                        years.append(year)
            except:
                pass

    except Exception as e:
        for key in all_list.keys():
            all_list[key].append(np.nan)
        print(f"{symbol} no data")

data_dict = {
    'symbol': symbols,  
    **all_list,  
}

forecast_df = pd.DataFrame.from_dict(data_dict)

current_year = datetime.now().year
last_year= f"{current_year-1}-12-31"
db_data = supabase.table("idx_financials_annual").select("symbol","total_revenue","diluted_eps").eq("date", last_year).execute()
db_df = pd.DataFrame(db_data.data).sort_values(['symbol'])
eps_data = supabase.table("idx_calc_metrics_annual").select("symbol","diluted_eps").eq("date", last_year).execute()
eps_df = pd.DataFrame(eps_data.data).sort_values(['symbol'])

df = forecast_df.merge(db_df, on='symbol', how='inner').merge(key_df, on='symbol', how='inner').merge(eps_df, on='symbol', how='inner')

numeric_columns = ['avg_estimate_earnings_current_year', 'avg_estimate_earnings_next_year', 'avg_estimate_revenue_current_year', 'avg_estimate_revenue_next_year','revenue_year_ago']

for column in numeric_columns:
    df[column] = df[column].apply(preprocess_numeric_value)

df['multiplier'] = 1
df_1000 = df.copy()
df_1000['multiplier'] = 1000
df_1000['revenue_year_ago'] = df_1000['revenue_year_ago'] * df_1000['multiplier']
clean_estimation_df = pd.concat([df, df_1000], axis=0, ignore_index=True)
clean_estimation_df = clean_estimation_df.sort_values(by=["symbol", "multiplier"])
clean_estimation_df['ratio_mult'] = clean_estimation_df['total_revenue']/ clean_estimation_df['revenue_year_ago']
clean_estimation_df = clean_estimation_df.query("ratio_mult > 0.5 and ratio_mult < 2")

# Reshape the DataFrame
clean_estimation_df['avg_estimate_revenue_current_year'] = clean_estimation_df['avg_estimate_revenue_current_year']*clean_estimation_df['multiplier']
clean_estimation_df['avg_estimate_revenue_next_year'] = clean_estimation_df['avg_estimate_revenue_next_year']*clean_estimation_df['multiplier']
clean_estimation_df = clean_estimation_df.drop(['revenue_year_ago','total_revenue','diluted_eps','multiplier','ratio_mult'], axis = 1)
clean_estimation_df = pd.melt(clean_estimation_df, id_vars=['symbol', 'sub_sector_id'], var_name='column', value_name='value')
clean_estimation_df['year'] = clean_estimation_df['column'].apply(lambda x: years[0] if 'current_year' in x else years[1])
clean_estimation_df['metric_type'] =  clean_estimation_df['column'].str.split('_').str[2]

clean_estimation_df = clean_estimation_df.pivot_table(index=['symbol', 'year', 'sub_sector_id'], columns='metric_type', values='value', aggfunc='first').reset_index()

# Rename columns
clean_estimation_df.columns.name = None 
clean_estimation_df = clean_estimation_df.rename(columns={'earnings': 'eps_estimate', 'revenue': 'revenue_estimate'})

# # Change columns to dtypes
clean_estimation_df['eps_estimate'] = clean_estimation_df['eps_estimate'].astype('float32')
clean_estimation_df['revenue_estimate'] = clean_estimation_df['revenue_estimate'].astype('float64')


def convert_df_to_records(df):
    temp_df = df.copy()
    for cols in temp_df.columns:
        if temp_df[cols].dtype == "datetime64[ns]":
            temp_df[cols] = temp_df[cols].astype(str)
    temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
    temp_df = temp_df.replace({np.nan: None})
    records = temp_df.to_dict("records")
    return records

# clean_estimation_df["sub_sector_id"] = clean_estimation_df["sub_sector_id"].astype(int)
clean_estimation_df = clean_estimation_df.drop(['sub_sector_id'],axis = 1)
records = convert_df_to_records(clean_estimation_df)

clean_estimation_df.to_csv('result/idx_company_growth_forecast_2.csv',index = False)

# try:
#     supabase.table("idx_company_forecast").upsert(records, returning='minimal').execute()
#     print("Upsert operation successful.")
# except Exception as e:
#     print(f"Error during upsert operation: {e}")