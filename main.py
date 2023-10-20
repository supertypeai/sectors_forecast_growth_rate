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

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

key_data = supabase.table("idx_company_profile").select("symbol","sub_sector_id").execute()
key_df = pd.DataFrame(key_data.data).sort_values(['symbol'])

symbols = key_df['symbol'].to_list()

headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0'}

all_list = {
    'avg_estimate_earnings_current_year': [],
    'avg_estimate_earnings_next_year': [],
    'avg_estimate_revenue_current_year': [],
    'avg_estimate_revenue_next_year': [],
    'estimate_overall_growth_current_year': [],
    'estimate_overall_growth_next_year': [],
    'estimate_overall_growth_next_five_years': [],
}

for symbol in symbols[:3]:
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
        avg_estimate_revenue_next_year = revenue_df.loc[revenue_df['Revenue Estimate'] == 'Avg. Estimate'].iloc[:, 4].values[0]
        estimate_overall_growth_current_year = overall_growth_df.loc[overall_growth_df['Growth Estimates'] == 'Current Year'].iloc[0, :].values[1]
        estimate_overall_growth_next_year = overall_growth_df.loc[overall_growth_df['Growth Estimates'] == 'Next Year'].iloc[0, :].values[1]
        estimate_overall_growth_next_five_years = overall_growth_df.loc[overall_growth_df['Growth Estimates'] == 'Next 5 Years (per annum)'].iloc[0, :].values[1]

        all_list['avg_estimate_earnings_current_year'].append(avg_estimate_earnings_current_year)
        all_list['avg_estimate_earnings_next_year'].append(avg_estimate_earnings_next_year)
        all_list['avg_estimate_revenue_current_year'].append(avg_estimate_revenue_current_year)
        all_list['avg_estimate_revenue_next_year'].append(avg_estimate_revenue_next_year)
        all_list['estimate_overall_growth_current_year'].append(estimate_overall_growth_current_year)
        all_list['estimate_overall_growth_next_year'].append(estimate_overall_growth_next_year)
        all_list['estimate_overall_growth_next_five_years'].append(estimate_overall_growth_next_five_years)

        print(f"{symbol} data processed")
    except Exception as e:
        for key in all_list.keys():
            all_list[key].append(np.nan)
        print(f"{symbol} no data")

data_dict = {
    'symbol': symbols[:3],  
    **all_list,  
}

df = pd.DataFrame.from_dict(data_dict)

current_year = datetime.now().year
last_year= f"{current_year-1}-12-31"
f_data = supabase.table("idx_financials_annual").select("symbol","total_revenue","basic_eps").eq("date", last_year).execute()
f_df = pd.DataFrame(f_data.data).sort_values(['symbol'])

growth_forecast_df = df.merge(f_df, on='symbol', how='inner').merge(key_df, on='symbol', how='inner')

def preprocess_numeric_value(value):
    if pd.isna(value) or value == 0.0:
        return np.nan
    value = str(value).strip().replace(',', '') 
    if 'T' in value.upper():
        return float(value.upper().replace('T', '')) * 1e12
    elif 'B' in value.upper():
        return float(value.upper().replace('B', '')) * 1e9
    elif 'M' in value.upper():
        return float(value.upper().replace('M', '')) * 1e6
    elif 'K' in value.upper():
        return float(value.upper().replace('K', '')) * 1e3
    else:
        return float(value)

def calculate_growth(row, y, type=None):
    try:
        year2 = row[y]

        if type == 'revenue':
            year1 = row['total_revenue'] / 1000
        else:
            year1 = row['basic_eps']

        if pd.isna(year1) or year1 == 0.0:
            return np.nan
        else:
            return (year2 - year1) / year1
    except (ValueError, KeyError):
        return np.nan

numeric_columns = ['avg_estimate_earnings_current_year', 'avg_estimate_earnings_next_year', 'avg_estimate_revenue_current_year', 'avg_estimate_revenue_next_year']

for column in numeric_columns:
    growth_forecast_df[column] = growth_forecast_df[column].apply(preprocess_numeric_value)
    type = None
    if 'revenue' in column:
        type = 'revenue'
    growth_forecast_df[f'{column[4:12]}_growth_{column[13:]}'] = growth_forecast_df.apply(calculate_growth, y=column, type=type, axis=1)


def preprocess_percentage_value(value):
    if pd.isna(value):
        return np.nan
    if '%' in str(value):
        return float(str(value).replace('%', '').replace(',', ''))/100
    else:
        return float(str(value))

percentage_columns = ['estimate_overall_growth_current_year', 'estimate_overall_growth_next_year',	'estimate_overall_growth_next_five_years']
for percentage_column in percentage_columns:
  growth_forecast_df[str(percentage_column)] = growth_forecast_df[str(percentage_column)].apply(preprocess_percentage_value)

final_df = growth_forecast_df[['symbol','sub_sector_id','estimate_overall_growth_current_year','estimate_overall_growth_next_year','estimate_overall_growth_next_five_years','estimate_growth_earnings_current_year',	'estimate_growth_earnings_next_year','estimate_growth_revenue_current_year','estimate_growth_revenue_next_year']]
final_df.columns = ['symbol','sub_sector_id','overall_growth_current_year_f','overall_growth_next_year_f','overall_growth_next_five_years_f','eps_growth_current_year_f',	'eps_growth_next_year_f','revenue_growth_current_year_f','revenue_growth_next_year_f']

final_df.to_csv('idx_company_growth_forecast.csv',index = False)
supabase.table("idx_company_growth_forecast").upsert(final_df.to_dict(orient='records'), returning='minimal')