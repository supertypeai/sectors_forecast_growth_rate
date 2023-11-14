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
    'revenue_year_ago' : []
}

for symbol in symbols:
    try:
        url = f'https://finance.yahoo.com/quote/{symbol}/analysis?p={symbol}'
        html_content = requests.get(url, headers=headers).text
        source_df = pd.read_html(StringIO(html_content))

        revenue_df = source_df[1]

        year_ago = revenue_df.loc[revenue_df['Revenue Estimate'] == 'Year Ago Sales'].iloc[:, 3].values[0]

        all_list['revenue_year_ago'].append(year_ago)

        print(f"{symbol} data processed")
    except Exception as e:
        for key in all_list.keys():
            all_list[key].append(np.nan)
        print(f"{symbol} no data")

data_dict = {
    'symbol': symbols,  
    **all_list,  
}

df = pd.DataFrame.from_dict(data_dict)

current_year = datetime.now().year
last_year= f"{current_year-1}-12-31"
f_data = supabase.table("idx_financials_annual").select("symbol","total_revenue","basic_eps").eq("date", last_year).execute()
f_df = pd.DataFrame(f_data.data).sort_values(['symbol'])

rev_year_ago_df = df.merge(f_df, on='symbol', how='inner').merge(key_df, on='symbol', how='inner')

def preprocess_numeric_value(value):
    if pd.isna(value) or value == 0.0:
        return np.nan
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

numeric_columns = ['revenue_year_ago']

for column in numeric_columns:
    rev_year_ago_df[column] = rev_year_ago_df[column].apply(preprocess_numeric_value)

rev_year_ago_df.to_csv('idx_company_rev_year_ago.csv',index = False)
