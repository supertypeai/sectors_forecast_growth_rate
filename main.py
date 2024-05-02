import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

url_rapidapi = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-analysis"

headers = {
	"X-RapidAPI-Key": str(os.environ.get("rapid_api")),
	"X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
}

url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

# Retrieve symbols with analyst data based on the previous year
symbols_data = supabase.table("idx_company_forecast").select("symbol").execute()
symbols = list({d['symbol'] for d in symbols_data.data})

def extract_growth_data(symbol):
  querystring = {"symbol":symbol,"region":"ID"}
  response = requests.get(url_rapidapi, headers=headers, params=querystring)
  data = response.json()
  print(data)

  raw_analysis_data = data['earningsTrend']['trend']
  desired_periods = ['0y', '+1y']
  final_analysis_data = [entry for entry in raw_analysis_data if entry['period'] in desired_periods]

  growth_data = { 'symbol': symbol,
                  'revenue_year_ago': None,
                  'avg_estimate_earnings_current_year': None,
                  'avg_estimate_earnings_next_year': None,
                  'avg_estimate_revenue_current_year': None,
                  'avg_estimate_revenue_next_year': None}

  for entry in final_analysis_data:
    try:
        if entry['period'] == '0y':
            growth_data['revenue_year_ago'] = entry['revenueEstimate']['yearAgoRevenue']['raw']
            growth_data['avg_estimate_earnings_current_year'] = entry['earningsEstimate']['avg']['raw']
            growth_data['avg_estimate_revenue_current_year'] = entry['revenueEstimate']['avg']['raw']
        else:
            growth_data['avg_estimate_earnings_next_year'] = entry['earningsEstimate']['avg']['raw']
            growth_data['avg_estimate_revenue_next_year'] = entry['revenueEstimate']['avg']['raw']
    except:
       pass
  years = []
  for i in range(len(data["earningsTrend"]["trend"])):
    try:
        date = data["earningsTrend"]["trend"][i]["endDate"]
        year = datetime.strptime(date, '%Y-%m-%d').year
        if year not in years:
            years.append(year)
    except:
        pass

  return growth_data, years

all_growth_data = []
years = []
for symbol in symbols:
  growth, year_list = extract_growth_data(symbol)
  print(growth)
  all_growth_data.append(growth)
  for year in year_list:
    if year not in years:
      years.append(year)

forecast_df = pd.DataFrame(all_growth_data)
forecast_df.to_csv("forecast_rapid_api.csv", index = False)


# current_year = datetime.now().year
# last_year= f"{current_year-1}-12-31"
# key_df = pd.read_csv("idx_company_profile_rows.csv")
# db_data = supabase.table("idx_financials_annual").select("symbol","total_revenue").eq("date", last_year).execute()
# db_df = pd.DataFrame(db_data.data).sort_values(['symbol'])
# eps_data = supabase.table("idx_calc_metrics_annual").select("symbol","diluted_eps").eq("date", last_year).execute()
# eps_df = pd.DataFrame(eps_data.data).sort_values(['symbol'])

# df = forecast_df.merge(db_df, on='symbol', how='inner').merge(key_df, on='symbol', how='inner').merge(eps_df, on='symbol', how='inner')
# print(df)
# print(len(df))

# # numeric_columns = ['avg_estimate_earnings_current_year', 'avg_estimate_earnings_next_year', 'avg_estimate_revenue_current_year', 'avg_estimate_revenue_next_year','revenue_year_ago']

# df = df.replace("--", np.nan, regex=True)

# # for column in numeric_columns:
# #     df[column] = df[column].apply(preprocess_numeric_value)

# df['multiplier'] = 1
# df_1000 = df.copy()
# df_1000['multiplier'] = 1000
# df_1000['revenue_year_ago'] = df_1000['revenue_year_ago'] * df_1000['multiplier']
# clean_estimation_df = pd.concat([df, df_1000], axis=0, ignore_index=True)
# clean_estimation_df = clean_estimation_df.sort_values(by=["symbol", "multiplier"])
# clean_estimation_df['ratio_mult'] = clean_estimation_df['total_revenue']/ clean_estimation_df['revenue_year_ago']
# clean_estimation_df = clean_estimation_df.query("ratio_mult > 0.5 and ratio_mult < 2")

# print(clean_estimation_df)

# # Reshape the DataFrame
# clean_estimation_df['avg_estimate_revenue_current_year'] = clean_estimation_df['avg_estimate_revenue_current_year']*clean_estimation_df['multiplier']
# clean_estimation_df['avg_estimate_revenue_next_year'] = clean_estimation_df['avg_estimate_revenue_next_year']*clean_estimation_df['multiplier']
# clean_estimation_df = clean_estimation_df.drop(['revenue_year_ago','total_revenue','diluted_eps','multiplier','ratio_mult'], axis = 1)
# clean_estimation_df = pd.melt(clean_estimation_df, id_vars=['symbol', 'sub_sector_id'], var_name='column', value_name='value')
# clean_estimation_df['year'] = clean_estimation_df['column'].apply(lambda x: years[0] if 'current_year' in x else years[1])
# clean_estimation_df['metric_type'] =  clean_estimation_df['column'].str.split('_').str[2]

# clean_estimation_df = clean_estimation_df.pivot_table(index=['symbol', 'year', 'sub_sector_id'], columns='metric_type', values='value', aggfunc='first').reset_index()

# print(clean_estimation_df)

# # Rename columns
# clean_estimation_df.columns.name = None 
# clean_estimation_df = clean_estimation_df.rename(columns={'earnings': 'eps_estimate', 'revenue': 'revenue_estimate'})

# # # Change columns to dtypes
# clean_estimation_df['eps_estimate'] = clean_estimation_df['eps_estimate'].astype('float32')
# clean_estimation_df['revenue_estimate'] = clean_estimation_df['revenue_estimate'].astype('float64')


# def convert_df_to_records(df):
#     temp_df = df.copy()
#     for cols in temp_df.columns:
#         if temp_df[cols].dtype == "datetime64[ns]":
#             temp_df[cols] = temp_df[cols].astype(str)
#     temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
#     temp_df = temp_df.replace({np.nan: None})
#     records = temp_df.to_dict("records")
#     return records

# # clean_estimation_df["sub_sector_id"] = clean_estimation_df["sub_sector_id"].astype(int)
# clean_estimation_df = clean_estimation_df.drop(['sub_sector_id'],axis = 1)
# records = convert_df_to_records(clean_estimation_df)

# clean_estimation_df.to_csv('result/idx_company_growth_forecast_3.csv',index = False)

# # try:
# #     supabase.table("idx_company_forecast").upsert(records, returning='minimal').execute()
# #     print("Upsert operation successful.")
# # except Exception as e:
# #     print(f"Error during upsert operation: {e}")
