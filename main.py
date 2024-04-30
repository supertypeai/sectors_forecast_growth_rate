import requests
import json
import pandas as pd

url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-analysis"

headers = {
	"X-RapidAPI-Key": "6ab3672a06msh2ed5f438f0b0778p16617fjsnf2d8ec4cde43",
	"X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
}

symbols = ['AALI.JK','ABBA.JK','AMRT.JK','BANK.JK','BBCA.JK','BBLD.JK','BBRI.JK']

def extract_growth_data(symbol):
  querystring = {"symbol":symbol,"region":"ID"}
  response = requests.get(url, headers=headers, params=querystring)
  data = response.json()

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

  return growth_data

all_growth_data = []
for symbol in symbols:
  growth = extract_growth_data(symbol)
  print(growth)
  all_growth_data.append(growth)

df = pd.DataFrame(all_growth_data)
df.to_csv('forecast_rapid_api.csv', index = False)