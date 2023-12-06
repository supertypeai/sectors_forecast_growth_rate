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
import requests
from bs4 import BeautifulSoup


url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def get_avg_inflation_rate():
    year2 = datetime.now().year - 1
    url = f'https://www.inflationtool.com/indonesian-rupiah?amount=100&year1=2019&year2={year2}&frequency=yearly'

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        cell_value = soup.select_one("html > body > div:nth-of-type(3) > div:nth-of-type(2) > div:nth-of-type(2) > div:nth-of-type(1) > table tbody tr:nth-of-type(2) td:nth-of-type(2)")

        value = cell_value.get_text(strip=True) if cell_value else None
        avg_inflation_rate = float(value.strip('%')) / 100

    else:
        avg_inflation_rate = 0.0278
    return avg_inflation_rate 

def get_sub_sector_metrics(subsector_slug, supabase_client, metrics):
    if metrics == "pb":
        response = supabase_client.rpc("get_peers_pb", params={"p_subsector_slug": subsector_slug}).execute()
    elif metrics == "pe":
        response = supabase_client.rpc("get_peers_pe", params={"p_subsector_slug": subsector_slug}).execute()
    elif metrics == "ps":
        response = supabase_client.rpc("get_peers_ps", params={"p_subsector_slug": subsector_slug}).execute()

    median_value = float(response.data[0]['median'])
    return median_value

def get_supabase_data():

    company_sub_sector_data = supabase.table("idx_company_report").select("symbol","historical_financials","sub_sector").execute() #market cap data is available in this table if needed
    company_sub_sector_df = pd.DataFrame(company_sub_sector_data.data).sort_values(['symbol'])
    company_sub_sector_df.dropna(inplace = True)

    def filter_rows(rows):
        if len(rows) >= 4:
            if all('earnings' in row and 'revenue' in row and row['earnings'] is not None and row['revenue'] is not None for row in rows):
                max_year_dict = max(rows, key=lambda x: x['year'])
                if max_year_dict['earnings'] > 0 and max_year_dict['revenue'] > 0:
                    return True

        return False

    company_sub_sector_df['qualified_ticker'] = company_sub_sector_df['historical_financials'].apply(filter_rows)
    company_sub_sector_df = company_sub_sector_df[company_sub_sector_df['qualified_ticker']]

    def get_value_sorted_by_year(row, metrics):

        sorted_year = sorted(row, key=lambda x: x.get('year', 0))

        if metrics == 'revenue':
            sorted_value = [d.get('revenue') for d in sorted_year]
        elif metrics == 'earnings':
            sorted_value = [d.get('earnings') for d in sorted_year]

        return sorted_value

    company_sub_sector_df['ticker_revenue'] = company_sub_sector_df['historical_financials'].apply(get_value_sorted_by_year, metrics = 'revenue')
    company_sub_sector_df['ticker_net_income'] = company_sub_sector_df['historical_financials'].apply(get_value_sorted_by_year, metrics = 'earnings')

    company_sub_sector_slug_data = supabase.table("idx_subsector_metadata").select("sub_sector","slug").execute()
    company_sub_sector_slug_df = pd.DataFrame(company_sub_sector_slug_data.data)
    company_report_df = pd.merge(company_sub_sector_df, company_sub_sector_slug_df, on='sub_sector', how='left')

    calc_metrics_daily_data = supabase.table("idx_calc_metrics_daily").select("symbol","pe_ttm","ps_ttm","pb_mrq","market_cap").execute()
    calc_metrics_daily_df = pd.DataFrame(calc_metrics_daily_data.data).sort_values(['symbol'])
    calc_metrics_quarter_data = supabase.table("idx_calc_metrics_quarter").select("symbol","total_liabilities_mrq","total_debt_mrq","total_equity_mrq","diluted_eps_ttm","avg_diluted_shares_ttm").execute()
    calc_metrics_quarter_df = pd.DataFrame(calc_metrics_quarter_data.data).sort_values(['symbol'])
    fa_df = pd.merge(calc_metrics_daily_df, calc_metrics_quarter_df, on='symbol', how='left')

    data = pd.merge(company_report_df, fa_df, on='symbol', how='left')
    
    data.rename(columns={'pe_ttm': 'ticker_pe_ttm', 'ps_ttm': 'ticker_ps_ttm', 'pb_mrq': 'ticker_pb_ttm',
                       'total_liabilities_mrq':'ticker_total_liabilities', 'total_debt_mrq':'ticker_total_debt',
                       'total_equity_mrq':'ticker_total_equity','diluted_eps_ttm':'diluted_eps','avg_diluted_shares_ttm':'diluted_shares_outstanding'}, inplace=True)



    sub_sector_pe = {}
    sub_sector_ps = {}
    sub_sector_pb = {}

    for slug in data['slug'].unique():
        if slug not in sub_sector_pe:
            sub_sector_pe[slug] = get_sub_sector_metrics(slug,supabase,'pe')
            sub_sector_pb[slug] = get_sub_sector_metrics(slug,supabase,'pb')
            sub_sector_ps[slug] = get_sub_sector_metrics(slug,supabase,'ps')

    data['sub_sector_pe_ttm'] = data['slug'].map(sub_sector_pe.get)
    data['sub_sector_pb_ttm'] = data['slug'].map(sub_sector_pb.get)
    data['sub_sector_ps_ttm'] = data['slug'].map(sub_sector_ps.get)

    data = data.dropna().drop(['qualified_ticker','historical_financials','sub_sector'], axis=1)

    return data

def calculate_avg_cae(net_income_list):

    # CAE: Cyclically Adjusted Earning

    avg_inflation_rate = get_avg_inflation_rate()
    cae = [price * (1 + float(avg_inflation_rate)) ** (len(net_income_list) - i - 1) for i, price in enumerate(net_income_list)]

    # cae = [price * (1 + 0.0278) ** (len(net_income_list) - i - 1) for i, price in enumerate(net_income_list)]
    
    avg_cae = np.median(cae)
    return avg_cae

def calculate_der(row):

    if row['slug'] == 'banks':
        try:
            der = row['ticker_total_liabilities']/row['ticker_total_equity']
        except:
            der = row['ticker_total_debt']/row['ticker_total_equity']
    else:
        der = row['ticker_total_debt']/row['ticker_total_equity']

    return der

def calculate_profit_margin_stability(row):

    # OPM: Operating Profit Margin
    # Profit Margin Stability (covariance) = std OPM / avg OPM
    try:
        # 1. Calculate Operating Profit Margin (Net Income/Total Revenue)
        operating_profit_margin = [a / b for a, b in zip(row['ticker_net_income'], row['ticker_revenue'])]
        # 2. Calculate the average
        avg = np.mean(operating_profit_margin)
        # 3. Calculate the standard deviation
        std = np.std(operating_profit_margin)
        # 4. Calculate the profit_margin_stability
        profit_margin_stability = std / avg

    except (ZeroDivisionError, ValueError):
        profit_margin_stability = np.nan
    
    return profit_margin_stability


def calculate_correlation(row):

    # Earning Predictability: correlation between year and net income
    generated_list = list(range(1, len(row['ticker_net_income']) + 1))
    ticker_earning_predictability = np.corrcoef(row['ticker_net_income'], generated_list)[0, 1]
    
    return ticker_earning_predictability

def calculate_discount_rate(row):
    beta_list = [0.33, 0.67, 1, 1.5, 2]
    classified_beta = []

    data = {
        'roe': [row['ticker_roe'], [row['sub_sector_roe'] + (5/100) * i for i in range(2, -3, -1)]],
        'der': [row['ticker_der'], [0.1, 0.25, 0.5, 1, 3]],
        'npm': [row['ticker_npm'], [row['sub_sector_npm'] + (5/100) * i for i in range(2, -3, -1)]],
        'profit_margin_stab': [row['ticker_profit_margin_stability'], [0.025, 0.075, 0.1, 0.15, 0.25]],
        'earning_predictability': [row['ticker_earning_predictability'], [1, 0.85, 0.75, 0.25, 0]],
        'market_cap': [row['market_cap']/1000000000000, [200, 100, 50, 10, 1]]
    }

    for key, value in data.items():
        thresholds = value[1]
        if key in ['roe','npm','earning_predictability','market_cap']:
          if value[0] > thresholds[0]:
              classified_beta.append(beta_list[0])
          else:
              for i in range(1, 5):
                  if value[0] > thresholds[i]:
                      classified_beta.append(beta_list[i] - (value[0] - thresholds[i]) / (thresholds[i-1] - thresholds[i]) * (beta_list[i] - beta_list[i-1]))
                      break
              else:
                  classified_beta.append(beta_list[4])
        else:
          if value[0] < thresholds[0]:
              classified_beta.append(beta_list[0])
          else:
              for i in range(1, 5):
                  if value[0] < thresholds[i]:
                      classified_beta.append(beta_list[i] - (value[0] - thresholds[i]) / (thresholds[i-1] - thresholds[i]) * (beta_list[i] - beta_list[i-1]))
                      break
              else:
                  classified_beta.append(beta_list[4])


    beta = np.median(classified_beta)
    discount_rate = 0.07+(0.05 * beta)

    return discount_rate

def calculate_intrinsic_value(row):
    avg_eps = row['avg_eps'] 
    growth_rate_10y = 0.065 
    growth_rate_after_10y = 0.02
    discount_rate = row['discount_rate']

    values = []
    for i in range(1, 101):
        if i <= 10:
            values.append(avg_eps * (1 + growth_rate_10y) ** i)
        else:
            values.append(values[9] * (1 + growth_rate_after_10y) ** (i - 10))

    present_values = [value / ((1 + discount_rate) ** (i + 1)) for i, value in enumerate(values)]
    intrinsic_value = sum(present_values)

    return intrinsic_value


data = get_supabase_data()

# data = pd.read_csv('dcf_valuation_data_new.csv')

# data['avg_cae'] = data['ticker_net_income'].apply(calculate_avg_cae)
# print("Done calculate average cae")
# data['sub_sector_roe'] = data['sub_sector_pb_ttm']/data['sub_sector_pe_ttm']
# data['sub_sector_npm'] = data['sub_sector_ps_ttm']/data['sub_sector_pe_ttm']
# data['ticker_roe'] = data['ticker_pb_ttm']/data['ticker_pe_ttm']
# data['ticker_npm'] = data['ticker_ps_ttm']/data['ticker_pe_ttm']
# data['ticker_der'] = data.apply(calculate_der, axis=1)
# data['ticker_profit_margin_stability'] = data.apply(calculate_profit_margin_stability, axis=1)
# print("Done calculate profit margin stability")
# data['ticker_earning_predictability'] = data.apply(calculate_correlation, axis=1)
# print("Done calculate correlation")

# data['discount_rate'] = data.apply(calculate_discount_rate, axis=1)
# print("Done calculate discount rate")
# data['cae_per_share'] = data['avg_cae']/data['share_issued']
# data['avg_eps'] = np.mean([data['cae_per_share'], data['diluted_eps']], axis=0)
# data['intrinsic_value'] = data.apply(calculate_intrinsic_value, axis=1)
# print("Done calculate intrinsic value")

# data = data[['symbol','intrinsic_value']]
# data = data[data['intrinsic_value'] > 0]


data.to_csv('dcf_valuation_data_new_2.csv', index = False)
