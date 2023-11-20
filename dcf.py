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

def get_avg_inflation_rate():
    return avg_inflation_rate 

def get_supabase_data():
    company_report_data = supabase.table("idx_company_report_mv").select("symbol","sub_sector","historical_valuation","historical_financials","market_cap").execute()
    company_report_df = pd.DataFrame(company_report_data.data).sort_values(['symbol'])
    sub_sector_id_map = {
        "Transportation Infrastructure": 28, "Food & Beverage": 2, "Holding & Investment Companies": 21, "Leisure Goods": 12,"Software & IT Services": 30,
        "Basic Materials": 8,"Automobiles & Components": 10,"Retailing": 14,"Investment Service": 22,"Consumer Services": 11,"Media & Entertainment": 13,
        "Telecommunication": 6,"Technology Hardware & Equipment": 31,"Banks": 19,"Pharmaceuticals & Health Care Research": 24,"Household Goods": 1,"Tobacco": 3,
        "Insurance": 4,"Industrial Goods": 5,"Properties & Real Estate": 7,"Apparel & Luxury Goods": 9,"Food & Staples Retailing": 15,"Nondurable Household Products": 16,
        "Alternative Energy": 17,"Oil, Gas & Coal": 18,"Financing Service": 20,"Healthcare Equipment & Providers": 23,"Multi-sector Holdings": 26,
        "Heavy Constructions & Civil Engineering": 27,"Industrial Services": 25,"Utilities": 29,"Logistics & Deliveries": 32,"Transportation": 33,
    }
    company_report_df['sub_sector_id'] = company_report_df['sub_sector'].map(sub_sector_id_map.get)
    company_report_df.dropna(inplace = True)

    company_report_df['ticker_pe_ttm'] = company_report_df['historical_valuation'].apply(lambda x: x[-1].get('pe'))
    company_report_df['ticker_pb_ttm'] = company_report_df['historical_valuation'].apply(lambda x: x[-1].get('pb'))
    company_report_df['ticker_ps_ttm'] = company_report_df['historical_valuation'].apply(lambda x: x[-1].get('ps'))
    company_report_df['ticker_total_liabilities'] = company_report_df['historical_financials'].apply(lambda x: x[-1].get('total_liabilities'))
    company_report_df['ticker_total_debt'] = company_report_df['historical_financials'].apply(lambda x: x[-1].get('total_debt'))
    company_report_df['ticker_total_equity'] = company_report_df['historical_financials'].apply(lambda x: x[-1].get('total_equity'))
    company_report_df['ticker_revenue'] = company_report_df['historical_financials'].apply(lambda x: [d.get('revenue') for d in x])
    company_report_df['ticker_net_income'] = company_report_df['historical_financials'].apply(lambda x: [d.get('earnings') for d in x])

    company_report_df = company_report_df[company_report_df['ticker_net_income'].apply(lambda x: len(x) >= 4)]
    company_report_df = company_report_df.dropna().drop(['sub_sector','historical_valuation','historical_financials'], axis=1)
    company_report_df = company_report_df[company_report_df['ticker_net_income'].apply(lambda x: x[-1] >= 0)]


    current_year = datetime.now().year
    last_year= f"{current_year-1}-12-31"
    fa_data = supabase.table("idx_financials_annual").select("symbol","basic_eps","share_issued").eq("date", last_year).execute()
    fa_df = pd.DataFrame(fa_data.data).sort_values(['symbol'])
    comp_fa_df = pd.merge(company_report_df, fa_df, on='symbol', how='left')

    sub_sector_report_data = supabase.table("idx_sector_reports_calc").select("sub_sector_id","historical_valuation").execute()
    sub_sector_report_df = pd.DataFrame(sub_sector_report_data.data)
    data = pd.merge(comp_fa_df, sub_sector_report_df, on='sub_sector_id', how='left')

    data['sub_sector_pe_ttm'] = data['historical_valuation'].apply(lambda x: x[-1].get('pe'))
    data['sub_sector_pb_ttm'] = data['historical_valuation'].apply(lambda x: x[-1].get('pb'))
    data['sub_sector_ps_ttm'] = data['historical_valuation'].apply(lambda x: x[-1].get('ps'))

    data= data.dropna().drop(['historical_valuation'], axis=1)

    return data

def calculate_avg_cae(net_income_list):

    # CAE: Cyclically Adjusted Earning

    # avg_inflation_rate = get_avg_inflation_rate()
    # cae = [price * (1 + float(avg_inflation_rate)) ** (len(net_income_list) - i - 1) for i, price in enumerate(net_income_list)]

    cae = [price * (1 + 0.0278) ** (len(net_income_list) - i - 1) for i, price in enumerate(net_income_list)]
    avg_cae = np.median(cae)
    return avg_cae

def calculate_der(row):

    if row['sub_sector_id'] == 19:
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

data['avg_cae'] = data['ticker_net_income'].apply(calculate_avg_cae)
data['sub_sector_roe'] = data['sub_sector_pb_ttm']/data['sub_sector_pe_ttm']
data['sub_sector_npm'] = data['sub_sector_ps_ttm']/data['sub_sector_pe_ttm']
data['ticker_roe'] = data['ticker_pb_ttm']/data['ticker_pe_ttm']
data['ticker_npm'] = data['ticker_ps_ttm']/data['ticker_pe_ttm']
data['ticker_der'] = data.apply(calculate_der, axis=1)
data['ticker_profit_margin_stability'] = data.apply(calculate_profit_margin_stability, axis=1)
data['ticker_earning_predictability'] = data.apply(calculate_correlation, axis=1)

data['discount_rate'] = data.apply(calculate_discount_rate, axis=1)
data['cae_per_share'] = data['avg_cae']/data['share_issued']
data['avg_eps'] = np.mean([data['cae_per_share'], data['basic_eps']], axis=0)
data['intrinsic_value'] = data.apply(calculate_intrinsic_value, axis=1)

data = data[['symbol','intrinsic_value']]
data = data[data['intrinsic_value'] > 0]


data.to_csv('dcf_valuation.csv', index = False)
