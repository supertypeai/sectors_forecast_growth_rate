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

data= data.dropna().drop(['sub_sector_id','historical_valuation'], axis=1)

data.to_csv('cr.csv',index = False)
