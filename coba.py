import requests
from bs4 import BeautifulSoup
import datetime
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

# url = "https://www.inflationtool.com/indonesian-rupiah/2019-to-present-value?amount=100&year2=2023&frequency=yearly"
# url = "https://www.inflationtool.com/indonesian-rupiah?amount=100&year1=2019&year2=2022&frequency=yearly"

# # Send a GET request to the URL
# response = requests.get(url)

# # Check if the request was successful (status code 200)
# if response.status_code == 200:
#     soup = BeautifulSoup(response.content, 'html.parser')
#     cell_value = soup.select_one("html > body > div:nth-of-type(3) > div:nth-of-type(2) > div:nth-of-type(2) > div:nth-of-type(1) > table tbody tr:nth-of-type(2) td:nth-of-type(2)")

#     value = cell_value.get_text(strip=True) if cell_value else None
#     avg_inflation_rate = float(value.strip('%')) / 100

# else:
#     avg_inflation_rate = 0.0278

# print(avg_inflation_rate)

# year2 = datetime.now().year - 1
# print(year2)


from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

response = (supabase.rpc("get_peers_pb",params={"p_subsector_slug":"basic-materials"}).execute())


def get_sub_sector_metrics(subsector_slug, supabase_client, metrics):
    if metrics == "pb":
        response = supabase_client.rpc("get_peers_pb", params={"p_subsector_slug": subsector_slug}).execute()
    elif metrics == "pe":
        response = supabase_client.rpc("get_peers_pe", params={"p_subsector_slug": subsector_slug}).execute()
    elif metrics == "ps":
        response = supabase_client.rpc("get_peers_ps", params={"p_subsector_slug": subsector_slug}).execute()

    median_value = response.data[0]['median']
    return median_value

print(get_sub_sector_metrics('basic-materials',supabase,'pe'))
print(get_sub_sector_metrics('basic-materials',supabase,'pb'))
print(get_sub_sector_metrics('basic-materials',supabase,'ps'))