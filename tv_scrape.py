from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import os 
import time
import re

from datetime import datetime
import pandas as pd

from supabase import create_client
import pandas as pd
from dotenv import load_dotenv

def clean_value(value):
    # Remove Unicode control characters using regex
    cleaned_value = re.sub(r'[\u202a\u202c\u202f]', '', value)

    # Replace Unicode minus (U+2212) with ASCII hyphen-minus (U+002D)
    cleaned_value = cleaned_value.replace('−', '-')
    
    return cleaned_value

def convert_to_numeric(value):
    # Dictionary to map suffixes to their corresponding multipliers
    suffix_multipliers = {
        'T': 1e12,  # Trillion
        'B': 1e9,   # Billion
        'M': 1e6,   # Million
        'K': 1e3    # Thousand
    }

    # If the value ends with a known suffix
    if value[-1] in suffix_multipliers:
        multiplier = suffix_multipliers[value[-1]]  # Get the corresponding multiplier
        numeric_value = float(value[:-1]) * multiplier  # Remove the suffix, convert to float, and multiply
        return int(numeric_value)
    else:
        return float(value)
    
def get_eps_growth(driver):    
    # get the element
    eps = driver.find_element(By.XPATH, '//*[@id="js-category-content"]/div[2]/div/div[1]/div[2]/div/div[3]/div[2]/div/div[1]')
    html = eps.get_attribute("innerHTML")

    # Parse the HTML
    soup = BeautifulSoup(html, 'html.parser')

    # Extract the years (column headers)
    years = [div.get_text() for div in soup.select('.values-OWKkVLyj .value-OxVAcLqi')]
    try:
        # Extract the reported values (data)
        reported_values = [div.get_text().strip() for div in soup.select('[data-name="Reported"] .value-OxVAcLqi')]

        # Cleaned list
        reported_values = [clean_value(val) if val != '—' else val for val in reported_values]

        estimated_values = [div.get_text().strip() for div in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]

        # Cleaned list
        estimated_values = [clean_value(val) if val != '—' else val for val in estimated_values]

        # Create a dictionary for the table
        data = {
            'Year': years,
            'Reported': reported_values,
            'Estimate': estimated_values
        }

        # Convert to DataFrame
        df = pd.DataFrame(data)
    except:
        years = years[2:]
        
        # Extract the reported values (data)
        reported_values = [div.get_text().strip() for div in soup.select('[data-name="Reported"] .value-OxVAcLqi')]

        # Cleaned list
        reported_values = [clean_value(val) if val != '—' else val for val in reported_values]

        estimated_values = [div.get_text().strip() for div in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]

        # Cleaned list
        estimated_values = [clean_value(val) if val != '—' else val for val in estimated_values]

        # Create a dictionary for the table
        data = {
            'Year': years,
            'Reported': reported_values,
            'Estimate': estimated_values
        }

        # Convert to DataFrame
        df = pd.DataFrame(data)

    df = df[df['Estimate'] != '—']
        
    df["Estimate"] = df["Estimate"].apply(lambda x: convert_to_numeric(x))
    df["Year"] = df["Year"].astype('int')

    df = df[df.Year >= datetime.now().year][['Year','Estimate']]

    df.columns = ["year","eps_estimate"]

    return df

def get_revenue(driver):
    # get the element
    revenue = driver.find_element(By.XPATH, '//*[@id="js-category-content"]/div[2]/div/div[1]/div[2]/div/div[7]/div[2]/div/div[1]')
    html = revenue.get_attribute("innerHTML")

    # Parse the HTML
    soup = BeautifulSoup(html, 'html.parser')

    # Extract the years (column headers)
    years = [div.get_text() for div in soup.select('.values-OWKkVLyj .value-OxVAcLqi')]

    try:
        # Extract the reported values (data)
        reported_values = [div.get_text().strip() for div in soup.select('[data-name="Reported"] .value-OxVAcLqi')]

        # Cleaned list
        reported_values = [clean_value(val) if val != '—' else val for val in reported_values]

        estimated_values = [div.get_text().strip() for div in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]

        # Cleaned list
        estimated_values = [clean_value(val) if val != '—' else val for val in estimated_values]

        # Create a dictionary for the table
        data = {
            'Year': years,
            'Reported': reported_values,
            'Estimate': estimated_values
        }
        
        # Convert to DataFrame
        df = pd.DataFrame(data)

    except:
        years = years[2:]

        # Extract the reported values (data)
        reported_values = [div.get_text().strip() for div in soup.select('[data-name="Reported"] .value-OxVAcLqi')]

        # Cleaned list
        reported_values = [clean_value(val) if val != '—' else val for val in reported_values]

        estimated_values = [div.get_text().strip() for div in soup.select('[data-name="Estimate"] .value-OxVAcLqi')]

        # Cleaned list
        estimated_values = [clean_value(val) if val != '—' else val for val in estimated_values]

        # Create a dictionary for the table
        data = {
            'Year': years,
            'Reported': reported_values,
            'Estimate': estimated_values
        }

        # Convert to DataFrame
        df = pd.DataFrame(data)

    df = df[df['Estimate'] != '—']
        
    df["Estimate"] = df["Estimate"].apply(lambda x: convert_to_numeric(x))
    df["Year"] = df["Year"].astype('int')

    df = df[df.Year >= datetime.now().year][['Year','Estimate']]

    df.columns = ["year","revenue_estimate"]

    return df
    
# Get supabase connection
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# get active stock data
active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data)

active_stock[['symbol','index']] = active_stock["symbol"].str.split('.',expand=True)

# Initiate Selenium
cService = webdriver.ChromeService(executable_path='./chromedriver')
driver = webdriver.Chrome(service = cService)

driver.maximize_window()

# Start Scraping
df_fore = pd.DataFrame()

for i in range(0,active_stock.shape[0]):
    symbol = active_stock.symbol.iloc[i]
    url = f"https://www.tradingview.com/symbols/IDX-{symbol}/forecast/"

    driver.get(url)
    time.sleep(5)

    try:
        # Scroll to eps growth section
        ## Locate the element by XPath
        element = driver.find_element(By.XPATH, '/html/body/div[3]/div[4]/div[3]/div[2]/div/div[1]/div[1]/div[2]/div[2]/div')  # Replace with the actual XPath

        ## Scroll to the element using JavaScript execution
        driver.execute_script("arguments[0].scrollIntoView();", element)

        ann = driver.find_element(By.XPATH, '/html/body/div[3]/div[4]/div[3]/div[2]/div/div[1]/div[2]/div/div[1]/div/div/div/button[1]')

        ann.click()

        time.sleep(5)

        # Scroll to Revenue Section
        ## Locate the element by XPath
        element = driver.find_element(By.XPATH, '/html/body/div[3]/div[4]/div[3]/div[2]/div/div[1]/div[2]')  # Replace with the actual XPath

        ## Scroll to the element using JavaScript execution
        driver.execute_script("arguments[0].scrollIntoView();", element)

        ann = driver.find_element(By.XPATH, '/html/body/div[3]/div[4]/div[3]/div[2]/div/div[1]/div[2]/div/div[5]/div/div/div/button[1]')

        ann.click()

        time.sleep(5)

        # get revenue data
        eps = get_eps_growth(driver)

        # get revenue data
        revenue = get_revenue(driver)

        growth = revenue.merge(eps, on="year")

        growth["symbol"] = symbol

        df_fore = pd.concat([df_fore,growth])

        print(f"Finish for stock {symbol}")
    
    except:
        print(f"No forecast data for stock {symbol}")