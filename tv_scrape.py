from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import os 
import time
import re
import logging
from imp import reload

from datetime import datetime
import pandas as pd

from supabase import create_client
import pandas as pd
from dotenv import load_dotenv

from pyvirtualdisplay import Display

def clean_value(value):
    # Remove Unicode control characters using regex
    cleaned_value = re.sub(r'[\u202a\u202c\u202f]', '', value)

    # Replace Unicode minus (U+2212) with ASCII hyphen-minus (U+002D)
    cleaned_value = cleaned_value.replace('−', '-')
    
    return cleaned_value

def convert_to_numeric(value):
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
    

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program add incomplete stock started')

# Initiate logging
LOG_FILENAME = 'scraping.log'
initiate_logging(LOG_FILENAME)

# Get supabase connection
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

supabase = create_client(url, key)

# get active stock data
active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data)

active_stock[['symbol','index']] = active_stock["symbol"].str.split('.',expand=True)

display = Display(visible=0, size=(1200, 1200))  
display.start()

chrome_options = webdriver.ChromeOptions()    
# Add your options as needed    
options = [
  # Define window size here
   "--window-size=1200,1200",
    "--ignore-certificate-errors"
]

for option in options:
    chrome_options.add_argument(option)

driver = webdriver.Chrome(service=Service(), options=chrome_options)

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
    
df_fore.symbol = df_fore.symbol + ".JK"
df_fore['revenue_estimate'] = df_fore.revenue_estimate.astype('int')
df_fore["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")

def upsert_data(table,symbol, year, other_data,new_data):
    # Check if a row with the same symbol and year exists
    existing_entry = supabase.table(table)\
                             .select("*")\
                             .eq("symbol", symbol)\
                             .eq("year", year)\
                             .execute()

    if existing_entry.data:
        # Update the existing row if found
        result = supabase.table(table)\
                         .update(other_data)\
                         .eq("symbol", symbol)\
                         .eq("year", year)\
                         .execute()
        print(f"Updated: {result}")
    else:
        # Insert a new row if not found
        result = supabase.table(table).insert(new_data).execute()
        print(f"Inserted: {result}")

for i in range(0, df_fore.shape[0]):
    upsert_data("idx_company_forecast", df_fore.symbol.iloc[i],df_fore.year.iloc[i],df_fore[["revenue_estimate","eps_estimate"]].iloc[i:i+1].to_dict(orient="records"),df_fore.iloc[i:i+1].to_dict(orient="records"))

logging.info(f"Finish update forecast growth data in {pd.Timestamp.now(tz="GMT").strftime('%Y-%m-%d %H:%M:%S')}")
