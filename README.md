# sectors_forecast_growth_rate

source: trading view forecast menu

---------------

How to run (Run it every quarter):
- Download Chromedriver and put it in the root folder
- Create .env file consisting of `SUPABASE_URL` and `SUPABASE_KEY`
- Do pip install -r tv_requirements.txt
- Run python tv_scrape.py, if all stock get "No forecast data", please adjust the xpath for the scrolling and clicking the annual for both eps and revenue
