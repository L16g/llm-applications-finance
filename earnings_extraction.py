import pandas as pd
import numpy as np
import time
from urllib.request import urlopen
import ssl
import json
import my_secrets

api_key = my_secrets.FMP_API_KEY

def get_jsonparsed_data(url):
    """
    Receive the content of ``url``, parse it as JSON and return the object.
    Modified from Financial Modeling Prep.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    ssl_context = ssl.create_default_context()
    response = urlopen(url, context=ssl_context)
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_sp500_companies(n):
    '''Returns a list of length n of S&P 500 companies chosen at random'''
    ticker = 'spy'
    sp500_url = f'https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx'
    data = pd.read_excel(sp500_url)

    # set header as the row that includes "ticker" in the second column
    mask = data.iloc[:,1] == 'Ticker'
    data.columns = data[mask].values[0]
    # get rid of rows above header
    row_number = mask[mask == True].index[0]
    rows_to_drop = [row for row in range(row_number + 1)]
    _holdings = data.drop(rows_to_drop)
    # drop NAs
    sp500_holdings = _holdings.dropna(how='all').dropna(how='all', axis=1).dropna(how='any')

    holdings = sp500_holdings.Ticker.to_list()
   
   # choosing n companies at random
    l_idx = []
    for _ in range(n):
        idx = round(np.random.rand() * 500, 0)
        l_idx.append(int(idx))
    return [holdings[val] for val in l_idx]

# fiscal years and quarters to extract
quarters = ['1','2','3','4']
years = ['2018', '2019', '2020', '2021', '2022']

# holdings = get_sp500_companies(25)

holdings = ['WMT', 'EQIX', 'CMG', 'MCHP', 'VTRS', 'RMD', 'PEG',
            'PEP', 'CI', 'HON', 'BALL', 'CPB', 'MRO', 'NVDA',
            'PARA', 'MTCH', 'ETSY', 'EMN', 'WBD', 'CINF',
            'LDOS', 'CE', 'SBAC', 'NOW', 'MDLZ'
            ]

all_data = []
for ticker in holdings:
  for year in years:
    for quarter in quarters:

      url = f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?quarter={quarter}&year={year}&apikey={api_key}"
      print(f'Getting {ticker} {year}Q{quarter} transcript...')
      data = get_jsonparsed_data(url)
      pd.DataFrame(data).to_csv(f'Transcripts/Transcript_{ticker}_{year}Q{quarter}.txt', index=False)
      all_data += data
    time.sleep(1) # the api can handle 5 hits per second, by sleeping 1 second in between each year, we can ensure we don't hit the rate limit

df = pd.DataFrame(all_data)
df.to_csv('EarningsTranscriptData.txt', index=False)
