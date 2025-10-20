import cloudscraper
from bs4 import BeautifulSoup
import asyncio
import json
import warnings
import urllib3
import pandas as pd
from datetime import datetime
import ccxt.async_support as ccxt

async def fetch_price_data(ticker='AAVE/USDT', start_date_str='2020-01-01'): 
    print(f"Fetching ALL historical {ticker} price data from Binance since {start_date_str}...")
    binance = ccxt.binance()
    all_ohlcv = []
    since_timestamp = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp() * 1000)
    limit = 1000 
    try:
        api_desc = binance.describe().get('api', {})
        klines_desc = api_desc.get('publicGetKlines', {})
        limits_desc = klines_desc.get('limits', {})
        limit = limits_desc.get('limit', 1000)
    except Exception:
        pass
    try:
        while True:
            print(f"Fetching chunk since timestamp {since_timestamp}...")
            ohlcv_chunk = await binance.fetch_ohlcv(ticker, '1d', since=since_timestamp, limit=limit)
            if not ohlcv_chunk:
                print("No more data found for this period.")
                break
            all_ohlcv.extend(ohlcv_chunk)
            last_timestamp_ms = ohlcv_chunk[-1][0]
            since_timestamp = last_timestamp_ms + 1 
            print(f"Fetched {len(ohlcv_chunk)} candles up to {datetime.fromtimestamp(last_timestamp_ms/1000)}. Next fetch starts after this.")
            await asyncio.sleep(binance.rateLimit / 1000) 
    except ccxt.NetworkError as e: print(f"ccxt Network Error: {e}. Stopping fetch.")
    except ccxt.ExchangeError as e: print(f"ccxt Exchange Error: {e}. Stopping fetch.")
    except Exception as e: print(f"An unexpected error occurred during fetch: {e}")
    finally: await binance.close()
    if not all_ohlcv: print(f"Warning: No price data fetched at all for {ticker}.")
    else:
        unique_timestamps = set()
        unique_ohlcv = []
        for candle in all_ohlcv:
            if candle[0] not in unique_timestamps:
                unique_ohlcv.append(candle)
                unique_timestamps.add(candle[0])
        print(f"Successfully fetched a total of {len(unique_ohlcv)} unique price data points.")
        return unique_ohlcv
    return []

def process_and_merge(tvl_data_raw, price_data_raw):
    print("Processing and merging data...")

    if not tvl_data_raw:
        print("Merge process cancelled: TVL data is empty.")
        return pd.DataFrame()

    processed_tvl = []
    for item in tvl_data_raw:
         try:
             timestamp_sec = int(item[0])
             value_float = float(item[1])
             processed_tvl.append({'timestamp': timestamp_sec, 'tvl': value_float})
         except (ValueError, IndexError, TypeError): pass 

    if not processed_tvl:
        print("Merge process cancelled: No valid TVL rows after processing.")
        return pd.DataFrame()

    df_tvl = pd.DataFrame(processed_tvl)
    df_tvl['date'] = pd.to_datetime(df_tvl['timestamp'], unit='s').dt.normalize()
    df_tvl = df_tvl[['date', 'tvl']] 

    print(f"\n--- TVL Data Range ---")
    print(f"Start Date: {df_tvl['date'].min()}")
    print(f"End Date:   {df_tvl['date'].max()}")
    print(f"Shape:      {df_tvl.shape}")

    if not price_data_raw:
        print("Merge process cancelled: Price data is empty.")
        return pd.DataFrame()

    df_price = pd.DataFrame(price_data_raw, columns=['timestamp_ms', 'open', 'high', 'low', 'price', 'volume'])
    df_price = df_price[['timestamp_ms', 'price']]
    df_price['date'] = pd.to_datetime(df_price['timestamp_ms'], unit='ms').dt.normalize()
    df_price = df_price.drop(columns=['timestamp_ms'])

    print(f"\n--- Price Data Range (from ccxt) ---")
    print(f"Start Date: {df_price['date'].min()}")
    print(f"End Date:   {df_price['date'].max()}")
    print(f"Shape:      {df_price.shape}")

    print("Merging TVL and Price data based on date...")
    df_merged = pd.merge(df_tvl, df_price, on='date', how='inner')
    df_merged['tvl'] = pd.to_numeric(df_merged['tvl'])
    df_merged['price'] = pd.to_numeric(df_merged['price'])
    df_merged = df_merged.sort_values(by='date').reset_index(drop=True)
    print(f"Merge completed. Resulting data shape: {df_merged.shape}")
    return df_merged

async def scrape_aave_tvl_raw(url: str) -> list | None:
    print(f"Attempting to fetch TVL data from: {url} (using cloudscraper)...")
    scraper = cloudscraper.create_scraper(delay=10, browser='chrome')
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
    tvl_chart_data = None 

    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, scraper.get, url)

        if response.status_code == 200:
            print("Successfully bypassed Cloudflare and downloaded HTML.")
            soup = BeautifulSoup(response.text, 'html.parser')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if script_tag:
                print("Successfully found __NEXT_DATA__ tag.")
                try:
                    json_data = json.loads(script_tag.string) #type: ignore
                    tvl_chart_data = json_data.get('props', {}).get('pageProps', {}).get('tvlChartData', []) 

                    if tvl_chart_data:
                        print("Successfully extracted tvlChartData.")
                    else:
                        print("Warning: tvlChartData not found at path props -> pageProps.")
                        tvl_chart_data = []
                except json.JSONDecodeError:
                    print("Error: Failed to parse JSON within __NEXT_DATA__.")
                    tvl_chart_data = []
            else:
                print("Warning: Tag <script id='__NEXT_DATA__'> not found.")
                tvl_chart_data = []
        else:
            print(f"Warning: Failed after Cloudflare. Status Code: {response.status_code}")
            tvl_chart_data = []

    except Exception as e:
        print(f"Error during process: {e}")
        tvl_chart_data = []
    finally:
        if 'scraper' in locals():
            scraper.close()

    return tvl_chart_data

async def main():
    aave_url = "https://defillama.com/protocol/aave"
    
    print("Step 1: Fetching TVL data...")
    tvl_raw = await scrape_aave_tvl_raw(aave_url)

    start_date_for_price = '2020-01-01'
    if tvl_raw:
        print("Step 2: Determining oldest date from TVL data...")
        try:
            oldest_timestamp_sec = min(int(item[0]) for item in tvl_raw if item and len(item)>0 and item[0].isdigit())
            oldest_date = datetime.fromtimestamp(oldest_timestamp_sec)
            start_date_for_price = oldest_date.strftime('%Y-%m-%d')
            print(f"Oldest date found in TVL data: {start_date_for_price}. Using this for price fetch.")
        except (ValueError, TypeError) as e:
             print(f"Could not determine oldest date from TVL data ({e}), using default: {start_date_for_price}.")
    else:
        print("TVL data is empty, using default start date for price fetch.")

    print(f"Step 3: Fetching Price data since {start_date_for_price}...")
    price_raw = await fetch_price_data(start_date_str=start_date_for_price) 

    df_final = process_and_merge(tvl_raw, price_raw)

    if not df_final.empty:
        print("\n--- Merged Data Output (First 5 rows) ---")
        print(df_final.head())
        print("\n--- Merged Data Output (Last 5 rows) ---")
        print(df_final.tail())

        output_file = "aave_tvl_vs_price_merged.csv"
        df_final.to_csv(output_file, index=False)
        print(f"\nSuccess! Merged data saved to: {output_file}")
    else:
        print("\nFailed to merge data or data was empty.")

if __name__ == "__main__":
    asyncio.run(main())