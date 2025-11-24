import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

import cloudscraper
from bs4 import BeautifulSoup
import asyncio
import json
import warnings
import urllib3
import pandas as pd
import ccxt.async_support as ccxt

async def fetch_price_data(ticker='AAVE/USDT', start_date_str='2020-01-01'): 
    print(f"Fetching ALL historical {ticker} price data from Gate.io since {start_date_str}...")
    
    gateio = ccxt.gateio()
    
    all_ohlcv = []
    since_timestamp = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp() * 1000)
    limit = 1000 
    try:
        while True:
            print(f"Fetching chunk since timestamp {since_timestamp}...")
            ohlcv_chunk = await gateio.fetch_ohlcv(ticker, '1d', since=since_timestamp, limit=limit)  
            if not ohlcv_chunk:
                print("No more data found for this period.")
                break               
            all_ohlcv.extend(ohlcv_chunk)
            last_timestamp_ms = ohlcv_chunk[-1][0]
            since_timestamp = last_timestamp_ms + 1 
            print(f"Fetched {len(ohlcv_chunk)} candles up to {datetime.fromtimestamp(last_timestamp_ms/1000)}. Next fetch starts after this.")
            await asyncio.sleep(gateio.rateLimit / 1000) 
            
    except ccxt.NetworkError as e: print(f"ccxt Network Error: {e}. Stopping fetch.")
    except ccxt.ExchangeError as e: print(f"ccxt Exchange Error: {e}. Stopping fetch.")
    except Exception as e: print(f"An unexpected error occurred during fetch: {e}")
    finally:
        print("Closing exchange connection...")
        await gateio.close()
    if not all_ohlcv: 
        print(f"Warning: No price data fetched at all for {ticker}.")
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

async def fetch_all_data():
    """Runs all async functions sequentially."""
    print("Debug: Starting Step 1 - Fetching TVL data...")
    tvl_url = "https://defillama.com/protocol/aave"
    tvl_data_raw = await scrape_aave_tvl_raw(tvl_url)
    
    if not tvl_data_raw:
        print("Debug: TVL data fetch failed.")
        return None, None

    print("Debug: Step 1 SUCCESS.")
    print("Debug: Starting Step 2 - Finding oldest date...")
    oldest_date_str = '2020-01-01'
    try:
        oldest_timestamp_sec = min(int(item[0]) for item in tvl_data_raw if item and len(item) > 0)
        oldest_date = datetime.fromtimestamp(oldest_timestamp_sec)
        oldest_date_str = oldest_date.strftime('%Y-%m-%d')
        print(f"Debug: Step 2 SUCCESS. Oldest date found: {oldest_date_str}.")
    except Exception as e:
        print(f"Debug: Step 2 FAILED: {e}. Using default {oldest_date_str}.")
    
    print(f"Debug: Starting Step 3 - Fetching Price data since {oldest_date_str}...")
    price_data_raw = await fetch_price_data('AAVE/USDT', oldest_date_str)
    
    if not price_data_raw:
        print("Debug: Price data fetch failed.")
        return tvl_data_raw, None

    print("Debug: Step 3 SUCCESS.")
    return tvl_data_raw, price_data_raw

@st.cache_data(ttl=3600, show_spinner=False)
def load_master_data():
    """
    A synchronous wrapper function that runs the async fetch
    and processes the data. This is what Streamlit caches.
    """
    st.session_state.just_fetched = True
    print("CACHE MISS: Running async data fetching...")
    
    tvl_raw, price_raw = asyncio.run(fetch_all_data())
    
    if tvl_raw is None or price_raw is None:
        st.error("Failed to fetch data from APIs. Please check logs.")
        return pd.DataFrame()

    print("Debug: Starting Step 4 - Processing and merging data...")
    merged_df = process_and_merge(tvl_raw, price_raw)
    
    if merged_df.empty:
        print("Debug: Step 4 WARNING. Merge result is empty.")
    else:
        print("Debug: Step 4 SUCCESS. Data processing complete. Storing in cache.")
        
    return merged_df

def filter_dataframe(df, timeframe_option):
    """Filters DataFrame based on the selected timeframe."""
    if df.empty:
        return df, "N/A", "N/A" 
        
    end_date = df['date'].max() 

    if timeframe_option == "1 Week":
        start_date = end_date - timedelta(weeks=1)
    elif timeframe_option == "1 Month":
        start_date = end_date - timedelta(days=30)
    elif timeframe_option == "YTD":
        start_date = datetime(end_date.year, 1, 1) 
    elif timeframe_option == "1 Year":
        start_date = end_date - timedelta(days=365) 
    elif timeframe_option == "Full":
        start_date = df['date'].min() 
    else: 
        start_date = df['date'].min()

    filtered_df = df[df['date'] >= start_date].copy() 

    start_date_str = filtered_df['date'].min().strftime('%Y-%m-%d')
    end_date_str = filtered_df['date'].max().strftime('%Y-%m-%d')

    return filtered_df, start_date_str, end_date_str

def get_correlation_interpretation(corr_value):
    """Provides a simple text interpretation of the correlation value."""
    if pd.isna(corr_value):
        return "Not available (insufficient data)."
        
    strength = ""
    direction = ""

    abs_corr = abs(corr_value)

    if abs_corr >= 0.7:
        strength = "Strong"
    elif abs_corr >= 0.4:
        strength = "Moderate"
    elif abs_corr >= 0.1:
        strength = "Weak"
    else:
        strength = "Very Weak / No clear"

    if corr_value > 0.1:
        direction = "Positive"
    elif corr_value < -0.1:
        direction = "Negative"
    else:
        direction = "Linear Relationship"

    if strength.startswith("Very Weak"):
        return f"{strength} {direction}."
    else:
        return f"{strength} {direction} Correlation."
    
def display_bidirectional_progress(col, corr_value):
    COLOR_POSITIVE_RGBA = "rgba(36, 161, 72, 1.0)" 
    COLOR_NEGATIVE_RGBA = "rgba(255, 75, 75, 1.0)"
    TRACK_COLOR = "rgba(51, 51, 51, 1.0)"

    abs_val = abs(corr_value)

    bar_main_color = COLOR_POSITIVE_RGBA if corr_value >= 0 else COLOR_NEGATIVE_RGBA
    bar_color = f"linear-gradient(to right, {bar_main_color}, {bar_main_color})"
    width_percent = abs_val * 50
    
    fill_style = f"width: {width_percent}%; background: {bar_color};"
    empty_style = f"width: {50 - width_percent}%; background-color: {TRACK_COLOR};"

    if corr_value >= 0:
        div1_style = f"width: 50%; background-color: {TRACK_COLOR};"
        div2_style = fill_style
        div3_style = empty_style
        
        html_code = f"""
        <div style="display: flex; height: 8px; width: 100%; border-radius: 4px; overflow: hidden;">
            <div style="{div1_style}"></div>
            <div style="{div2_style}"></div>
            <div style="{div3_style}"></div>
        </div>
        """
        
    else: 
        div1_style = empty_style
        div2_style = fill_style
        div3_style = f"width: 50%; background-color: {TRACK_COLOR};"

        html_code = f"""
        <div style="display: flex; height: 8px; width: 100%; border-radius: 4px; overflow: hidden;">
            <div style="{div1_style}"></div>
            <div style="{div2_style}"></div>
            <div style="{div3_style}"></div>
        </div>
        """
    
    col.markdown(html_code, unsafe_allow_html=True)

with st.spinner("Please wait, fetching the latest data from DefiLlama and Gate.io (via ccxt)..."):
    df_full = load_master_data()


if 'just_fetched' in st.session_state and st.session_state.just_fetched:
    st.toast("Data successfully loaded!",duration=3)
    
    st.session_state.just_fetched = False


st.set_page_config(layout="wide", page_title="AAVE TVL vs Price Analysis")

st.markdown("""
<style>
div.stRadio > div[role="radiogroup"] {
    display: flex;
    flex-direction: row;
    justify-content: center;
    padding: 0;
    border: 1px solid #444;
    border-radius: 0.5rem;
    overflow: hidden;
    width: fit-content; 
    margin: 0 auto; 
}
            
div.stRadio > div[role="radiogroup"] > label {
    display: flex !important;      
    align-items: center !important;  
    justify-content: center !important;
    
    padding: 0.5rem 1rem;
    margin: 0;
    border: none;
    border-right: 1px solid #444;
    background-color: transparent;
    color: #CCC;
    cursor: pointer;
    transition: background-color 0.3s ease, color 0.3s ease;   
}
            
div.stRadio > div[role="radiogroup"] > label[data-baseweb="radio"] > div{
    display: flex;
    align-items: center !important;  
    justify-content: center !important;
}
            
div.stRadio > div[role="radiogroup"] > label:last-child {
    border-right: none;
}

div.stRadio > div[role="radiogroup"] > label > div[data-testid="stMarkdownContainer"] > p {
    margin-bottom: 0 !important; 
}
            
div.stRadio > div[role="radiogroup"] > label[aria-checked="true"] {
    background-color: #007bff !important; 
    color: white !important;
}
            
div.stRadio > div[role="radiogroup"] > label:hover {
    background-color: #333;
}
            
div.stRadio > div > label > div:first-child {
    display: none !important;
}
            
div.stRadio input[type="radio"] {
    display: none !important;
}

div.stRadio > div[role="radiogroup"] > label[data-baseweb="radio"]:has(input:checked) {
    background-color: #007bff !important;
    color: white !important;
    border-color: #007bff !important; 
}                     
</style>
""", unsafe_allow_html=True)

max_date = datetime.today()
min_date = max_date - timedelta(days=365)

if df_full.empty:
    st.warning("Could not load data. Please check the CSV file.")
else:
    min_date = df_full['date'].min()
    max_date = df_full['date'].max()

col_pad, col_method, col_radio, col_reset = st.columns([6, 1.2, 3, 1])

with col_pad:
    st.markdown("<h1>📊 AAVE: TVL vs. Price Correlation Analysis</h1>", unsafe_allow_html=True)
    st.markdown(
        """
        <p style="margin-top:-10px; margin-bottom:10px;">
        Exploring the relationship between Aave's Total Value Locked (TVL) and its token price (AAVE/USDT).
        </p>
        """, unsafe_allow_html=True
    )

with col_method:
    st.markdown("")
    with st.popover("📚 Methodology"):
        st.markdown("### AAVE Analysis: Methodology & Interpretation")
        st.markdown(
            """
            **What this application does:**

            1.  **Loads Data:** Reads the pre-processed CSV file containing daily Aave TVL (scraped from [DefiLlama's initial HTML](https://defillama.com/protocol/aave)) and [AAVE/USDT price data](https://www.gate.com/trade/AAVE_USDT) (fetched from Gate.io via `ccxt`).
            2.  **Filters Data:** Allows you to select a specific timeframe (e.g., 1 Year, 1 Month) to focus the analysis.
            3.  **Visualizes:** Displays a dual-axis line chart showing how TVL (orange, right axis) and Price (blue, left axis) have moved together over the selected period.
            4.  **Calculates Correlation:** Computes the Pearson correlation coefficient between TVL and Price for the chosen timeframe. This measures the *linear* relationship between the two variables.
            
            ---

            **How to Read the Correlation Score:**

            * **Range:** The score ranges from **-1.0 to +1.0**.
            * **+1.0:** Perfect positive linear correlation (As TVL goes up, Price goes up proportionally).
            * **0.0:** No linear correlation.
            
            ---
            
            **Understanding Lagged Correlation (TVL Today vs. Price Tomorrow):**

            * **Purpose:** This calculation tests a specific hypothesis: *Does a change in TVL **today** potentially influence the Price **tomorrow**?* It checks if TVL acts as a **leading indicator** for price movement.
            * **How it Works (Lag 1 Day):** We calculate the correlation between today's TVL and *tomorrow's* Price values (by mathematically "shifting" the price data one day forward).
            * **Interpretation:** A strong positive lagged correlation (e.g., 1 Day, 7 Days) suggests that increases in TVL tend to **precede** price increases.

            **Disclaimer:** Correlation does not imply causation. These analyses show historical relationships but do not guarantee future results. This tool is for informational and educational purposes only, not financial advice.
            """
        )

with col_radio:
    st.markdown("")
    timeframe_options = ["1w", "1m", "3m", "6m", "YTD", "1y", "All"]

    initial_index = timeframe_options.index("All") 

    selected_timeframe = st.radio(
        "Time Range Selector",
        timeframe_options,
        index=initial_index,
        key='selected_tf', 
        horizontal=True,
        label_visibility="collapsed"
    )

end_date = max_date
start_date = min_date

if selected_timeframe == "1w":
    start_date = max(end_date - timedelta(days=7), min_date) 
elif selected_timeframe == "1m":
    start_date = max(end_date - timedelta(days=30), min_date) 
elif selected_timeframe == "3m":
    start_date = max(end_date - timedelta(days=90), min_date)
elif selected_timeframe == "6m":
    start_date = max(end_date - timedelta(days=180), min_date)
elif selected_timeframe == "YTD":
    ytd_start = datetime(end_date.year, 1, 1)
    start_date = max(ytd_start, min_date)
elif selected_timeframe == "1y":
    start_date = max(end_date - timedelta(days=365), min_date)


df_filtered_for_corr = df_full[df_full['date'] >= start_date].copy()

with col_reset:
    st.markdown("")
    def reset_view_callback():
        st.session_state.selected_tf = "All"
    if st.button("Reset View", help="Reset chart view to selected time range", on_click=reset_view_callback):
        st.rerun()

fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Scatter(x=df_full['date'], y=df_full['tvl'], name="TVL (USD)", line=dict(color='orange')),
    secondary_y=True,
)

fig.add_trace(
    go.Scatter(x=df_full['date'], y=df_full['price'], name="Price (USD)", line=dict(color='green')),
    secondary_y=False,
)

fig.update_layout(
    xaxis_title="Date",
    hovermode="x unified",
    xaxis_range=[start_date, end_date],
    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    margin=dict(l=20, r=20, t=50, b=20),
    xaxis=dict(fixedrange=True)
)
fig.update_yaxes(title_text="<b>Price (USD)</b>", secondary_y=False, color='green', gridcolor='rgba(100,100,100,0.1)', fixedrange=True) 
fig.update_yaxes(title_text="<b>TVL (USD)</b>", secondary_y=True, color='orange', rangemode='tozero', gridcolor='rgba(100,100,100,0.1)', fixedrange=True)

config = {
      'modeBarButtonsToRemove': ['zoom', 'pan', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'toImage', 'resetScale2d'],  
      'displaylogo': False
    }

st.plotly_chart(fig, use_container_width=True, config=config)
    
st.subheader(f"Correlation Analysis ({selected_timeframe} Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")

if df_filtered_for_corr.empty or len(df_filtered_for_corr) < 2:
    st.warning("Not enough data for correlation in the selected timeframe.")
else:
    correlation = df_filtered_for_corr['tvl'].corr(df_filtered_for_corr['price'])
    lagged_corr = df_filtered_for_corr['tvl'].corr(df_filtered_for_corr['price'].shift(-1)) if len(df_filtered_for_corr) > 1 else pd.NA
    lagged_corr_7d = df_filtered_for_corr['tvl'].corr(df_filtered_for_corr['price'].shift(-7)) if len(df_filtered_for_corr) > 7 else pd.NA
    lagged_corr_30d = df_filtered_for_corr['tvl'].corr(df_filtered_for_corr['price'].shift(-30)) if len(df_filtered_for_corr) > 30 else pd.NA

    corr_display = f"{correlation:.3f}" if pd.notna(correlation) else "Min Range 1 Week"
    lag1d_display = f"{lagged_corr:.3f}" if pd.notna(lagged_corr) else "Min Range 1 Week"
    lag7d_display = f"{lagged_corr_7d:.3f}" if pd.notna(lagged_corr_7d) else "Min Range 1 Month"
    lag30d_display = f"{lagged_corr_30d:.3f}" if pd.notna(lagged_corr_30d) else "Min Range 3 Month"

    col_c1, col_c2, col_c3, col_c4 = st.columns(4)

    with col_c1:
        st.metric(label="TVL vs. Price Correlation (0-Day)", value=corr_display) 
        st.markdown(f"**Interpretation:** {get_correlation_interpretation(correlation)}")
        if pd.notna(correlation): 
            display_bidirectional_progress(col_c1, correlation) 
        else:
            st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True) 

    with col_c2:
        st.metric(label="Lag Correlation: 1 Day", value=lag1d_display)
        st.markdown(f"**Interpretation:** {get_correlation_interpretation(lagged_corr)}")
        if pd.notna(lagged_corr): 
            display_bidirectional_progress(col_c2, lagged_corr) 
        else:
            st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

    with col_c3:
        st.metric(label="Lag Correlation: 1 Week", value=lag7d_display)
        st.markdown(f"**Interpretation:** {get_correlation_interpretation(lagged_corr_7d)}")
        if pd.notna(lagged_corr_7d): 
            display_bidirectional_progress(col_c3, lagged_corr_7d)
        else:
            st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

    with col_c4:
        st.metric(label="Lag Correlation: 1 Month", value=lag30d_display) 
        st.markdown(f"**Interpretation:** {get_correlation_interpretation(lagged_corr_30d)}")
        if pd.notna(lagged_corr_30d): 
            display_bidirectional_progress(col_c4, lagged_corr_30d)
        else:
            st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

