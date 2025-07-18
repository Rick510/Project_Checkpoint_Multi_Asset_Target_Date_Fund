
## -- Pls Manually run below code
##############################################################################################################################



import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from io import StringIO

# Getting Tickers from Vanguard website to search on yf
@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=60))
def fetch_html_tables(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text), header=0)
    if not tables:
        raise ValueError("Did not find any table")
    return tables[0]

def fetch_target_retirement_tickers() -> list[str]:
    url = "https://investor.vanguard.com/investment-products/mutual-funds/target-retirement-funds"
    try:
        df = fetch_html_tables(url)
    except Exception as e:
        print(f"Fail to Fetch：{e}")
        return []

    if "Fund name" not in df.columns:
        print(f"Cannot fetch 'Fund name' column，the column exists now is：{df.columns.tolist()}")
        return []

    tickers = (
        df["Fund name"]
        .str.extract(r"([A-Z]{5,6})", expand=False).dropna().unique().tolist())
    return tickers

if __name__ == "__main__":
    tickers_list = fetch_target_retirement_tickers()
    print(tickers_list)






## -- Manually execute above code to here 
#######################################################################
#######################################################################
## -- Pls Manually run below code
# Create a pipeline for Porfolio Performance

import yfinance as yf
import pandas as pd

def get_portfolio_performance(
    tickers: list[str],
    start_date: str,
    end_date: str,
    frequency: str = "D"  # frequency options: 'D', 'W', 'M', 'Y'
) -> pd.DataFrame:
    # 1. Download daily price data, only Close
    raw = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        interval="1d",
        progress=False,        
    )

    # 2. Select Close column
    if isinstance(raw.columns, pd.MultiIndex):
        price = raw.xs("Close", axis=1, level=0)
    else:
        price = raw[["Close"]].copy()
        price.columns = tickers if isinstance(tickers, list) else [tickers]

    # 3. Calculate PERFORMANCEFACTOR (rate of change based on frequency)
    if frequency == "D":
        perf = price.pct_change()
    else:
        perf = price.resample(frequency).last().pct_change()

    # 4. Reshape DataFrame
    df = perf.stack().reset_index()
    df.columns = ["HISTORYDATE", "PORTFOLIOCODE", "PERFORMANCEFACTOR"]

    # 5. Fill other static columns
    df["CURRENCYCODE"]             = "USD"
    df["CURRENCY"]                 = "US Dollar"
    df["PERFORMANCECATEGORY"]      = pd.NA
    df["PERFORMANCECATEGORYNAME"]  = pd.NA
    df["PERFORMANCETYPE"]          = pd.NA
    df["PERFORMANCEINCEPTIONDATE"] = pd.NA
    df["PORTFOLIOINCEPTIONDATE"]   = pd.NA
    df["PERFORMANCEFREQUENCY"]     = frequency

    # 6. Format date column
    df["HISTORYDATE"] = pd.to_datetime(df["HISTORYDATE"]).dt.strftime("%Y-%m-%d")

    # 7. Reorder columns and return
    cols = [
        "PORTFOLIOCODE", "HISTORYDATE", "CURRENCYCODE", "CURRENCY",
        "PERFORMANCECATEGORY", "PERFORMANCECATEGORYNAME",
        "PERFORMANCETYPE", "PERFORMANCEINCEPTIONDATE",
        "PORTFOLIOINCEPTIONDATE", "PERFORMANCEFREQUENCY",
        "PERFORMANCEFACTOR"
    ]
    return df[cols]


# Example usage
df_portfolio_performance = get_portfolio_performance(
    tickers=["VTINX", "VNQ"],
    start_date="2014-12-31",
    end_date="2024-12-31",
    frequency="M"
)

print(df_portfolio_performance)




## -- Manually execute above code to here 
#######################################################################
#######################################################################
## -- Pls Manually run below code
# Create a pipeline for Benchmark Performance


import yfinance as yf
import pandas as pd

def get_benchmark_performance(
    benchmark_ticker: str,
    start_date: str,
    end_date: str,
    frequency: str = "D"
) -> pd.DataFrame:
    raw = yf.download(
        benchmark_ticker,
        start=start_date, end=end_date,
        interval="1d",
        progress=False,
        auto_adjust=True
    )

    price = raw["Close"]
    if frequency != "D":
        price = price.resample(frequency).last()

    # Reset column name
    df = price.reset_index()
    df.columns = ["HISTORYDATE1", "VALUE"]

    # Fill the column name to make sure it identical with table in snowflake
    df["BENCHMARKCODE"]        = benchmark_ticker.lstrip("^")
    df["PERFORMANCEDATATYPE"]  = "Prices"
    df["CURRENCYCODE"]         = "USD"
    df["CURRENCY"]             = "US Dollar"
    df["PERFORMANCEFREQUENCY"] = frequency
    df["HISTORYDATE"] = pd.to_datetime(df["HISTORYDATE1"])\
                          .dt.strftime("%Y-%m-%d 00:00:00.000")

    cols = [
        "BENCHMARKCODE", "PERFORMANCEDATATYPE",
        "CURRENCYCODE",   "CURRENCY",
        "PERFORMANCEFREQUENCY",
        "VALUE",
        "HISTORYDATE1",   "HISTORYDATE"
    ]
    return df[cols]

df_benchmark_performance = get_benchmark_performance(
        benchmark_ticker="^GSPC",
        start_date="2004-01-01",
        end_date="2024-12-31",
        frequency="D"
    )
print(df_benchmark_performance)


df_test = df_benchmark_performance.iloc[[0]]
print(df_test)






## -- Manually execute above code to here 
#######################################################################
#######################################################################
## -- Pls Manually run below code
# Below Code will occur error since I Comment the password, When type correct Password the code can run

## Setting up account identification first
import os
import snowflake.connector


ctx = snowflake.connector.connect(
    user="YUHSIANGW",
    password="XXXXXXXXXXXXXX",
    account="ASSETTE-SSAPPOC",       # ← do not use Locator but Identifier 
    role="AST_MULTIASSET_DB_RW", 
    warehouse="AST_BU_WH",
    database="AST_MULTIASSET_DB",
    schema="DBO",
)


cs = ctx.cursor()

try:
    # Can change role (if needed)
    # cs.execute("USE ROLE ANALYST_ROLE")

    # Try whether connection success or nor
    cs.execute("SELECT CURRENT_VERSION()")
    one_row = cs.fetchone()
    print("Snowflake version:", one_row[0])
    # Try to Insert
    # Construct Insert query
    cols = df_test.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
    INSERT INTO AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE ({', '.join(cols)})
    VALUES ({placeholders})
    """
    # Construct data list 
    data = []
    for row in df_test.itertuples(index=False, name=None):
        converted = []
        for v in row:
            if isinstance(v, pd.Timestamp):
                converted.append(v.to_pydatetime())
            else:
                converted.append(v)
        data.append(tuple(converted))

    # batch execute
    cs.executemany(sql, data)
    ctx.commit()
    print(f"Inserted {cs.rowcount} rows via executemany.")


    # validate the result
    cs.execute("SELECT * FROM AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE WHERE BENCHMARKCODE = %s", ("GSPC",))
    for r in cs.fetchall():
        print(r)


finally:
    # Make sure to close cursor connection
    cs.close()
    ctx.close()

## -- Manually execute above code to here 
#######################################################################
#######################################################################
## -- Pls Manually run below code

# Below Code will occur error since I Comment the password, When type correct Password the code can run

import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

## Setting up account identification first

ctx = snowflake.connector.connect(
    user="YUHSIANGW",
    password="XXXXXXXXXXXXXXXX",
    account="ASSETTE-SSAPPOC",       # do not use Locator but Identifier 
    role="AST_MULTIASSET_DB_RW",
    warehouse="AST_BU_WH",
    database="AST_MULTIASSET_DB",
    schema="DBO",
)

cs = ctx.cursor()

try:
    # 2. Create a temporal table in snowflake, need to make sure the column is identical
    cs.execute("""
    CREATE OR REPLACE TEMPORARY TABLE tmp_benchmarkperformance
    LIKE AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE
    """)

    # 3. Use write_pandas to batch upload the DataFrame to this temporary table 
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=df_benchmark_performance,
        table_name="tmp_benchmarkperformance",
        database="AST_MULTIASSET_DB",
        schema="DBO"
    )
    print(f"Staged {nrows} rows into tmp_benchmarkperformance across {nchunks} chunks")

    # 4. MERGE：when(BENCHMARKCODE, HISTORYDATE1) not exist, then insert 
    cs.execute("""
    MERGE INTO AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE AS target
    USING AST_MULTIASSET_DB.DBO.tmp_benchmarkperformance   AS src
      ON target.BENCHMARKCODE = src.BENCHMARKCODE
     AND target.HISTORYDATE1  = src.HISTORYDATE1
    WHEN NOT MATCHED THEN
      INSERT (
        BENCHMARKCODE, PERFORMANCEDATATYPE, CURRENCYCODE, CURRENCY,
        PERFORMANCEFREQUENCY, VALUE, HISTORYDATE1, HISTORYDATE
      )
      VALUES (
        src.BENCHMARKCODE, src.PERFORMANCEDATATYPE, src.CURRENCYCODE, src.CURRENCY,
        src.PERFORMANCEFREQUENCY, src.VALUE, src.HISTORYDATE1, src.HISTORYDATE
      )
    """)
    print(f"MERGE completed, {cs.rowcount} rows inserted")

finally:
    cs.close()
    ctx.close()

## -- Manually execute above code to here 
#######################################################################
