import pandas as pd
import yfinance as yf
import psycopg2
import os
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')


df_yahf= yf.download(['TSLA','MSFT','AAPL'], start = formatted_date )
df_yahf.columns = df_yahf.columns.map('_'.join)
df_yahf.reset_index(inplace= True)

db_params = {
    "dbname": "postgres",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": "5432"
}


try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # SQL Insert Query
    insert_query = """
    INSERT INTO yahoo_finc (Date, Close_AAPL, Close_MSFT, Close_TSLA, High_AAPL,
       High_MSFT, High_TSLA, Low_AAPL, Low_MSFT, Low_TSLA,
       Open_AAPL, Open_MSFT, Open_TSLA, Volume_AAPL, Volume_MSFT,
       Volume_TSLA)
    VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s ,%s,%s)
    ON CONFLICT (Date) DO NOTHING;  -- Avoids duplicate primary key errors
    """
    
    # Insert DataFrame records one by one
    for _, row in df_yahf.iterrows():
        cursor.execute(insert_query, (
            row['Date'], row['Close_AAPL'], row['Close_MSFT'],  row['Close_TSLA'], row['High_AAPL'], row['High_MSFT'],
            row['High_TSLA'],row['Low_AAPL'], row['Low_MSFT'], row['Low_TSLA'],  row['Open_AAPL'], row['Open_MSFT'], row['Open_TSLA'],
            row['Volume_AAPL'],row['Volume_MSFT'],row['Volume_TSLA']
        ))

    # Commit and close
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        cursor.close()
        conn.close()