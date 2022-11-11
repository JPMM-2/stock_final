
import sqlite3
from sqlite3 import Error
import yfinance as yf
import time
from datetime import datetime


start_at    = datetime.now()


def get_data(startt, endd, list_tickers, what):
    hist = []
    msft = yf.Ticker(list_tickers[0])
    hist = msft.history(start = startt, end = endd)
    hist['Symbol'] = list_tickers[0]
    
    x = time.time()
    
    #for n, i in enumerate(list_tickers[1:]):
    for i in list_tickers[1:]:
        partial = []
        #print(n);print(i)
        msft = yf.Ticker(i)
        partial = msft.history(start = startt, end = endd)
        partial['Symbol'] = i
        hist = partial.append(hist)
    
    y = time.time()
    
    print (str(what) + '  -  ' + str(y-x))
    
    list_tickers = ['hello']
    
    return hist


def shift_ (df, period):

    df = df.copy()
    df['Prev_symbol'] = df['Symbol'].shift(periods = period)
    df['Prev_open'] = df['Open'].shift(periods = period)
    df['shift'] = period
    df = df[df['Symbol'] == df['Prev_symbol']]
    df['Performance'] = (df['Close'] - df['Prev_open'])/df['Prev_open']
    df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'
             , 'Symbol', 'Prev_symbol', 'Prev_open', 'shift','Performance']]
    
    return df


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


# =============================================================================
# startt = '2022-04-01'
# endd = '2022-04-02'
# 
# 
# =============================================================================

listt = open('list_symbols.txt', 'r').read().replace('\n', '').split(',')

df = get_data('2017-01-01', '2022-06-16', listt,1)
df_perf = shift_(df,3)
            
conn = sqlite3.connect('sqliteDB.db')

df_perf.to_sql('data',con=conn,if_exists='append')





end_at    = datetime.now()


print (end_at - start_at)



