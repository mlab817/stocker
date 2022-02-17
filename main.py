# This is a sample Python script.
import logging

import pandas as pd
import psycopg2
import requests
import sys
import time

from AlmaIndicator import ALMAIndicator
from bs4 import BeautifulSoup
from datetime import datetime
from ta.momentum import RSIIndicator, WilliamsRIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD, CCIIndicator, TRIXIndicator, EMAIndicator, PSARIndicator
from ta.volatility import AverageTrueRange

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

base_url = 'https://frames.pse.com.ph/security/'

logging.basicConfig(filename="errors.log", level=logging.ERROR)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename="info.log")


def get_stock_data():
    # Use a breakpoint in the code line below to debug your script.
    companies = get_companies()

    for company in companies:
        slug = company[1].lower()
        extract_data(slug, company[0])
        
    close_db()

    sys.exit()


def get_db():
    conn = psycopg2.connect('dbname=stocker user=lester password=password')
    return conn


def close_db():
    conn = get_db()
    if conn:
        conn.close()


def get_companies():
    conn = get_db()
    cur = conn.cursor()

    cur.execute('select id, symbol from companies where active is TRUE order by id')
    companies = cur.fetchall()

    companies.sort()

    return companies


def convert_string_to_float(value):
    """
    remove comma from string

    :param str value:
    :return: float value
    """
    return float(value.replace(',', ''))


def extract_data(slug: str, _id: int):
    url = base_url + slug

    for i in range(4):
        try:
            html = requests.get(url)
            soup = BeautifulSoup(html.text, 'html.parser')

            # let's find the table data
            data_table = soup.find('table', {'id': 'data'})

            # let's get the first row from the tbody of the table
            if len(data_table.find_all('tr')) > 1:
                extract_and_save(data_table.find_all('tr')[1], slug, _id)
            else:
                message = 'No data to extract for ' + slug
                logging.info(message)
                print(message)
            break
        except (ConnectionError, ConnectionResetError) as e:
            print(e)
            time.sleep(3)
            # go to next try
            continue
    else:
        message = 'Something went wrong with ' + slug
        logging.error(message)
        print(message)


def extract_and_save(row, slug, _id):
    columns = row.find_all('td')
    date = datetime.strptime(columns[0].text, '%b %d, %Y').strftime('%Y-%m-%-d')
    openp = convert_string_to_float(columns[1].text)
    highp = convert_string_to_float(columns[2].text)
    lowp = convert_string_to_float(columns[3].text)
    closep = convert_string_to_float(columns[4].text)
    value = convert_string_to_float(columns[7].text)

    # let's re-initialize the cursor
    conn = get_db()
    cur = conn.cursor()

    # let's skip if the data is already in the table
    cur.execute('''
                SELECT * FROM historical_prices WHERE date=%s and company_id=%s
                ''', (date, _id))
    result = cur.fetchall()

    if len(result):
        message = 'Record already exists for ' + slug
        logging.info(message)
        print(message)
    else:
        cur.execute('''
                        INSERT INTO historical_prices (company_id, date, open, high, low, close, value)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                    ''', (_id, date, openp, highp, lowp, closep, value))
        conn.commit()
        message = 'Successfully inserted data for ' + slug

        # add indicators
        add_indicators(_id)

        logging.info(message)
        print(message)

    cur.close()


def add_indicators(_id):
    cur = get_db().cursor()

    cur.execute('''
        select date, open, high, low, close from historical_prices
        where company_id=%s
        order by date desc
        limit 200
    ''' % _id)

    stock = pd.DataFrame(cur.fetchall())

    stock.columns = [desc[0] for desc in cur.description]

    stock = stock.astype({
        'open': 'float',
        'high': 'float',
        'low': 'float',
        'close': 'float',
    })

    stock.sort_values(by='date', ascending=True, inplace=True)
    stock['alma'] = ALMAIndicator(stock.close).alma()
    macd = MACD(stock.close)
    stock['macd'] = macd.macd()
    stock['macd_signal'] = macd.macd_signal()
    stock['macd_hist'] = macd.macd_diff()
    stock['ma_20'] = SMAIndicator(stock.close, 20).sma_indicator()
    stock['ma_50'] = SMAIndicator(stock.close, 50).sma_indicator()
    stock['ma_100'] = SMAIndicator(stock.close, 100).sma_indicator()
    stock['ma_200'] = SMAIndicator(stock.close, 200).sma_indicator()
    stock['rsi'] = RSIIndicator(stock.close).rsi()
    stock['cci'] = CCIIndicator(stock.high, stock.low, stock.close).cci()
    # stock['atr'] = AverageTrueRange(stock.high, stock.low, stock.close).average_true_range()
    stock['sts'] = StochasticOscillator(stock.high, stock.low, stock.close).stoch()
    stock['williams_r'] = WilliamsRIndicator(stock.high, stock.low, stock.close).williams_r()
    stock['trix'] = TRIXIndicator(stock.close, 7, fillna=False).trix()
    stock['psar'] = PSARIndicator(stock.high, stock.low, stock.close, fillna=False).psar()
    stock['ema_9'] = EMAIndicator(stock.close, 9, fillna=False).ema_indicator()
    stock['pct_change'] = stock.close.pct_change()
    data_to_insert = stock[-1:].to_records(index=False)[0]

    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        update historical_prices
        set alma=%s, macd=%s, macd_signal=%s, macd_hist=%s, ma_20=%s, ma_50=%s, ma_100=%s, ma_200=%s, rsi=%s, cci=%s, sts=%s, williams_r=%s, trix=%s, psar=%s, ema_9=%s, pct_change=%s 
        where company_id=%s and date=%s
    ''', (
        data_to_insert[5],
        data_to_insert[6],
        data_to_insert[7],
        data_to_insert[8],
        data_to_insert[9],
        data_to_insert[10],
        data_to_insert[11],
        data_to_insert[12],
        data_to_insert[13],
        data_to_insert[14],
        data_to_insert[15],
        data_to_insert[16],
        data_to_insert[17],
        data_to_insert[18],
        data_to_insert[19],
        data_to_insert[20],
        _id,
        data_to_insert[0]
    ))

    conn.commit()
    cur.close()

    print(data_to_insert)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_stock_data()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
