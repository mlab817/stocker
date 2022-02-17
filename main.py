# This is a sample Python script.
import logging

import pandas as pd
import psycopg2
import requests
import sys
import time
import warnings

warnings.filterwarnings('ignore')

from AlmaIndicator import ALMAIndicator
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, render_template, request

from ta.momentum import RSIIndicator, WilliamsRIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD, CCIIndicator, TRIXIndicator, PSARIndicator, EMAIndicator

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

app = Flask(__name__)


base_url = 'https://frames.pse.com.ph/security/'


@app.route('/', methods=['get', 'post'])
def index():
    if request.method == 'POST':
        return 'Retrieving stuff'

    return render_template('index.html')


@app.errorhandler(404)
def not_found(error):
    return render_template('error.html')


def get_stock_data():
    # Use a breakpoint in the code line below to debug your script.
    companies = get_companies()

    for company in companies:
        slug = company[1].lower()
        extract_data(slug, company[0])

    sys.exit()


class Conn:
    def __init__(self):
        self.config = config = {
            'host': 'localhost',
            'user': 'lester',
            'port': 5432,
            'password': 'password',
            'database': 'stocker'
        }
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()


def get_companies():
    conn = Conn()

    conn.cursor.execute('select id, symbol from companies where active is TRUE order by id')
    companies = conn.cursor.fetchall()

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
    conn = Conn()

    columns = row.find_all('td')
    date = datetime.strptime(columns[0].text, '%b %d, %Y').strftime('%Y-%m-%-d')
    openp = convert_string_to_float(columns[1].text)
    highp = convert_string_to_float(columns[2].text)
    lowp = convert_string_to_float(columns[3].text)
    closep = convert_string_to_float(columns[4].text)
    value = convert_string_to_float(columns[7].text)

    # let's skip if the data is already in the table
    conn.cursor.execute('''
                SELECT * FROM historical_prices WHERE date=%s and company_id=%s
                ''', (date, _id))
    result = conn.cursor.fetchall()

    if len(result):
        message = 'Record already exists for ' + slug
        logging.info(message)
        print(message)
    else:
        conn.cursor.execute('''
                        INSERT INTO historical_prices (company_id, date, open, high, low, close, value)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                    ''', (_id, date, openp, highp, lowp, closep, value))
        conn.conn.commit()
        message = 'Successfully inserted data for ' + slug

        # add indicators
        # add_indicators(_id)

        logging.info(message)
        print(message)

    add_indicators(_id)


def add_indicators(_id):
    conn = Conn()

    conn.cursor.execute('''
        select date, open, high, low, close from historical_prices
        where company_id=%s
        order by date desc
        limit 200
    ''' % _id)

    prices = conn.cursor.fetchall()

    stock = pd.DataFrame(prices)

    stock.columns = [desc[0] for desc in conn.cursor.description]

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
    stock['trix'] = TRIXIndicator(stock.close, 7).trix()
    stock['psar'] = PSARIndicator(stock.high, stock.low, stock.close).psar()
    stock['ema_9'] = EMAIndicator(stock.close, 9).ema_indicator()
    stock['pct_change'] = stock.close.pct_change()
    data_to_insert = stock[-1:].to_records(index=False)[0]

    cur = conn.cursor

    cur.execute('''
        update historical_prices
        set alma=%s, macd=%s, macd_signal=%s, macd_hist=%s, ma_20=%s, ma_50=%s, ma_100=%s, ma_200=%s, rsi=%s, cci=%s, 
        sts=%s, williams_r=%s, trix=%s, psar=%s, ema_9=%s, pct_change=%
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

    conn.conn.commit()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_stock_data()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
