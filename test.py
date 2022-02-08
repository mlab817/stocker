from unittest import TestCase

from trader import set_technical_indicators, Company
import psycopg2
import yfinance as yf


def test_trader():
    company = Company('2GO')
    config = {}

    conn = psycopg2.connect('dbname=stocker user=lester password=password')
    cur = conn.cursor()

    cur.execute('''
        select close as Close from historical_prices where company_id=1 and date > '02-03-2021' order by date asc
    ''')
    company.prices = cur.fetchall()

    print(company.prices)

    set_technical_indicators(config, company)


if __name__ == '__main__':
    test_trader()
    print('everything passed')
