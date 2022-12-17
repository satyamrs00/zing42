import sqlite3
from helpers import getSecuritiesFile, getBhavcopyFiles, bhavcopy_names
conn = sqlite3.connect('nse.db')
c = conn.cursor()

def main():
    # c.execute('CREATE TABLE IF NOT EXISTS securities (symbol TEXT PRIMARY KEY, name_of_company TEXT, series TEXT, date_of_listing TEXT, paid_up_value INTEGER, market_lot INTEGER, isin_number TEXT, face_value INTEGER)')
    # c.execute('CREATE TABLE IF NOT EXISTS bhavcopy (symbol TEXT, series TEXT, open REAL, high REAL, low REAL, close REAL, last REAL, prevclose REAL, tottrdqty INTEGER, tottrdval INTEGER, timestamp TEXT, totaltrades INTEGER, isin TEXT, FOREIGN KEY (symbol) REFERENCES securities(symbol))')
    # getSecuritiesFile()
    # getBhavcopyFiles()
    print(bhavcopy_names)


if __name__ == '__main__':
    main()