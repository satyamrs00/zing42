import sqlite3
from helpers import getSecuritiesFile, getBhavcopyFiles,clearBhavcopyFiles, clearSecuritiesFile, bhavcopy_names, latest_datetime
conn = sqlite3.connect('nse.db')
c = conn.cursor()

def add_data():
    c.execute('CREATE TABLE IF NOT EXISTS securities (symbol TEXT PRIMARY KEY, name_of_company TEXT, series TEXT, date_of_listing TEXT, paid_up_value INTEGER, market_lot INTEGER, isin_number TEXT, face_value INTEGER)')
    c.execute('CREATE TABLE IF NOT EXISTS bhavcopy (symbol TEXT, series TEXT, open REAL, high REAL, low REAL, close REAL, last REAL, prevclose REAL, tottrdqty INTEGER, tottrdval INTEGER, timestamp TEXT, totaltrades INTEGER, isin TEXT, FOREIGN KEY (symbol) REFERENCES securities(symbol))')
    c.execute('DELETE FROM securities')
    c.execute('DELETE FROM bhavcopy')

    clearSecuritiesFile()
    clearBhavcopyFiles()
    getSecuritiesFile()
    getBhavcopyFiles()
    print(bhavcopy_names)
    
    print('Adding securities data to database...')
    with open('securities.csv', 'r') as f:
        next(f)
        for line in f:
            line = line.strip().split(',')
            c.executemany('INSERT INTO securities (symbol, name_of_company, series, date_of_listing, paid_up_value, market_lot, isin_number, face_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (line, ))
            conn.commit()
            
    bhavcopy_names = ['cm16DEC2022bhav.csv', 'cm15DEC2022bhav.csv', 'cm14DEC2022bhav.csv', 'cm13DEC2022bhav.csv', 'cm12DEC2022bhav.csv', 'cm09DEC2022bhav.csv', 'cm08DEC2022bhav.csv', 'cm07DEC2022bhav.csv', 'cm06DEC2022bhav.csv', 'cm05DEC2022bhav.csv', 'cm02DEC2022bhav.csv', 'cm01DEC2022bhav.csv', 'cm30NOV2022bhav.csv', 'cm29NOV2022bhav.csv', 'cm28NOV2022bhav.csv', 'cm25NOV2022bhav.csv', 'cm24NOV2022bhav.csv', 'cm23NOV2022bhav.csv', 'cm22NOV2022bhav.csv', 'cm21NOV2022bhav.csv', 'cm18NOV2022bhav.csv', 'cm17NOV2022bhav.csv', 'cm16NOV2022bhav.csv', 'cm15NOV2022bhav.csv', 'cm14NOV2022bhav.csv', 'cm11NOV2022bhav.csv', 'cm10NOV2022bhav.csv', 'cm09NOV2022bhav.csv', 'cm07NOV2022bhav.csv', 'cm04NOV2022bhav.csv']
    print('Adding bhavcopy data to database...')
    for filename in bhavcopy_names:
        with open(filename, 'r') as f:
            next(f)
            for line in f:
                line = line.strip().split(',')[:-1]
                c.executemany('INSERT INTO bhavcopy (symbol, series, open, high, low, close, last, prevclose, tottrdqty, tottrdval, timestamp, totaltrades, isin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (line, ))
                conn.commit()
            
def get_data():
    # c.execute('SELECT * FROM securities INNER JOIN bhavcopy ON securities.symbol = bhavcopy.symbol SORT BY close DESC LIMIT 10')
    # with open('result1.csv', 'w') as f:
    #     pass
    pass

def main():
    # add_data()
    get_data()

if __name__ == '__main__':
    main()