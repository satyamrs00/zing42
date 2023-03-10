import psycopg 
import os
from dotenv import load_dotenv
from helpers import getSecuritiesFile, getBhavcopyFiles, bhavcopy_names, latest_datetime, last_datetime, clearFilesAll

load_dotenv()

try:
    conn = psycopg.connect(dbname='nse', autocommit=True, user="postgres", password=os.environ.get('DB_PASS'))
    c = conn.cursor()
except:
    print('Creating database nse...')
    conn = psycopg.connect(dbname='postgres', autocommit=True, user="postgres", password=os.environ.get('DB_PASS'))
    c = conn.cursor()
    c.execute("CREATE DATABASE nse")
    conn.close()
    conn = psycopg.connect(dbname='nse', autocommit=True, user="postgres", password=os.environ.get('DB_PASS'))
    c = conn.cursor()


if bhavcopy_names == []: 
    bhavcopy_names = ['cm16DEC2022bhav.csv', 'cm15DEC2022bhav.csv', 'cm14DEC2022bhav.csv', 'cm13DEC2022bhav.csv', 'cm12DEC2022bhav.csv', 'cm09DEC2022bhav.csv', 'cm08DEC2022bhav.csv', 'cm07DEC2022bhav.csv', 'cm06DEC2022bhav.csv', 'cm05DEC2022bhav.csv', 'cm02DEC2022bhav.csv', 'cm01DEC2022bhav.csv', 'cm30NOV2022bhav.csv', 'cm29NOV2022bhav.csv', 'cm28NOV2022bhav.csv', 'cm25NOV2022bhav.csv', 'cm24NOV2022bhav.csv', 'cm23NOV2022bhav.csv', 'cm22NOV2022bhav.csv', 'cm21NOV2022bhav.csv', 'cm18NOV2022bhav.csv', 'cm17NOV2022bhav.csv', 'cm16NOV2022bhav.csv', 'cm15NOV2022bhav.csv', 'cm14NOV2022bhav.csv', 'cm11NOV2022bhav.csv', 'cm10NOV2022bhav.csv', 'cm09NOV2022bhav.csv', 'cm07NOV2022bhav.csv', 'cm04NOV2022bhav.csv']
if not last_datetime: 
    last_datetime = '04-NOV-2022'
if not latest_datetime: 
    latest_datetime = '16-DEC-2022'

def add_data():
    c.execute('CREATE TABLE IF NOT EXISTS securities (symbol TEXT PRIMARY KEY, name_of_company TEXT, series TEXT, date_of_listing TEXT, paid_up_value INTEGER, market_lot INTEGER, isin_number TEXT, face_value INTEGER)')
    c.execute('CREATE TABLE IF NOT EXISTS bhavcopy (symbol TEXT, series TEXT, open REAL, high REAL, low REAL, close REAL, last REAL, prevclose REAL, tottrdqty REAL, tottrdval REAL, timestamp TEXT, totaltrades REAL, isin TEXT, FOREIGN KEY (symbol) REFERENCES securities(symbol))')
    c.execute('TRUNCATE TABLE bhavcopy CASCADE')
    c.execute('TRUNCATE TABLE securities CASCADE')

    getSecuritiesFile()
    getBhavcopyFiles()
    print(bhavcopy_names)
    
    print('Adding securities data to database...')
    with open('securities.csv', 'r') as f:
        next(f)
        for line in f:
            line = line.strip().split(',')
            line = [line[0], line[1], line[2], line[3], int(line[4]), int(line[5]), line[6], int(line[7])]
            c.execute('INSERT INTO securities (symbol, name_of_company, series, date_of_listing, paid_up_value, market_lot, isin_number, face_value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', line)
            conn.commit()
            
    print('Adding bhavcopy data to database...')
    for filename in bhavcopy_names:
        with open(filename, 'r') as f:
            next(f)
            for line in f:
                line = line.strip().split(',')[:-1]
                line = [line[0], line[1], float(line[2]), float(line[3]), float(line[4]), float(line[5]), float(line[6]), float(line[7]), float(line[8]), float(line[9]), line[10], float(line[11]), line[12]]
                try:
                    c.execute('INSERT INTO bhavcopy (symbol, series, open, high, low, close, last, prevclose, tottrdqty, tottrdval, timestamp, totaltrades, isin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', line)
                except Exception as e:
                    pass
                conn.commit()
            
def get_data():
    print('Getting data by query 1...')
    try:
        latest_str = latest_datetime.strftime('%d-%b-%Y').upper()
    except:
        latest_str = '16-DEC-2022'
    c.execute("""SELECT securities.symbol as SYMBOL, ((close-open)/open) as PROFIT, name_of_company as "NAME OF COMPANY" FROM securities INNER JOIN bhavcopy ON securities.symbol=bhavcopy.symbol WHERE timestamp = %s AND bhavcopy.series = 'EQ' ORDER BY PROFIT DESC LIMIT 25""", (latest_str, ))
    with open('result1.csv', 'w') as f:
        f.write('SYMBOL,PROFIT,NAME OF COMPANY\n')
        for row in c.fetchall():
            f.write(','.join(map(str, row)) + '\n')

    print('Getting data by query 2...')
    with open('result2.csv', 'w') as f:
        f.write('SYMBOL,PROFIT,NAME OF COMPANY,TIMESTAMP\n')
    for name in bhavcopy_names:
        date = name[2:4] + '-' + name[4:7] + '-' + name[7:11]
        c.execute("""SELECT securities.symbol as SYMBOL, ((close-open)/open) as PROFIT, name_of_company as "NAME OF COMPANY", timestamp as TIMESTAMP FROM securities INNER JOIN bhavcopy ON securities.symbol=bhavcopy.symbol WHERE timestamp = %s AND bhavcopy.series = 'EQ' ORDER BY PROFIT DESC LIMIT 25""", (date, ))
        with open('result2.csv', 'a') as f:
            for row in c.fetchall():
                f.write(','.join(map(str, row)) + '\n')

    print('Getting data by query 3...')
    try:
        latest_str = latest_datetime.strftime('%d-%b-%Y').upper()
        last_str = last_datetime.strftime('%d-%b-%Y').upper()
    except:
        latest_str = '16-DEC-2022'
        last_str = '04-NOV-2022'
    c.execute("""SELECT latest.symbol AS SYMBOL, ((latest.close-lastd.open)/lastd.open) AS PROFIT, name_of_company AS "NAME OF COMPANY" FROM (SELECT symbol, open, close, timestamp, series FROM bhavcopy) AS latest JOIN (SELECT symbol, open, close, timestamp, series FROM bhavcopy) AS lastd ON latest.symbol = lastd.symbol INNER JOIN securities ON securities.symbol = latest.symbol WHERE latest.timestamp = %s AND lastd.timestamp = %s AND latest.series = 'EQ' AND lastd.series = 'EQ' ORDER BY PROFIT DESC LIMIT 25""", (latest_str, last_str))
    with open('result3.csv', 'w') as f:
        f.write('SYMBOL,PROFIT,NAME OF COMPANY\n')
        for row in c.fetchall():
            f.write(','.join(map(str, row)) + '\n')
    

def main():
    # add_data()
    get_data()
    conn.close()
    # clearFilesAll()

if __name__ == '__main__':
    main()