from bs4 import BeautifulSoup as bs
import requests
import zipfile
import datetime
import os

headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
bhavcopy_names = []
latest_datetime = None

def getSecuritiesFile():
    print('Getting securities file...')
        
    # get securities csv
    url = 'https://www.nseindia.com/market-data/securities-available-for-trading'
    r = requests.get(url, headers=headers, timeout=20)
    soup = bs(r.text, 'html.parser')
    for link in soup.find_all(attrs={'data-entity-type': 'file'}):
        if link.text.__contains__('Securities available for Equity segment (.csv)'):
            csv_path = link.get('href')
            with open('securities.csv', 'wb') as f:
                f.write(requests.get(csv_path).content)
            break

def getBhavcopyFiles():
    print('Getting bhavcopy files...')
    # get latest bhavcopy
    #  https://archives.nseindia.com/content/historical/EQUITIES/' + getFullyear + '/' + getMMM + '/cm' + DDMMMYYYY + 'bhav.csv.zip
    date_datetime = datetime.datetime.today()
    date_search = {
        "DDMMMYYYY": date_datetime.strftime('%d%b%Y').upper(),
        "getMMM": date_datetime.strftime('%b').upper(),
        "getFullyear": date_datetime.strftime('%Y')
    }
    url = f'https://archives.nseindia.com/content/historical/EQUITIES/{date_search["getFullyear"]}/{date_search["getMMM"]}/cm{date_search["DDMMMYYYY"]}bhav.csv.zip'
    r = requests.get(url, headers=headers, timeout=20)
    while r.status_code != 200:
        date_datetime = date_datetime - datetime.timedelta(days=1)
        date_search = {
            "DDMMMYYYY": date_datetime.strftime('%d%b%Y').upper(),
            "getMMM": date_datetime.strftime('%b').upper(),
            "getFullyear": date_datetime.strftime('%Y')
        }
        url = f'https://archives.nseindia.com/content/historical/EQUITIES/{date_search["getFullyear"]}/{date_search["getMMM"]}/cm{date_search["DDMMMYYYY"]}bhav.csv.zip'
        r = requests.get(url, headers=headers, timeout=20)
    # print(r.status_code)
    latest_datetime = date_datetime
    with open(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip', 'wb') as f:
        f.write(r.content)
    with zipfile.ZipFile(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip', 'r') as zip_ref:
        zip_ref.extractall('.')
        
    os.remove(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip')
    bhavcopy_names.append(f'cm{date_search["DDMMMYYYY"]}bhav.csv')

    # get past 29 days bhavcopy
    last_datetime = latest_datetime
    count = 0
    while count < 29:
        date_datetime = last_datetime - datetime.timedelta(days=1)
        date_search = {
            "DDMMMYYYY": date_datetime.strftime('%d%b%Y').upper(),
            "getMMM": date_datetime.strftime('%b').upper(),
            "getFullyear": date_datetime.strftime('%Y')
        }
        url = f'https://archives.nseindia.com/content/historical/EQUITIES/{date_search["getFullyear"]}/{date_search["getMMM"]}/cm{date_search["DDMMMYYYY"]}bhav.csv.zip'
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            with open(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip', 'wb') as f:
                f.write(r.content)
            with zipfile.ZipFile(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip', 'r') as zip_ref:
                zip_ref.extractall('.')
            os.remove(f'cm{date_search["DDMMMYYYY"]}bhav.csv.zip')
            bhavcopy_names.append(f'cm{date_search["DDMMMYYYY"]}bhav.csv')
            count += 1
        last_datetime = date_datetime

def clearSecuritiesFile():
    print('Clearing securities file...')
    if os.path.exists('securities.csv'):
        os.remove('securities.csv')

def clearBhavcopyFiles():
    print('Clearing bhavcopy files...')
    for file in bhavcopy_names:
        os.remove(file)
    bhavcopy_names.clear()