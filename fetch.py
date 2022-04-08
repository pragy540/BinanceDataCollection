import requests
import csv
import datetime
import os
import multiprocessing
import time


def getStringDate(date):
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    strDate = year+"-"+month+"-"+day
    return strDate

def getDateTimeDate(date):
    date=date.split('-')
    retDate = datetime.datetime(int(date[0]), int(date[1]), int(date[2]))
    return retDate

def fetchDataForSymbolAndDate(date, symbol, interval):
    URL ="https://fapi.binance.com/fapi/v1/klines"

    
    endDate = date + datetime.timedelta(days = 1)
    startTime = int(datetime.datetime.timestamp(date)*1000.0)
    endTime = int(datetime.datetime.timestamp(endDate)*1000.0)
    
    print(startTime)
    print(endTime)

    PARAMS = {'symbol':symbol, 'interval':interval, "startTime": startTime, 'endTime':endTime, "limit":1500}
    r = requests.get(url = URL, params = PARAMS)
    data = r.json()
    print(len(data))

   
    header = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"]

    #creating path to store data
    path = symbol+'/'+interval
    doesExist = os.path.exists(path)
    if not doesExist:
        os.makedirs(path)
  
    filePath = path+'/'+getStringDate(date)+'.csv'
    with open(filePath, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(header) 
        csvwriter.writerows(data) 

    


def fetchDataForSymbol(symbol, interval, startdate, enddate):

    startDate1 = getDateTimeDate(startdate)
    endDate1 = enddate
    days = endDate1 - startDate1
    num_days=days.days + 1
    print("Number of days", num_days)

    #getting list of dates
    dates = []
    dates.append(startDate1)
    for i in range(num_days-2):
        if(dates[-1]+datetime.timedelta(days = 1) != endDate1):
            dates.append(dates[-1]+datetime.timedelta(days = 1))
    dates.append(endDate1)

    i=0
    while(num_days > i):
        processes = []
        startTime = time.time()
        for x in range(100):
            if(num_days > i+x):
                p = multiprocessing.Process(target=fetchDataForSymbolAndDate, args=[dates[i+x], symbol, interval])
                if __name__ == "__main__":
                    p.start()
                    processes.append(p)
        for p in processes:
            p.join()
        endTime = time.time()
        sleep = 60 - (endTime-startTime)
        if sleep > 0:
            time.sleep(sleep)
        i+=100


def fetchData(symbols, refresh_no_of_days, default_start_date, interval):
    for symbol in symbols:
        path = symbol+'/'+interval
        doesExist = os.path.exists(path)
        if not doesExist:
            print("here1")
            enddate = datetime.datetime.now()
            fetchDataForSymbol(symbol, interval, default_start_date, enddate)
        elif len(os.listdir(path)) == 0:
            print("here2")
            enddate = datetime.datetime.now()
            fetchDataForSymbol(symbol, interval, default_start_date, enddate)
        else:
            print("here3")
            files = os.listdir(path)
            dates_in_dir=[]
            for date in files:
                date=getDateTimeDate(date[:-4])
                dates_in_dir.append(date)
            print(dates_in_dir)

            latest_date = dates_in_dir[0]
            for  date in dates_in_dir:
                if date > latest_date:
                    latest_date = date

            # min(today’s date - refresh_no_of_days, latest date for data in folder)
            diff = datetime.datetime.now() -  datetime.timedelta(days = refresh_no_of_days)
            startDate = min(diff, latest_date)
            enddate = datetime.datetime.now()
            fetchDataForSymbol(symbol, interval, getStringDate(startDate), enddate)

            


fetchData(["BTCUSDT"], 3, "2022-04-02", "1m")



