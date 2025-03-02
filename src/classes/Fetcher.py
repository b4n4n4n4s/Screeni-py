'''
 *  Project             :   Screenipy
 *  Author              :   Pranjal Joshi
 *  Created             :   28/04/2021
 *  Description         :   Class for handling networking for fetching stock codes and data
'''

import sys
import urllib.request
import csv
import requests
import random
import os
import datetime
import yfinance as yf
import pandas as pd
from nsetools import Nse
from classes.ColorText import colorText
from classes.SuppressOutput import SuppressOutput
from classes.Utility import isDocker
from pytickersymbols import PyTickerSymbols

nse = Nse()

# Exception class if yfinance stock delisted


class StockDataEmptyException(Exception):
    pass

# This Class Handles Fetching of Stock Data over the internet


class tools:

    def __init__(self, configManager):
        self.configManager = configManager
        pass

    def getAllNiftyIndices(self) -> dict:
        return {
            "^NDX": "NASDAQ 100",
            "^NSMIDCP": "NIFTY NEXT 50",
            "^CNX100": "NIFTY 100",
            "^CNX200": "NIFTY 200",
            "^CNX500": "NIFTY 500",
            "^NSEMDCP50": "NIFTY MIDCAP 50",
            "NIFTY_MIDCAP_100.NS": "NIFTY MIDCAP 100",
            "^CNXSC": "NIFTY SMALLCAP 100",
            "^INDIAVIX": "INDIA VIX",
            "NIFTYMIDCAP150.NS": "NIFTY MIDCAP 150",
            "NIFTYSMLCAP50.NS": "NIFTY SMALLCAP 50",
            "NIFTYSMLCAP250.NS": "NIFTY SMALLCAP 250",
            "NIFTYMIDSML400.NS": "NIFTY MIDSMALLCAP 400",
            "NIFTY500_MULTICAP.NS": "NIFTY500 MULTICAP 50:25:25",
            "NIFTY_LARGEMID250.NS": "NIFTY LARGEMIDCAP 250",
            "NIFTY_MID_SELECT.NS": "NIFTY MIDCAP SELECT",
            "NIFTY_TOTAL_MKT.NS": "NIFTY TOTAL MARKET",
            "NIFTY_MICROCAP250.NS": "NIFTY MICROCAP 250",
            "^NSEBANK": "NIFTY BANK",
            "^CNXAUTO": "NIFTY AUTO",
            "NIFTY_FIN_SERVICE.NS": "NIFTY FINANCIAL SERVICES",
            "^CNXFMCG": "NIFTY FMCG",
            "^CNXIT": "NIFTY IT",
            "^CNXMEDIA": "NIFTY MEDIA",
            "^CNXMETAL": "NIFTY METAL",
            "^CNXPHARMA": "NIFTY PHARMA",
            "^CNXPSUBANK": "NIFTY PSU BANK",
            "^CNXREALTY": "NIFTY REALTY",
            "NIFTY_HEALTHCARE.NS": "NIFTY HEALTHCARE INDEX",
            "NIFTY_CONSR_DURBL.NS": "NIFTY CONSUMER DURABLES",
            "NIFTY_OIL_AND_GAS.NS": "NIFTY OIL & GAS",
            "NIFTYALPHA50.NS": "NIFTY ALPHA 50",
            "^CNXCMDT": "NIFTY COMMODITIES",
            "NIFTY_CPSE.NS": "NIFTY CPSE",
            "^CNXENERGY": "NIFTY ENERGY",
            "^CNXINFRA": "NIFTY INFRASTRUCTURE",
            "^CNXMNC": "NIFTY MNC",
            "^CNXPSE": "NIFTY PSE",
            "^CNXSERVICE": "NIFTY SERVICES SECTOR",
            "NIFTY100_ESG.NS": "NIFTY100 ESG SECTOR LEADERS",
        }

    def _getBacktestDate(self, backtest):
        try:
            end = backtest + datetime.timedelta(days=1)
            if "d" in self.configManager.period:
                delta = datetime.timedelta(days = self.configManager.getPeriodNumeric())
            elif "wk" in self.configManager.period:
                delta = datetime.timedelta(days = self.configManager.getPeriodNumeric() * 7)
            elif "m" in self.configManager.period:
                delta = datetime.timedelta(minutes = self.configManager.getPeriodNumeric())
            elif "h" in self.configManager.period:
                delta = datetime.timedelta(hours = self.configManager.getPeriodNumeric())
            start = end - delta
            return [start, end]
        except:
            return [None, None]
        
    def _getDatesForBacktestReport(self, backtest):
        dateDict = {}
        try:
            today = datetime.date.today()
            dateDict['T+1d'] = backtest + datetime.timedelta(days=1) if backtest + datetime.timedelta(days=1) < today else None
            dateDict['T+1wk'] = backtest + datetime.timedelta(weeks=1) if backtest + datetime.timedelta(weeks=1) < today else None
            dateDict['T+1mo'] = backtest + datetime.timedelta(days=30) if backtest + datetime.timedelta(days=30) < today else None
            dateDict['T+6mo'] = backtest + datetime.timedelta(days=180) if backtest + datetime.timedelta(days=180) < today else None
            dateDict['T+1y'] = backtest + datetime.timedelta(days=365) if backtest + datetime.timedelta(days=365) < today else None
            for key, val in dateDict.copy().items():
                if val is not None:
                    if val.weekday() == 5:  # 5 is Saturday, 6 is Sunday
                        adjusted_date = val + datetime.timedelta(days=2)
                        dateDict[key] = adjusted_date
                    elif val.weekday() == 6: 
                        adjusted_date = val + datetime.timedelta(days=1)
                        dateDict[key] = adjusted_date
        except:
            pass
        return dateDict

    def fetchCodes(self, tickerOption,proxyServer=None):
        stock_data = PyTickerSymbols()
        listStockCodes = []

        if tickerOption == 1:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('NASDAQ 100')) if "symbol" in item]        

        if tickerOption == 2:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('S&P 100')) if "symbol" in item]                
        
        if tickerOption == 3:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('S&P 500')) if "symbol" in item]      

        if tickerOption == 4:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('FTSE 100')) if "symbol" in item]      

        if tickerOption == 5:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('DOW JONES')) if "symbol" in item]   

        if tickerOption == 6:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('AEX')) if "symbol" in item] 

        if tickerOption == 7:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('BEL 20')) if "symbol" in item] 

        if tickerOption == 8:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('DAX')) if "symbol" in item] 

        if tickerOption == 9:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('SDAX')) if "symbol" in item] 

        if tickerOption == 10:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('CAC 40')) if "symbol" in item]
        
        if tickerOption == 11:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('SMI')) if "symbol" in item]
        
        if tickerOption == 12:
            url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed.csv"
            return list(pd.read_csv(url)['Symbol'].values)

        if tickerOption == 13:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('TECDAX')) if "symbol" in item]
    
        if tickerOption == 14:
            return [item["symbol"] for item in list(stock_data.get_stocks_by_index('MOEX')) if "symbol" in item]


    # Fetch all stock codes from NSE
    def fetchStockCodes(self, tickerOption, proxyServer=None):
        listStockCodes = []
        if tickerOption == 0:
            stockCode = None
            while stockCode == None or stockCode == "":
                stockCode = str(input(colorText.BOLD + colorText.BLUE +
                                      "[+] Enter Stock Code(s) for screening (Multiple codes should be seperated by ,): ")).upper()
            stockCode = stockCode.replace(" ", "")
            listStockCodes = stockCode.split(',')
        else:
            print(colorText.BOLD +
                  "[+] Getting Stock Codes From NASDAQ... ", end='')
            listStockCodes = self.fetchCodes(tickerOption,proxyServer=proxyServer)
            if type(listStockCodes) == dict:
                listStockCodes = list(listStockCodes.keys())
            if len(listStockCodes) > 10:
                print(colorText.GREEN + ("=> Done! Fetched %d stock codes." %
                                         len(listStockCodes)) + colorText.END)
                if self.configManager.shuffleEnabled:
                    random.shuffle(listStockCodes)
                    print(colorText.BLUE +
                          "[+] Stock shuffling is active." + colorText.END)
                else:
                    print(colorText.FAIL +
                          "[+] Stock shuffling is inactive." + colorText.END)
                if self.configManager.stageTwo:
                    print(
                        colorText.BLUE + "[+] Screening only for the stocks in Stage-2! Edit User Config to change this." + colorText.END)
                else:
                    print(
                        colorText.FAIL + "[+] Screening only for the stocks in all Stages! Edit User Config to change this." + colorText.END)

            else:
                input(
                    colorText.FAIL + "=> Error getting stock codes from NASDAQ! Press any key to exit!" + colorText.END)
                sys.exit("Exiting script..")

        return listStockCodes

    # Fetch stock price data from Yahoo finance
    def fetchStockData(self, stockCode, period, duration, proxyServer, screenResultsCounter, screenCounter, totalSymbols, backtestDate=None, printCounter=False, tickerOption=None):
        dateDict = None
        with SuppressOutput(suppress_stdout=True, suppress_stderr=True):
            append_exchange = ""
            if tickerOption == 15 or tickerOption == 16:
                append_exchange = ""
            data = yf.download(
                tickers=stockCode + append_exchange,
                period=period,
                interval=duration,
                proxy=proxyServer,
                progress=False,
                timeout=10,
                start=self._getBacktestDate(backtest=backtestDate)[0],
                end=self._getBacktestDate(backtest=backtestDate)[1],
                auto_adjust=False
            )
            # For df backward compatibility towards yfinance 0.2.32
            data = self.makeDataBackwardCompatible(data)
            # end
            if backtestDate != datetime.date.today():
                dateDict = self._getDatesForBacktestReport(backtest=backtestDate)
                backtestData = yf.download(
                    tickers=stockCode + append_exchange,
                    interval='1d',
                    proxy=proxyServer,
                    progress=False,
                    timeout=10,
                    start=backtestDate - datetime.timedelta(days=1),
                    end=backtestDate + datetime.timedelta(days=370)
                )
                for key, value in dateDict.copy().items():
                    if value is not None:
                        try:
                            dateDict[key] = backtestData.loc[pd.Timestamp(value)]['Close']
                        except KeyError:
                            continue
                dateDict['T+52wkH'] = backtestData['High'].max()
                dateDict['T+52wkL'] = backtestData['Low'].min()
        if printCounter:
            sys.stdout.write("\r\033[K")
            try:
                print(colorText.BOLD + colorText.GREEN + ("[%d%%] Screened %d, Found %d. Fetching data & Analyzing %s..." % (
                    int((screenCounter.value/totalSymbols)*100), screenCounter.value, screenResultsCounter.value, stockCode)) + colorText.END, end='')
            except ZeroDivisionError:
                pass
            if len(data) == 0:
                print(colorText.BOLD + colorText.FAIL +
                      "=> Failed to fetch!" + colorText.END, end='\r', flush=True)
                raise StockDataEmptyException
                return None
            print(colorText.BOLD + colorText.GREEN + "=> Done!" +
                  colorText.END, end='\r', flush=True)
        return data, dateDict

    # Get Daily Nifty 50 Index:
    def fetchLatestNiftyDaily(self, proxyServer=None):
        data = yf.download(
                auto_adjust=False,
                tickers="^NSEI",
                period='5d',
                interval='1d',
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        gold = yf.download(
                auto_adjust=False,
                tickers="GC=F",
                period='5d',
                interval='1d',
                proxy=proxyServer,
                progress=False,
                timeout=10
            ).add_prefix(prefix='gold_')
        crude = yf.download(
                    auto_adjust=False,
                    tickers="CL=F",
                    period='5d',
                    interval='1d',
                    proxy=proxyServer,
                    progress=False,
                    timeout=10
                ).add_prefix(prefix='crude_')
        data = self.makeDataBackwardCompatible(data)
        gold = self.makeDataBackwardCompatible(gold, column_prefix='gold_')
        crude = self.makeDataBackwardCompatible(crude, column_prefix='crude_')
        data = pd.concat([data, gold, crude], axis=1)
        return data

    # Get Data for Five EMA strategy
    def fetchFiveEmaData(self, proxyServer=None):
        nifty_sell = yf.download(
                auto_adjust=False,
                tickers="^NSEI",
                period='5d',
                interval='5m',
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        banknifty_sell = yf.download(
                auto_adjust=False,
                tickers="^NSEBANK",
                period='5d',
                interval='5m',
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        nifty_buy = yf.download(
                auto_adjust=False,
                tickers="^NSEI",
                period='5d',
                interval='15m',
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        banknifty_buy = yf.download(
                auto_adjust=False,
                tickers="^NSEBANK",
                period='5d',
                interval='15m',
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        nifty_buy = self.makeDataBackwardCompatible(nifty_buy)
        banknifty_buy = self.makeDataBackwardCompatible(banknifty_buy)
        nifty_sell = self.makeDataBackwardCompatible(nifty_sell)
        banknifty_sell = self.makeDataBackwardCompatible(banknifty_sell)
        return nifty_buy, banknifty_buy, nifty_sell, banknifty_sell

    # Load stockCodes from the watchlist.xlsx
    def fetchWatchlist(self):
        createTemplate = False
        data = pd.DataFrame()
        try:
            data = pd.read_excel('watchlist.xlsx')
        except FileNotFoundError:
            print(colorText.BOLD + colorText.FAIL +
                  f'[+] watchlist.xlsx not found in f{os.getcwd()}' + colorText.END)
            createTemplate = True
        try:
            if not createTemplate:
                data = data['Stock Code'].values.tolist()
        except KeyError:
            print(colorText.BOLD + colorText.FAIL +
                  '[+] Bad Watchlist Format: First Column (A1) should have Header named "Stock Code"' + colorText.END)
            createTemplate = True
        if createTemplate:
            if isDocker():
                print(colorText.BOLD + colorText.FAIL +
                  f'[+] This feature is not available with dockerized application. Try downloading .exe/.bin file to use this!' + colorText.END)
                return None
            sample = {'Stock Code': ['SBIN', 'INFY', 'TATAMOTORS', 'ITC']}
            sample_data = pd.DataFrame(sample, columns=['Stock Code'])
            sample_data.to_excel('watchlist_template.xlsx',
                                 index=False, header=True)
            print(colorText.BOLD + colorText.BLUE +
                  f'[+] watchlist_template.xlsx created in {os.getcwd()} as a referance template.' + colorText.END)
            return None
        return data
    
    def makeDataBackwardCompatible(self, data:pd.DataFrame, column_prefix:str=None) -> pd.DataFrame:
        data = data.droplevel(level=1, axis=1)
        data = data.rename_axis(None, axis=1)
        column_prefix = '' if column_prefix is None else column_prefix
        data = data[
            [
                f'{column_prefix}Open', 
                f'{column_prefix}High', 
                f'{column_prefix}Low', 
                f'{column_prefix}Close', 
                f'{column_prefix}Adj Close', 
                f'{column_prefix}Volume'
            ]
        ]
        return data
