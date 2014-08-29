#import sqlite3
import mysql.connector as mysql
from KeyStatsScrape_AWS import keyStats
from datetime import datetime
import time
import urllib
from sys import exc_info

def keyStatsFunc(tkrs):
	'''This function receives the list of tickers from streamerClient and loops over it
	until the key stats have all been retreived from yahoo and inserted into the database'''

	params = open('/home/ubuntu/Yahoo/dbparams.txt').read()
	#params = open('dbparams.txt').read()
	params =  params.split(',')

	db = mysql.connect(user=params[0],password=params[1],host=params[2],database=params[3])
	
	cursor = db.cursor()

	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS keyStats(tickerDate VARCHAR(45),ticker VARCHAR(45),tDate DATE,
					tTime VARCHAR(25),avgDivYield REAL,avgVol10d VARCHAR(15),avgVol30d VARCHAR(15),beta REAL,
					bookValPS REAL,cash VARCHAR(45),cashPS REAL,companyName VARCHAR(45),currRatio REAL,d52week REAL,
					employees VARCHAR(15),enterpriseValue REAL,floatShort REAL,fwdDivPS REAL,
					fwdDivYld REAL,fwdPE REAL,grossProfit REAL,hi52week REAL,
					industry VARCHAR(45),lo52week REAL,ma200day REAL,ma50day REAL,marketCap REAL,
					netIncSH REAL,opCashFlow REAL,operateMarg REAL,payoutRatio VARCHAR(15),peg REAL,
					profitMarg REAL,pSales REAL,qEarnGrth REAL,qRevGrowth REAL,retAssets REAL,
					retEquity REAL, revenue REAL,revenuePS REAL,sector VARCHAR(45),shortRatio REAL,
					shrsShrt REAL,shrsShrtNew REAL,sp52week REAL,totDebt REAL,
					totDebtEquity REAL,trailPE REAL, PRIMARY KEY (tickerDate))ENGINE=InnoDB''')

	for t in tkrs:
		try:
			ks = keyStats(t)
		except urllib.error.URLError as err:
			print("__URLError___:", err)
			time.sleep(3)
		except:
			print("___Unexpected___ error:", exc_info())
			time.sleep(3)

		#Set the variable values here
		tickerDate = ks.ticker + '_' + ks.tDate
		try:
			cursor.execute('''INSERT IGNORE INTO keyStats(tickerDate,ticker,tDate,tTime,avgDivYield,avgVol10d,
							avgVol30d ,beta,bookValPS,cash,cashPS,companyName,currRatio,d52week,employees,
							enterpriseValue ,floatShort,fwdDivPS,fwdDivYld,fwdPE,grossProfit ,hi52week,
							industry,lo52week,ma200day,ma50day,marketCap,netIncSH,opCashFlow,operateMarg,
							payoutRatio,peg,profitMarg,pSales,qEarnGrth,qRevGrowth,retAssets,retEquity, 
							revenue,revenuePS,sector,shortRatio,shrsShrt,shrsShrtNew,sp52week,totDebt,
							totDebtEquity,trailPE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
							%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (tickerDate,ks.ticker,ks.tDate,ks.tTime,
							ks.avgDivYield,ks.avgVol10d,ks.avgVol30d,ks.beta,ks.bookValPS,ks.cash,ks.cashPS,ks.companyName,
							ks.currRatio,ks.d52week,ks.employees,ks.enterpriseValue,ks.floatShort,ks.fwdDivPS,ks.fwdDivYld,
							ks.fwdPE,ks.grossProfit,ks.hi52week,ks.industry,ks.lo52week,ks.ma200day,
							ks.ma50day,ks.marketCap,ks.netIncSH,ks.opCashFlow,ks.operateMarg,ks.payoutRatio,
							ks.peg,ks.profitMarg,ks.pSales,ks.qEarnGrth,ks.qRevGrowth,ks.retAssets,ks.retEquity,
							ks.revenue,ks.revenuePS,ks.sector,ks.shortRatio,ks.shrsShrt,ks.shrsShrtNew,ks.sp52week,
							ks.totDebt,ks.totDebtEquity,ks.trailPE))
			db.commit()
		except Exception as err:
			print('Error entering ', t, err)
		
		print(ks.companyName)
	cursor.close()
	db.close()	
		
