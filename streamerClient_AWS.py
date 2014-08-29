'''This will open a connection to the yahoo streamerapi server and 
print the data dict to the terminal.'''

import requests
from ast import literal_eval
from datetime import datetime
import pytz
#import sqlite3
import mysql.connector as mysql
from time import sleep
from keyStatsDB_AWS import keyStatsFunc



def stringGen(Chars,Char,numChars):
	'''Take in one character at a time and add it to the end of the string
	Chars while dropping the first character of the Chars string.
	eg: Chars='hart' Char='s' numChars=4 => 'arts'.'''
	#<script>try{parent.yfs_u1f({"MSFT":{}});}catch(e){}</script>
	charList = []
	newList=[]

	if len(Chars)==numChars:
		charList = list(Chars)
		newList = charList[1:]
		newList.append(Char)
		return(''.join(newList))
	else:
		charList = list(Chars)
		charList.append(Char)
		return(''.join(charList))

def insertQuotes(strIn, field):
	'''Take an almost dict, find one of the yahoo keys, 
	add single quotes around the key and return a dict using AST'''
	l=[]	
	for s in field:
		p = strIn.find(s)
		if  p > 0:
			l = list(strIn)
			l.insert(p,"'")
			l.insert(p+4,"'")
			strIn = ''.join(l)	
	d = literal_eval(strIn)
	return d

def assignVals(varDict):
	global LastPrice,bid,ask,bidSize,askSize, volume 
	#Start with all names == None. Will assign values to those in the dict
	(LastPrice,bid,ask,bidSize,askSize,volume) = (None,None,None,None,None,None)
	#LastPrice=l84,bid=b00,ask=a00,bidSize=b60,askSize=a50
	for dKey in varDict:
		if dKey == 'l84':
			LastPrice = varDict['l84']
		elif dKey == 'b00':
			bid = varDict['b00']
		elif dKey == 'a00':
			ask = varDict['a00']
		elif dKey == 'b60':
			bidSize = varDict['b60']
		elif dKey == 'a50':
			askSize = varDict['a50']
		elif dKey == 'v53':
			volume = varDict['v53']

def main():
	#Initate connection to the Yahoo server
	ind_tkrs = ('GE','UTX','HON','MMM')
	oil_tkrs = ('BP','TOT','XOM','CVX','COP')
	fin_tkrs = ('JPM','C','BAC','KEY','WFC')
	bank_tkrs = ('BBT','FITB','HBAN','MTB','PNC','STI','USB')
	tech_tkrs = ('GOOG','AMZN','FB','MSFT','NFLX','ADBE','ORCL','AAPL')
	pharma_tkrs = ('MRK','PFE','ABT','AGN','BAX','LLY')
	util_tkrs = ('XEL','SO','PEG','PCG','NRG','EXC','ED','AEE')
	aero_tkrs = ('GD','LMT','NOC','RTN')
	pack_tkrs = ('FDX','UPS')
	air_tkrs = ('DAL','LUV')
	lux_tkrs = ('COH','KORS','RL','PVH','TIF')
	car_tkrs = ('F','GM')
	srv_tkrs = ('BHI','CAM','SLB','NOV','COG')
	etf_tkrs = ('XLI','XLE','XLF','KBW','XLK','XPH','XLU','OIH')
	
	tickers = ind_tkrs+oil_tkrs+fin_tkrs+bank_tkrs+tech_tkrs+pharma_tkrs+util_tkrs+aero_tkrs+pack_tkrs+air_tkrs+lux_tkrs+car_tkrs+srv_tkrs+etf_tkrs
	#These are the yahoo streamer codes for LastPrice, ask, bid, ask size, bid size, volume respectively
	fields = ('l84','a00','b00','a50','b60','v53')
	fieldsStr = ','.join(fields)
	tickerStr = ','.join(tickers)

	url = 'http://streamerapi.finance.yahoo.com/streamer/1.0?s=%s&k=%s&r=0&callback=parent.yfs_u1f&mktmcb=parent.yfs_mktmcb&gencallback=parent.yfs_gencb' % (tickerStr,fieldsStr)
	r = requests.get(url, stream=True)

	tagB = '' #Recepticle for characters
	tagE = '' #Recepticle for characters
	beginQ = '<script>try{parent.yfs_u1f({' #Leading character tag
	endQ = ');}catch(e){}</script>' #Trailing character tag
	#<script>try{parent.yfs_u1f({"MSFT":{}});}catch(e){}</script>
	inState = False #Start the machine in the Not Recording state
	dataCollect = ''

	#This is a text file with my db username and PW etc.
	#params = open('dbparams.txt').read()
	params = open('/home/ubuntu/Yahoo/dbparams.txt').read()
	params =  params.split(',')

	db = mysql.connect(user=params[0],password=params[1],host=params[2],database=params[3])
	cursor = db.cursor()
	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS livePrices(tickTime VARCHAR(40) NOT NULL, 
					Ticker VARCHAR(40), qDate DATE, qTime VARCHAR(40), LastPrice REAL, bid REAL, ask REAL, 
					bidSize INTEGER, askSize INTEGER, volume VARCHAR(20),PRIMARY KEY (tickTime))ENGINE=InnoDB''')
	db.commit()

	#Stoping time
	eastern = pytz.timezone('US/Eastern')
	fourPMstop = datetime.now(eastern).replace(hour=16, minute=0, second=0,microsecond=50000)

	#Pause until 9:30am
	startTime = datetime.now(eastern).replace(hour=9, minute=30, second=0,microsecond=0)
	while datetime.now(eastern)<startTime:
		sleep(1)
		print('Waiting for 9:30...',datetime.now(eastern))

	#This for loops continuously
	for char in r.iter_content():
		c = char.decode()
		tagB = stringGen(tagB,c,28)
		tagE = stringGen(tagE,c,22)

		if tagB == beginQ:
			#Transition current state to Recording
			inState = True
		if tagE == endQ:
			#Transition current state to Not Recording
			#Remove end tag from recorded string
			t = datetime.now(eastern)
			notRecTime = (t.date().isoformat(),t.time().isoformat())
			s = dataCollect[:len(dataCollect)-(len(endQ)-1)]
			#Sometimes an empty string is returned, this skips them
			if len(s)>0:
				retDict = insertQuotes(s,fields)
				retDict['timeStamp'] = notRecTime 
				#You now have dict with ticker:value and timestamp:(date,time)	
				#Insert those values into the DB
				for tkey in retDict.keys(): #can I call a 'any key but this one' operator to replace the loop?
					if tkey != 'timeStamp':
						ticker = tkey
				qTime = retDict['timeStamp'][1]		
				qDate = retDict['timeStamp'][0]
				tickTime = str.join('_',(ticker,qDate,qTime))
				
				#'assignVals' assigns vals to global variables depending on what is 
				# present in the returned dict (retDict)
				assignVals(retDict[ticker])
				print(ticker,qTime)
				
				try:
					cursor.execute('''INSERT INTO livePrices(tickTime, Ticker, qDate, qTime, LastPrice, 
									bid, ask, bidSize, askSize, volume) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
									(tickTime,ticker,qDate,qTime,LastPrice,bid,ask,bidSize,askSize,volume))
					db.commit()
				except Exception:
					sleep(.01)
					db.commit()
				

			dataCollect = '' #Reset the collection
			inState = False #Not Recoding state
			
			#This ends the session at 4pm
			if datetime.now(eastern)>fourPMstop:
				print("Quitting time!")
				#Call the key stats function before quitting
				keyStatsFunc(tickers)
				break	

		#When current state is Record, record all the characters that come in.
		if inState == True:
			dataCollect = dataCollect + c
	cursor.close()
	db.close()

if __name__ == '__main__':
	main()