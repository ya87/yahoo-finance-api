import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import requests
from lxml import html
import pandas as pd
import re
from pyspark import *
from pyspark.sql import SQLContext
import datetime as dt
import json
from os import listdir
from os.path import isfile, join, getsize
import networkx as nx
from graph_tool.all import *
import numpy as np
import matplotlib.pyplot as plt
from random import *

# 'force' parameter
# if True then load fresh data from web
# otherwise load data previously stored in file

DATA_FILE_LOC = "data/"
PROCESSED_FILE_LOC = "processed/"
#PREFIX = "nasdaq_index_"
#PREFIX = "sp500_"
PREFIX = "nasdaq_2014_2015_"
CRITERIA = "return_"
#SECTOR_INFO_FILE = PROCESSED_FILE_LOC + "nasdaq_index.csv"
#SECTOR_INFO_FILE = PROCESSED_FILE_LOC + "sp500.csv"
#SECTOR_INFO_FILE = "nasdaq_sector_industry_company.json"
SECTOR_INFO_FILE = PROCESSED_FILE_LOC + "nasdaq.csv"

config = SparkConf()
#self.awsAccessKeyId="<awsAccessKeyId>"
#self.awsSecretAccessKey="<awsSecretAccessKey>"
#config.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
#config.set("fs.s3n.awsAccessKeyId", self.awsAccessKeyId)
#config.set("fs.s3n.awsSecretAccessKey", self.awsSecretAccessKey)

sc = SparkContext(conf=config)
sqlContext = SQLContext(sc)


def loadJsonDataAsPandasDF(filename):
    with open(filename, 'rb') as f:
        data = f.readlines()

    # remove the trailing "\n" from each line
    data = map(lambda x: x.rstrip(), data)

    # each element of 'data' is an individual JSON object.
    # i want to convert it into an *array* of JSON objects
    # which, in and of itself, is one large JSON objectfr
    # basically... add square brackets to the beginning
    # and end, and have all the individual business JSON objects
    # separated by a comma
    data_json_str = "[" + ','.join(data) + "]"

    # now, load it into pandas dataframe and return it
    return pd.read_json(data_json_str)


def loadJsonDataAsSparkDF(filename):
	#df = sqlContext.read.json("s3n://"+self.awsAccessKeyId+":"+self.awsSecretAccessKey+"@"+self.bucketName+"/"+fileName)
	df = sqlContext.read.json(filename)
	return df


def writePandasDataFrameAsJsonToFile(df, filename):
	f = open(filename, "wb")
	df.to_json(f, orient='records')
	f.close()


# Get list of companies for particular stock exchange
# exchange: {'nasdaq', 'nyse', 'amex'}
def getStockExchangeSymbols(exchange="nasdaq", force=False):
	filename = PROCESSED_FILE_LOC + exchange + ".csv"
	print isfile(filename)

	if force or not isfile(filename):
		url = "http://www.nasdaq.com/screening/companies-by-name.aspx"
		payload = {'letter': '0', 'exchange': str(exchange), 'render': 'download'}
		response = requests.get(url, params=payload)
		data = re.sub(r'\,(\r\n)', r'\1', response.content)
		symbols = (pd.read_csv(StringIO(data), quotechar='"'))#['Symbol']
		symbols.to_csv(filename, index=False)
		#symbols = symbols.sort_values()
		#symbols.index = range(len(symbols))
	else:
		symbols = pd.read_csv(filename)

	return symbols

# TODO - add code for other stock indexes
def getStockExchangeIndexSymbols(exchange="nasdaq", force=False):
	filename = PROCESSED_FILE_LOC + exchange + "_index.csv"

	if force or not isfile(filename):
		# add code for other stock indexes if required
		page = requests.get('http://www.cnbc.com/nasdaq-100/')
		tree = html.fromstring(page.content)
		data = tree.xpath('//div[@class="future-row"]//tr/@data-table-chart-symbol')
		symbols = []
		for symbol in data:
			symbols.append(symbol)
		df = pd.DataFrame(symbols, columns=["Symbol"])
		dfAll = getStockExchangeSymbols(exchange = exchange)
		df = df.merge(nasdaqAll, on="Symbol")
		df.to_csv(filename)
	else:
		df = pd.read_csv(filename)

	return df

# Search for stocks
def searchStock(searchTerm):
	url = "http://autoc.finance.yahoo.com/autoc"
	payload = {'query': str(searchTerm), 'callback': 'YAHOO.Finance.SymbolSuggest.ssCallback'}
	response = requests.get(url, params=payload)
	#print(response)
	return response

# Gets a list of sectors and indutries belonging to each sector
# {sectorName: [{industryName, industryId}, ...], ....}
def getSectorData():
	
	def extractIndustryId(url):
		parts = url.split('/')
		lastPart = parts[-1]
		return lastPart.split('.')[0]

	page = requests.get('http://biz.yahoo.com/ic/ind_index.html')
	tree = html.fromstring(page.content)
	data = tree.xpath('//tr/td[@width="50%"]/table//tr')

	sectors = dict()
	for tr in data:
		td = tr.xpath('./td')
		if len(td) > 1:
			# replace \n in string with space
			industryName = re.sub(r'(.*)\n(.*)', r'\1 \2', td[1].xpath('.//a/text()')[0])
			industryId = extractIndustryId(td[1].xpath('.//a/@href')[0])
			sectors[key].append({'Name': industryName, 'Id': industryId})
		else:
			if td[0].get('height') == None:
				sectorName = re.sub(r'(.*)\n(.*)', r'\1 \2', td[0].xpath('.//b/text()')[0])
				key = sectorName
				sectors[key] = []

	return sectors

# Gets a list of sectors and indutry ids belonging to each sector
# {sectorName: industryIdList, ...}
def getIndustryIdsForSectors():
	sectors = getSectorData()
	industryIdsPerSector = dict()
	for name,industryList in sectors.iteritems():
		industryIdsPerSector[name] = []
		for industry in industryList:
			industryIdsPerSector[name].append(industry['Id'])

	return industryIdsPerSector


# Get list of sectors and all industries within those sectors and all companies within those industries
# returns dataframe with columns: [sector_name, industry_id, industry_name, company_symbol, company_name]
def getSectorIndustryData():
	sectors = getSectorData()

	industryIdList = []
	sectorDF = pd.DataFrame()
	
	for name,industryList in sectors.iteritems():
		temp = pd.DataFrame(industryList)
		temp.columns = ['industry_id', 'industry_name']
		temp['sector_name'] = name 
		sectorDF = sectorDF.append(temp)
		#industryIdList.extend(temp['industry_id'])

	sectorDF['industry_id'] = sectorDF['industry_id'].astype(int)
	sectorDF = sectorDF.drop_duplicates()
	#print len(sectorDF), len(sectorDF.drop_duplicates())
	#print sectorDF.head()
	industryIdList = (sectorDF['industry_id'].unique())
	industryIdList.sort()
	#print "sectorIndustryIds:", industryIdList

	industryData = getIndustryData(industryIdList)
	response = json.loads(industryData.content)
	results = response['query']['results']['industry']
	industryDF = pd.DataFrame()
	for industry in results:
		if 'company' in industry:
			data = industry['company']
			if type(data) == list:
				temp = pd.DataFrame(data)
			else:
				temp = pd.DataFrame([data])
			temp.columns = ['company_name', 'company_symbol']
			temp['industry_id'] = industry['id']
			temp['company_name'] = temp['company_name'].map(lambda x: re.sub(r'(.*)\n(.*)', r'\1 \2', x))
			industryDF = industryDF.append(temp)
	
	industryDF['industry_id'] = industryDF['industry_id'].astype(int)
	industryDF = industryDF.drop_duplicates()
	#print len(industryDF), len(industryDF.drop_duplicates())
	#print industryDF.head()
	#tempList = (industryDF['industry_id'].unique())
	#tempList.sort()
	#print "industryIndustryIds:", tempList
	
	joinedDF = industryDF.merge(sectorDF, on="industry_id") #, how="inner", lsuffix="_left", rsuffix="_right"
	joinedDF = joinedDF.drop_duplicates()

	writePandasDataFrameAsJsonToFile(joinedDF, "sector_industry_company.json")

	return joinedDF


def loadSectorIndustryData():
	df = loadJsonDataAsSparkDF("sector_industry_company.json")
	return df


# Get list of sectors and industrigetIndustryDataes belonging to each sector
# not giving data right now (check)
def getSectorDataOld():
	query = "select * from yahoo.finance.sectors"
	#print(query)
	return execQuery(query)


# Get list of companies belonging to these industryIds
def getIndustryData(industryIds):
	industryIds = ",".join(["'" + str(id) + "'" for id in industryIds])
	query = "select * from yahoo.finance.industry where id in (" + industryIds + ")"
	#print(query)
	return execQuery(query)


# Get historical divident data for a symbol
def getDividendHistory(symbol, startDate, endDate):
	query = "select * from yahoo.finance.dividendhistory where startDate='" + startDate + "' \
			and endDate='" + endDate + "' and symbol='" + symbol + "'";
	#print(query)
	return execQuery(query)


# Get historical data for a symbol
def getHistoricalData(symbol, startDate, endDate):
	query = "select * from yahoo.finance.historicaldata where startDate='" + startDate + "' \
			and endDate='" + endDate + "' and symbol='" + symbol + "'";
	#print(query)
	return execQuery(query)


# Get quotes (details) for one or multiple symbols
def getQuotes(symbols):
	symbols = ",".join(["'" + s + "'" for s in symbols])
	query = "select * from yahoo.finance.quotes where symbol in (" + symbols + ")"
	#print(query)
	return execQuery(query)


# Get quotes (summary) for one or multiple symbols
def getQuotesList(symbols):
	symbols = ",".join(["'" + s + "'" for s in symbols])
	query = "select * from yahoo.finance.quoteslist where symbol in (" + symbols + ")"
	#print(query)
	return execQuery(query)


# Execute yql query
def execQuery(query):
	url = "http://query.yahooapis.com/v1/public/yql"
	payload = {'env': 'http://datatables.org/alltables.env', 'format': 'json', 'q': str(query)}
	response = requests.get(url, params=payload)
	#print(response)
	return response


# fetch data from 01-01-startYear to today's date for symbols
# write data for each symbol in separate json file at fileLoc
def fetchData(symbols, startYear, endYear, fileLoc):

	dateFormat = '%Y-%m-%d'
	today = dt.date.today()
	#today = dt.date(2015, 11, 25)
	if endYear == None:
		endYear = today.year

	if startYear <= endYear:
		for symbol in symbols:
			fileName = symbol + '.json'
			if not isfile(join(fileLoc, fileName)): 
				print 'fetching quotes for', symbol
				with open(join(fileLoc, fileName), 'w') as f:
					tempYear = endYear
					df = pd.DataFrame()
					while tempYear >= startYear:
						startDate = dt.date(tempYear, 01, 01)
						endDate = dt.date(tempYear, 12, 31)
						if endDate > today:
							endDate = today
		                
						#print "downloading data from ", startDate, " to ", endDate
						historicalQuotesResponse = getHistoricalData(symbol, startDate.strftime(dateFormat), \
		                                                             endDate.strftime(dateFormat))

						historicalQuotesResponseJson = json.loads(historicalQuotesResponse.content)
						count = historicalQuotesResponseJson['query']['count']

						if count > 0:
							if count == 1:
								# converting dict to list
								quotes = [historicalQuotesResponseJson['query']['results']['quote']]
							else:
								quotes = historicalQuotesResponseJson['query']['results']['quote']
							
							data = pd.DataFrame(quotes)
							df = df.append(data)
							#print "data found:", len(data.index), len(df.index)
						else:
							#print "no data found"
							break
 
						tempYear -= 1

					if len(df.index) > 0:
						print "writing", len(df.index), "records to file", f
						df.to_json(f, orient='records')

			else:
				print 'quotes for', symbol, 'already exists'


# load all stock quotes from files in fileLoc in a spark dataframe
# exception: stackOverflow error
def loadData(fileLoc):
	#fileLoc = './data'
	fileList = listdir(fileLoc)
	#fileList = ['MSFT.json']
	fileList.sort()

	quotes = loadJsonDataAsSparkDF(join(fileLoc, 'dummy.json'))
	quotes.registerTempTable('quotes')
	quotes = sqlContext.sql("SELECT Symbol, Date, Close FROM quotes")

	for f in fileList: 
		path = join(fileLoc, f)
		if isfile(path) and getsize(path) > 0 and f!='dummy.json':
			print "loading data from file", f
			temp = loadJsonDataAsSparkDF(path)
			temp.registerTempTable('temp')
			temp = sqlContext.sql("SELECT Symbol, Date, Close FROM temp")	        
			#print f, temp.count()
			quotes = quotes.unionAll(temp)
			break

	quotes.registerTempTable('quotes')
	#quotes = sqlContext.sql("DELETE FROM quotes WHERE Symbol = 'DMMY'")
	print quotes.count()
	#quotes.show()  
	return quotes 


def gatherStats(fileLoc, sector=None):
	'''df = pd.read_json("sector_industry_company_subset.json")
	if sector != None:
		symbols = list(df[df['sector_name'] == sector]['company_symbol'])
	else:
		symbols = list(df['company_symbol'])

	fileList = [(symbol + ".json") for symbol in symbols]'''
	
	fileList = listdir(fileLoc)
	print "fileListLen:", len(fileList)

	stats = pd.DataFrame()
	for f in fileList: 
		path = join(fileLoc, f)
		if isfile(path) and getsize(path) > 0 and f!='dummy.json':
			#print "loading data from file", f
			quotes = loadJsonDataAsSparkDF(path)
			#quotes.registerTempTable('quotes')
			#quotes.printSchema()

			symbol = f.split(".")[0]
			count = quotes.groupBy("Symbol").count().first()[1]
			startDate = quotes.groupBy("Symbol").agg({"Date": "min"}).first()[1]
			endDate = quotes.groupBy("Symbol").agg({"Date": "max"}).first()[1]

			#print symbol, count, startDate, endDate
			#stats = sqlContext.sql("SELECT Symbol, COUNT(*) AS Count, MIN(t.Date) AS 'Start_Date', \
			#	MAX(t.Date) AS End_Date FROM temp AS t GROUP BY t.Symbol")
			stats = stats.append({"Symbol": symbol, "Count": count, "Start_Date": startDate, "End_Date": endDate}, ignore_index=True)

			#break

	return stats


def gatherStats2(fileLoc, startYear, endYear, sector=None):
	'''df = pd.read_json("sector_industry_company_subset.json")
	if sector != None:
		symbols = list(df[df['sector_name'] == sector]['company_symbol'])
	else:
		symbols = list(df['company_symbol'])

	fileList = [(symbol + ".json") for symbol in symbols]'''
	
	fileList = listdir(fileLoc)
	print "fileListLen:", len(fileList)

	startDate = str(dt.date(startYear, 01, 01))
	endDate = str(dt.date(endYear, 12, 31))

	stats = pd.DataFrame()
	for f in fileList: 
		path = join(fileLoc, f)
		if isfile(path) and getsize(path) > 0 and f!='dummy.json':
			#print "loading data from file", f
			quotes = loadJsonDataAsSparkDF(path)
			quotes.registerTempTable('quotes')
			#quotes.printSchema()

			temp = sqlContext.sql("SELECT Symbol, COUNT(*), MIN(Date), MAX(Date) FROM quotes WHERE Date >= '" + startDate + "' AND Date <= '" + endDate + "' GROUP BY Symbol")
			symbol = temp.first()[0]
			count = temp.first()[1]
			startDate = temp.first()[2]
			endDate = temp.first()[3]

			stats = stats.append({"Symbol": symbol, "Count": count, "Start_Date": startDate, "End_Date": endDate}, ignore_index=True)

			#break

	return stats			


# gather stats for all data in fileLoc directory and store it in a file
def gatherStats3(fileLoc, force=False):
	dirName = fileLoc.split("/")[-2]
	outFilename = PROCESSED_FILE_LOC + "stats_" + dirName + ".json"

	if force or not isfile(outFilename):
		startYear = int(dirName.split("_")[1])
		endYear = int(dirName.split("_")[2])

		fileList = listdir(fileLoc)
		print "fileListLen:", len(fileList)

		stats = pd.DataFrame(index = range(startYear, endYear+1))
		for f in fileList: 
			path = join(fileLoc, f)
			if isfile(path) and getsize(path) > 0 and f!='dummy.json':
				quotes = pd.read_json(path)
				quotes['Year'] = quotes['Date'].apply(lambda x: x.year)
				grp = quotes[['Year', 'Date']].groupby(['Year']).count()

				symbol = f.split('.')[0]
				s = pd.Series(0, index=stats.index)
				for year in grp.index:
					s[year] = grp.loc[year]['Date']

				stats[symbol] = s

		stats.to_json(outFilename)
	else:
		stats = pd.read_json(outFilename)

	return stats


# check number of records for each stock,
# select those stocks for which number of records is equal 
# to the number of records which most stocks have.
# there are 1502 stocks with 2745 stocks
# TODO - running too slow
def findStockSubsetToAnalyse(fileLoc, sector=None):
	stats = gatherStats(fileLoc, sector)
	#grp = stats.groupby('Count').count()
	maxRecords = int(stats['Count'].value_counts().index[0])
	print "maxRecords:", maxRecords
	stats = stats[stats['Count'] == maxRecords]
	stats.index = range(len(stats))
	print "numStocksWithMaxRecords:", stats.count()

	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset_"+sector+".csv"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset.csv"

	print "writing stock symbols to file: ", filename
	stats['Symbol'].to_csv(filename, index=False)

	return stats['Symbol']


def findStockSubsetToAnalyse2(fileLoc, startYear, endYear, sector=None):
	stats = gatherStats2(fileLoc, startYear, endYear, sector)
	#grp = stats.groupby('Count').count()
	maxRecords = int(stats['Count'].value_counts().index[0])
	print "maxRecords:", maxRecords
	stats = stats[stats['Count'] == maxRecords]
	stats.index = range(len(stats))
	print "numStocksWithMaxRecords:", stats.count()

	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset_"+sector+".csv"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset.csv"

	print "writing stock symbols to file: ", filename
	stats['Symbol'].to_csv(filename, index=False)

	return stats['Symbol']


def findStockSubsetToAnalyse3(fileLoc, startYear, endYear, sector=None):
	stats = gatherStats3(fileLoc)
	stats = stats.loc[startYear:endYear]
	stats = stats.sum(axis=0)

	maxRecords = (stats.value_counts()).index[0]
	print "maxRecords:", maxRecords

	symbols = pd.Series()
	i = 0
	for symbol in stats.index:
		if stats[symbol] == maxRecords:
			symbols.set_value(i, symbol)
			i += 1

	print "number of stocks with max records:", len(symbols)

	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset_"+sector+".csv"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset.csv"

	print "writing stock symbols to file:", filename
	symbols.to_csv(filename, index=False)

	return symbols


# criteria can be "return" or "cp" (closing price)
def loadStockSubsetToAnalyse(fileLoc, criteria="return", sector=None):
	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset_"+sector+".csv"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset_"+sector+".json"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset.csv"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset.json"

	print "reading stock symbols from file:", filename
	print "writing closing price data to file:", outFilename

	symbols = pd.read_csv(filename, names=['Symbol'])['Symbol']
	quotes = pd.DataFrame()

	for symbol in symbols:
		#print symbol
		path = join(fileLoc, symbol+'.json')
		if isfile(path):
			temp = pd.read_json(path)['Close']
			
			if criteria == "return":
				# calculating log return
				temp = np.diff(np.log(temp))
				np.insert(temp, 0, 0)
			
			#print temp.head()
			quotes[symbol] = temp
			#break

	quotes.to_json(outFilename)
	#print quotes.columns
	return quotes


def loadStockSubsetToAnalyse2(fileLoc, startYear, endYear, force=False, criteria="return", sector=None):
	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset_"+sector+".csv"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset_"+sector+".json"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + "stock_subset.csv"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset.json"

	print "reading stock symbols from file:", filename
	print "writing closing price data to file:", outFilename

	if force or not isfile(outFilename):
		symbols = pd.read_csv(filename, names=['Symbol'])['Symbol']
		quotes = pd.DataFrame()

		for symbol in symbols:
			#print symbol
			path = join(fileLoc, symbol+'.json')
			if isfile(path):
				temp = pd.read_json(path)[['Date', 'Close']]
				temp['Year'] = temp['Date'].apply(lambda x: x.year)
				temp = temp[(temp['Year'] >= startYear) & (temp['Year'] <= endYear)]['Close']
				#print temp.groupby("Year").count()
				
				if criteria == "return":
					# calculating log return
					temp = np.diff(np.log(temp))
					np.insert(temp, 0, 0)
				
				#print temp.head()
				quotes[symbol] = temp
				#break

		quotes.to_json(outFilename)
	else:
		quotes = pd.read_json(outFilename)
	
	#print quotes.columns
	return quotes


# not tested
def calculateCorrelationMatrix(sector=None):
	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset_"+sector+".json"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "corr_matrix_"+sector+".json"
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "closing_price_subset.json"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "corr_matrix.json"

	print "reading closing price data from file: ", filename

	quotes = pd.read_json(filename)
	corrMat = quotes.corr(method='pearson')

	print "writing correlation matrix to file: ", outFilename
	corrMat.to_json(outFilename)
	return corrMat


'''
threshold = values between 0.0 and 1.0
sector = one of ["Technology", ....]
lib = "nx" or "gt" 
'''
def createGraph(threshold=0.5, sector=None, lib="nx", force=False):
	#sectors = pd.read_json("sector_industry_company.json")
	sectors = pd.read_csv(SECTOR_INFO_FILE)

	th = re.sub(r'([0-9]*)\.([0-9]*)',r'\1\2',str(threshold))
	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "corr_matrix_"+sector+".json"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_"+lib+"_"+sector+"_th"+th+".xml"
		#industry = sectors[sectors['sector_name'] == sector]
		industry = sectors[sectors['Sector'] == sector]
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "corr_matrix.json"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_"+lib+"_th"+th+".xml"
		industry = sectors

	print "reading correlation matrix from file: ", filename
	print "writing graph to file: ", outFilename

	if force or not isfile(outFilename):
		#company = dict(zip(industry['company_symbol'], zip(industry['company_name'], industry['sector_name'])))
		company = dict(zip(industry['Symbol'], zip(industry['Name'], industry['Sector'])))
		#print company

		corrMat = pd.read_json(filename)
		#print corrMat.head()
		symbols = corrMat.columns
		numStocks = len(symbols)

		if lib == "nx":	
			g = nx.Graph()
			for i,sym in enumerate(symbols):
				cluster = 0 #randint(1, 5)
				if sym in company:
					companyName, sectorName = company.get(sym)
				else:
					companyName, sectorName = None, None
				if companyName == None or len(companyName) == 0:
					companyName = "Unavailable"
				if sectorName == None or len(sectorName) == 0:
					sectorName = "Unavailable"
				g.add_node(i, symbol=sym, name = companyName, sector = sectorName, cluster=cluster)

			for i in range(numStocks):
				for j in range(i+1, numStocks):
					w = corrMat[symbols[i]][symbols[j]]
					if abs(w) >= threshold:
						#print "adding edge: (", symbols[i], ",", symbols[j], ",", w, ")" 
						g.add_edge(i, j, weight=float(w))

			print g.number_of_nodes(), g.number_of_edges()
			nx.write_graphml(g, outFilename)

		elif lib == "gt":
			g = Graph(directed=False)
			g.add_vertex(numStocks)

			v_symbol = g.new_vertex_property("string")
			g.vp.symbol = v_symbol
			v_name = g.new_vertex_property("string")
			g.vp.name = v_name
			v_cluster = g.new_vertex_property("int")
			g.vp.cluster = v_cluster
			for i in range(numStocks):
				v = g.vertex(i)
				g.vp.symbol[v] = symbols[i]
				g.vp.name[v] = company.get(symbols[i])
				g.vp.cluster[v] = 0

			e_weight = g.new_edge_property("double")
			g.ep.weight = e_weight

			for i in range(numStocks-1):
				for j in range(i+1, numStocks):
					w = corrMat[symbols[i]][symbols[j]]
					if abs(w) >= threshold:
						#print "adding edge: (", symbols[i], ",", symbols[j], ",", w, ")" 
						g.add_edge(i, j)
						g.ep.weight[g.edge(i, j)] = w

			print g.num_vertices(), g.num_edges()
			g.save(outFilename, fmt="graphml")

		drawGraph(outFilename)
	else:
		g = nx.read_graphml(outFilename)

	getGraphStats(threshold, sector, lib)

	return g


def getGraphStats(threshold=0.5, sector=None, lib="nx"):
	th = re.sub(r'([0-9]*)\.([0-9]*)',r'\1\2',str(threshold))
	if sector != None:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_"+lib+"_"+sector+"_th"+th+".xml"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_stats_"+lib+"_"+sector+"_th"+th
	else:
		filename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_"+lib+"_th"+th+".xml"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_stats_"+lib+"_th"+th

	stats = dict()
	if lib == "nx":
		stats = dict()
		g = nx.read_graphml(filename)
		stats['num_nodes'] = g.number_of_nodes()
		stats['num_edges'] = g.number_of_edges()
		#stats['diameter'] = nx.diameter(g)
		stats['clustering_coeff'] = nx.average_clustering(g)
		stats['avg_degree'] = np.average(nx.degree(g).values())
		stats['degree_hist'] = nx.degree_histogram(g)

		y = stats['degree_hist']
		x = range(len(y))
		
	
	f = open(outFilename + ".txt", "wb")
	f.write(str(stats))
	f.close()

	plt.plot(x, y)
	plt.savefig(outFilename + ".png")
	plt.show()

	return stats


def multiplePlotsOnOneGraph(filenamePrefix, threshold, outFilename):
	import ast

	def trimZeros(a):
		while a and a[-1] == 0:
			a.pop()

	y = []
	label = []
	minLen = 10000
	color = ['r', 'g', 'b']
	i = 0

	for th in threshold:
		filename = filenamePrefix + th + ".txt"
		print filename
		with open(filename, 'r') as f:
			s = f.read()
			stats = ast.literal_eval(s)
			dh = stats['degree_hist']
			y.append(dh)
			label.append(th)
			l = len(dh)
			if l < minLen:
				minLen = l

	            
	x = range(minLen)
	fig, ax = plt.subplots()
	ax.plot(x, y[0][:minLen], 'k--', label=label[0])
	ax.plot(x, y[1][:minLen], 'k:', label=label[1])
	ax.plot(x, y[2][:minLen], 'k', label=label[2])
	legend = ax.legend(loc='upper center', shadow=True)

	plt.savefig(outFilename)
	plt.show()

'''
graph created with nx lib can be drawn using both nx and gt lib
grapg created with gt lib cab be created using gt lib but getting error with nx lib
'''
def drawGraph(inGraphFilename, lib="gt"):
	outFilename = inGraphFilename.split(".")[0] + "_" + lib + ".pdf"
	print "drawing graph to file: ", outFilename

	if lib == "nx":
		'''if graph_layout == 'shell':
	    	graph_pos=nx.spring_layout(G)
	    elif graph_layout == 'spectral':
	        graph_pos=nx.spectral_layout(G)
	    elif graph_layout == 'random':
	        graph_pos=nx.random_layout(G)
	    else:
	        graph_pos=nx.spring_layout(G)'''

		g = nx.read_graphml(inGraphFilename)

		graph_pos=nx.spring_layout(g)

		deg = g.degree().values()
		node_size = [d*100 for d in deg] #nx.eigenvector_centrality_numpy(G, weight="weight").values()
		node_alpha = 0.3
		node_color = nx.get_node_attributes(g, 'cluster').values()
		edge_thickness = 1
		edge_alpha = 0.3
		edge_color = 'blue'
		node_text_size = node_size
		text_font = 'sans-serif'
		edge_text_pos=0.3
		
		#if labels is None:
		#	labels = range(len(graph))

	    #edge_labels = dict(zip(graph, labels))

		# draw graph
		nx.draw_networkx_nodes(g, graph_pos, node_size=node_size, alpha=node_alpha, node_color=node_color)
		nx.draw_networkx_edges(g, graph_pos, width=edge_thickness, alpha=edge_alpha, edge_color=edge_color)
		#nx.draw_networkx_labels(g, graph_pos,font_size=node_text_size, font_family=text_font)
		#nx.draw_networkx_edge_labels(g, graph_pos, edge_labels=edge_labels, label_pos=edge_text_pos)

		plt.savefig(outFilename)
		# show graph
		plt.show()

	elif lib == "gt":
		g = load_graph(inGraphFilename)

		#g = GraphView(gg, vfilt=lambda v: v.out_degree() > 0)
		print g.num_vertices(), g.num_edges()

		deg = g.degree_property_map("in")
		deg.a = 10 * (np.sqrt(deg.a) * 0.5 + 0.4)
		#print "degree len:", len(deg.a)

		ebet = betweenness(g)[1]
		ebet.a /= ebet.a.max() / 10.
		#print "ebet len:", len(ebet.a)	

		eorder = ebet.copy()
		eorder.a *= -1

		groups = g.vp.cluster
		elen = ebet

		pos = sfdp_layout(g)
		#pos = fruchterman_reingold_layout(g)
		#print "pos len:", len(pos)

		control = g.new_edge_property("vector<double>")
		for e in g.edges():
			d = np.sqrt(sum((pos[e.source()].a - pos[e.target()].a) ** 2)) / 5
			control[e] = [0.3, d, 0.7, d]
			
		graph_draw(g, pos=pos, #vertex_text=g.vp.symbol,
			#vertex_size=deg, 
			vertex_fill_color=g.vp.cluster,
			#vertex_font_size=deg, 
			vorder=deg,
			edge_color=ebet, eorder=eorder, edge_control_points=control,
			output=outFilename)


def findFreqOfCliquesInGraph(g):
	cliques = nx.find_cliques(g)
	freq = dict()
	for clique in cliques:
	    clique_size = len(clique)
	    if clique_size in freq:
	        freq[clique_size] += 1
	    else:
	        freq[clique_size] = 1

	return freq


# Plot histogram from dictionary object
def plotHistFromDict(x):
	import matplotlib.pyplot as plt

	#x = {u'Label1':26, u'Label2': 17, u'Label3':30}

	plt.bar(range(len(x)), x.values(), align='center')
	plt.xticks(range(len(x)), x.keys())

	plt.show()


# find communites using clique percolation method (networkx)
def findCommunites(threshold=0.5, sector=None, k=5, force=False):
	th = re.sub(r'([0-9]*)\.([0-9]*)',r'\1\2',str(threshold))
	if sector != None:
		graphInFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_nx_"+sector+"_th"+th+".xml"
		graphOutFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_communities_nx_"+sector+"_th"+th+"_k"+str(k)+".xml"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_communities_nx_"+sector+"_th"+th+"_k"+str(k)+".csv"
	else:
		graphInFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_graph_nx_th"+th+".xml"
		graphOutFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_communities_nx"+"_th"+th+"_k"+str(k)+".xml"
		outFilename = PROCESSED_FILE_LOC + PREFIX + CRITERIA + "stock_communities_nx"+"_th"+th+"_k"+str(k)+".csv"

	print "reading graph from file: ", graphInFilename
	print "writing graph with community info to file: ", outFilename
	print "writing community details in csv format to file: ", outFilename

	if force or not isfile(graphOutFilename):
		g = nx.read_graphml(graphInFilename)
		#freq = findFreqOfCliquesInGraph(g)
		#plotHistFromDict(freq)
		
		comm = nx.k_clique_communities(g, k)
		communities = []
		for c in comm:
			communities.append(c) 
		
		numCommunities = len(communities)
		print "number of communities found: ", numCommunities
		
		colors = range(numCommunities)

		i = 0
		for c in communities:
			for v in c:
				g.node[v]['cluster'] = colors[i] + 1
			i += 1

		nx.write_graphml(g, graphOutFilename)
			
		import csv
		with open(outFilename, "wb") as f:
			writer = csv.writer(f, delimiter='|', quotechar="'", quoting=csv.QUOTE_MINIMAL)
			writer.writerow(["sector", "symbol", "name", "cluster"])
			for v in g:
				writer.writerow([g.node[v]['sector'], g.node[v]['symbol'], g.node[v]['name'], g.node[v]['cluster']])

		results = PROCESSED_FILE_LOC + "results.csv"
		with open(results, "a") as f1:
			f1.write(str(dt.datetime.today()) + "," + outFilename + "," + str(numCommunities) + "," + str(calculateModularity(graphOutFilename)) + "\n")

		drawGraph(graphOutFilename, "gt")


def calculateModularity(graphFilename):
	G = load_graph(graphFilename)
	return modularity(G, G.vp.cluster, weight=G.ep.weight)


def plotSectorHist():
	filename1 = "processed/nasdaq_2012_2013_return_stock_communities_nx_th08_k4.csv"
	filename2 = "processed/nasdaq_2014_2015_return_stock_communities_nx_th08_k4.csv"
	
	df1 = pd.read_csv(filename1, sep="|")
	df2 = pd.read_csv(filename2, sep="|")
	
	z1 = df1['sector'].value_counts()
	z1 = z1.sort_index()
	x = z1.index
	y1 = z1.values
	z2 = df2['sector'].value_counts()
	z2 = z2.sort_index()
	y2 = z2.values

	df = pd.DataFrame(columns=['sector', '2012-2013', '2014-2015'])
	df['sector'] = x
	df['2012-2013'] = y1
	df['2014-2015'] = y2
	df.to_csv("processed/sectors_hist.csv", index=False)

	ind = np.arange(len(x))
	width = 0.25       # the width of the bars

	fig, ax = plt.subplots()
	rects1 = ax.bar(ind, y1, width, color='r')
	rects2 = ax.bar(ind + width, y2, width, color='y')

	# add some text for labels, title and axes ticks
	ax.set_ylabel('Number of Companies')
	ax.set_title('Number of Companies by Sector')
	ax.set_xticks(ind + width)
	ax.set_xticklabels(x)

	ax.legend((rects1[0], rects2[0]), ('2012-2013', '2014-2015'))

	#plt.savefig("processed/sector_hist.png")
	plt.show()


def createSubsetMatrix(matDF, columns):
	subsetDF = pd.DataFrame()
	for column in columns:
		subsetDF[column] = matDF[column][columns]

	return subsetDF


def createGraphFromSubsetOfNodes(threshold=0.5, sector=None, lib="nx"):
	sectors = pd.read_json("nasdaq_sector_industry_company.json")

	if sector != None:
		filename = "corr_matrix_"+sector+".json"
		outFilename = "stock_graph_"+lib+"_"+sector+".xml"
		industry = sectors[sectors['sector_name'] == sector]
	else:
		filename = "corr_matrix.json"
		outFilename = "stock_graph_"+lib+".xml"
		industry = sectors

	company = dict(zip(industry['company_symbol'], industry['company_name']))

	corrMat = pd.read_json(filename)
	#print corrMat.head()
	symbols = corrMat.columns
	numStocks = len(symbols)
	print "number of stocks:", numStocks

	vertex_set = set()
	for i in range(numStocks):
		for j in range(i+1, numStocks):
			w = corrMat[symbols[i]][symbols[j]]
			if abs(w) >= threshold:
				vertex_set.add(symbols[i])
				vertex_set.add(symbols[j])

	subsetCorrMat = createSubsetMatrix(corrMat, vertex_set)
	symbols = subsetCorrMat.columns
	numStocks = len(symbols)
	print "number of stocks:", numStocks

	if lib == "nx":	
		g = nx.Graph()
		for i,sym in enumerate(symbols):
			cluster = 0 #randint(1, 5)
			companyName = company.get(sym)
			if companyName == None or len(companyName) == 0:
				companyName = "Unavailable"
			g.add_node(i, symbol=sym, name = companyName, cluster=cluster)

		for i in range(numStocks):
			for j in range(i+1, numStocks):
				w = subsetCorrMat[symbols[i]][symbols[j]]
				if abs(w) >= threshold:
					#print "adding edge: (", symbols[i], ",", symbols[j], ",", w, ")" 
					g.add_edge(i, j, weight=float(w))

		print g.number_of_nodes(), g.number_of_edges()
		nx.write_graphml(g, outFilename)
		
		return g


#draw graph using graphviz function in gt
def drawGraphviz(g):
	#g = price_network(1500)
	deg = g.degree_property_map("out")
	deg.a = 2 * (np.sqrt(deg.a) * 0.5 + 0.4)
	ebet = betweenness(g)[1]
	pos = sfdp_layout(g)

	graphviz_draw(g, pos=pos, vcolor="blue", elen=10, ecolor="red", output="graphviz-draw.pdf")

	print "printed graph"