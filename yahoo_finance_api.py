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


# Get list of companies for particular stock exchange
# exchange: {'nasdaq', 'nyse', 'amex'}
def getSymbols(exchange):
	url = "http://www.nasdaq.com/screening/companies-by-name.aspx"
	payload = {'letter': '0', 'exchange': str(exchange), 'render': 'download'}
	response = requests.get(url, params=payload)
	data = re.sub(r'\,(\r\n)', r'\1', response.content)
	symbols = (pd.read_csv(StringIO(data), quotechar='"'))['Symbol']
	symbols = symbols.sort_values()
	symbols.index = range(len(symbols))
	return symbols

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

	f = open("sector_industry_company.json", "wb")
	joinedDF.to_json(f, orient='records')
	f.close()

	return joinedDF


def loadSectorIndustryData():
	df = pd.DataFrame("sector_industry_company.json")
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
def fetchData(symbols, startYear, fileLoc):
	dateFormat = '%Y-%m-%d'
	#today = dt.date.today()
	today = dt.date(2015, 11, 25)
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
	config = SparkConf()
	#self.awsAccessKeyId="<awsAccessKeyId>"
	#self.awsSecretAccessKey="<awsSecretAccessKey>"
	#config.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
	#config.set("fs.s3n.awsAccessKeyId", self.awsAccessKeyId)
	#config.set("fs.s3n.awsSecretAccessKey", self.awsSecretAccessKey)

	sc = SparkContext(conf=config)
	sqlContext = SQLContext(sc)

	def loadJsonDataAsSparkDF(filename):
	    #df = sqlContext.read.json("s3n://"+self.awsAccessKeyId+":"+self.awsSecretAccessKey+"@"+self.bucketName+"/"+fileName)
	    df = sqlContext.read.json(filename)
	    return df

	#fileLoc = './data'
	#fileList = listdir(fileLoc)
	fileList = ['MSFT.json']
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

def gatherStats(fileLoc):
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	def loadJsonDataAsSparkDF(filename):
	    df = sqlContext.read.json(filename)
	    return df

	stats = pd.DataFrame()
	for f in listdir(fileLoc): 
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

# there are 1502 stocks with 2745 stocks
def findStockSubsetToAnalyse(fileLoc):
	stats = gatherStats(fileLoc)
	grp = stats.groupby('Count').count()
	maxRecords = grp.max(axis=1).index[0]
	stats = stats[stats['Count'] == maxRecords]
	stats.index = range(len(stats))
	# print stats.count()
	stats['Symbol'].to_csv('stock_subset.csv', index=False)

	return stats['Symbol']

def loadStockSubsetToAnalyse(fileLoc):
	symbols = pd.read_csv('stock_subset.csv', names=['Symbol'])['Symbol']
	quotes = pd.DataFrame()

	for symbol in symbols:
		#print symbol
		path = join(fileLoc, symbol+'.json')
		if isfile(path):
			temp = pd.read_json(path)
			#print temp.head()
			quotes[symbol] = temp['Close'] 
			#break

	quotes.to_json('closing_price_subset.json')
	#print quotes.columns
	return quotes

# not tested
def calculateCorrelationMatrix(quotes):
	quotes = pd.read_json('closing_price_subset.json')
	corrMat = quotes.corr(method='pearson')

	corrMat.to_json('corr_matrix.json')
	return corrMat

# using networkx lib
def createGraphNX(threshold=0.5):
	corrMat = pd.read_json('corr_matrix.json')
	#print corrMat.head()
	symbols = corrMat.columns
	numStocks = len(symbols)
	
	stockGraph = nx.Graph()
	for i in range(numStocks):
		for j in range(i+1, numStocks):
			w = corrMat[symbols[i]][symbols[j]]
			if abs(w) >= threshold:
				#print "adding edge: (", symbols[i], ",", symbols[j], ",", w, ")" 
				stockGraph.add_edge(symbols[i], symbols[j], weight=float(w))
				#stockGraph.add_edge(symbols[i], symbols[j])

	nx.write_graphml(stockGraph, "stock-graph-nx.xml")
	#print stockGraph.graph
	return stockGraph

# using graph-tool lib
def createGraphGT(threshold=0.5):
	corrMat = pd.read_json('corr_matrix.json')
	#print corrMat.head()
	symbols = corrMat.columns
	numStocks = len(symbols)
	
	g = Graph(directed=False)
	#print g.num_vertices(), g.num_edges()
	g.add_vertex(numStocks)

	v_symbol = g.new_vertex_property("string")
	g.vp.symbol = v_symbol
	for i in range(numStocks):
		g.vp.symbol[g.vertex(i)] = symbols[i]

	e_weight = g.new_edge_property("double")
	g.ep.weight = e_weight
	#vertex_set = set()

	#print g.num_vertices(), g.num_edges()
	for i in range(numStocks-1):
		for j in range(i+1, numStocks):
			w = corrMat[symbols[i]][symbols[j]]
			if abs(w) >= threshold:
				#print "adding edge: (", symbols[i], ",", symbols[j], ",", w, ")" 
				#vertex_set.add(i)
				#vertex_set.add(j)
				g.add_edge(i, j)
				g.ep.weight[g.edge(i, j)] = w

	#print g.num_vertices(), g.num_edges()

	#print "number of nodes:", len(vertex_set)
	#for v in vertex_set:
	#	g.vp.symbol[g.vertex(v)] = symbols[v]

	#print g.num_vertices(), g.num_edges()
	g.save("stock-graph-gt.xml", fmt="graphml")

	return g

# draw graph created using createGraphGT()
def drawGraph():
	g = load_graph("stock-graph-gt.xml")
	#g = price_network(1500)

	#g = GraphView(gg, vfilt=lambda v: v.out_degree() > 0)
	print g.num_vertices(), g.num_edges()

	deg = g.degree_property_map("in")
	deg.a = 4 * (np.sqrt(deg.a) * 0.5 + 0.4)
	#print "degree len:", len(deg.a)

	ebet = betweenness(g)[1]
	ebet.a /= ebet.a.max() / 10.
	#print "ebet len:", len(ebet.a)	

	eorder = ebet.copy()
	eorder.a *= -1

	pos = sfdp_layout(g)
	#pos = fruchterman_reingold_layout(g)
	#print "pos len:", len(pos)

	control = g.new_edge_property("vector<double>")
	for e in g.edges():
		d = np.sqrt(sum((	pos[e.source()].a - pos[e.target()].a) ** 2)) / 5
		control[e] = [0.3, d, 0.7, d]
		
	graph_draw(g, pos=pos, vertex_text=g.vp.symbol, vertex_size=deg, vertex_fill_color=deg, \
		vertex_font_size=deg, vorder=deg, \
		edge_color=ebet, eorder=eorder, edge_pen_width=ebet, edge_control_points=control, \
		output="stock-graph.pdf")


def drawGraph1():
	g = load_graph("stock-graph-gt.xml")

	#g = GraphView(gg, vfilt=lambda v: v.out_degree() > 0)
	print g.num_vertices(), g.num_edges()

	deg = g.degree_property_map("in")
	deg.a = 4 * (np.sqrt(deg.a) * 0.5 + 0.4)
	#print "degree len:", len(deg.a)

	ebet = betweenness(g)[1]
	ebet.a /= ebet.a.max() / 10.
	#print "ebet len:", len(ebet.a)	

	eorder = ebet.copy()
	eorder.a *= -1

	pos = sfdp_layout(g)
	#pos = fruchterman_reingold_layout(g)
	#print "pos len:", len(pos)

	control = g.new_edge_property("vector<double>")
	for e in g.edges():
		d = np.sqrt(sum((	pos[e.source()].a - pos[e.target()].a) ** 2)) / 5
		control[e] = [0.3, d, 0.7, d]
		
	graph_draw(g, pos=pos, vertex_text=g.vp.symbol, vertex_size=deg, vertex_fill_color=deg, \
		vertex_font_size=deg, vorder=deg, \
		edge_color=ebet, eorder=eorder, edge_control_points=control, \
		output="stock-graph.pdf")


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


def plotHistFromDict(x):
	import matplotlib.pyplot as plt

	#x = {u'Label1':26, u'Label2': 17, u'Label3':30}

	plt.bar(range(len(x)), x.values(), align='center')
	plt.xticks(range(len(x)), x.keys())

	plt.show()