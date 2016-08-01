from yahoo_finance_api import *
import json
import numpy as np
import pandas as pd
import datetime as dt
import networkx as nx
import matplotlib.pyplot as plt


# directory location where fetched data will be stored
fileLoc = "data/data_2005_2015/"

# fetch all stock symbols traded on NASDAQ
symbols = getSymbols('nasdaq')

# fetch historical data for all stocks symbols found in previous step
fetchData(symbols, 2005, 2015, fileLoc)


# processing subset of data
startYear = 2014
endYear = 2015

# gather stats for the subset of the data
stats = gatherStats3(fileLoc)

# check how many stocks in the subset existed for the entire period between start and end year
symbols = findStockSubsetToAnalyse3(fileLoc, startYear, endYear)

# load closing prices of the subset of stocks in dataframe
quotes = loadStockSubsetToAnalyse2(fileLoc, startYear, endYear)

# generate correlation matrix for the subset of stocks
corrMat = calculateCorrelationMatrix()

# create graph for subset of stocks for particular threshold 
g = createGraph(0.8)

# find communities for clique size k in graph created in previous step
findCommunites(threshold=0.8, k=3)