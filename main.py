"""
This is the main data generation script
The original dataset this script intends to address is a daily snapshot of customer account balances
This means the dataset will have a core number of accounts, with minor fluctuations in some numeric fields
over daily increments
To do this, we'll follow a two step process
	* create dataset of the first day
	* replicate the first day's data and adjust the appropriate values accordingly
Before doing this, we must interpret an input schema to inform the generation of the dataset
This way we can leverage this program for future data generation

In value generation, we have the following column data types
	* Unique Codes - i.e. "BU102"
	* Date 		   - i.e. 2015-09-01
	* Money  	   - i.e. 1002.30
	* Counter 	   - i.e. 1001
	

Notes:
A preview of the structure of a palette
	palette = {"aa":[1,2,3,4,5], "bb":[1,2,3,4,5], "cc":[1,2,3,4,5]}

"""

CODE_TYPE = "CODE"
DATE_TYPE = "DATE"
MONEY_TYPE = "MONEY"
COUNTER_TYPE = "COUNTER"

HEADER_KEY = "header"
TYPE_KEY = "type"
CARD_KEY = "cardinality"

MONEY_CHANGE_MAX = 20

import re
import sys
import random
import datetime
import copy
import FFB_Parameters as Params


# Global Parameters read from a Parameters file
baseSize = 0
replication = 0
moneyIncrements = 0
headerMap = {}
schema = []
directory = ""
resultsFile = ""

# Global Variables
palette = {}
base = []
	
"""
To create a dictionary of values to select from
We use the headers from the schema as the key of the palette
"""
def createPalette():
	# palette is a global variable
	for dimension in schema:
		header = dimension[HEADER_KEY]
		type = dimension[TYPE_KEY]
		cardinality = dimension[CARD_KEY]	
		if type == CODE_TYPE:
			palette[header] = generateUniqueCodes(header, cardinality)
		elif type == DATE_TYPE:
			palette[header] = generateDates(cardinality)	
	
	
# To create one day's worth of data
def createBase():
	# base table is a global variable, "base"
	index = 0
	while (index < baseSize):
		row = []
		for dimension in schema:
			header = dimension[HEADER_KEY]
			type = dimension[TYPE_KEY]
			cardinality = dimension[CARD_KEY]
			if (type == CODE_TYPE):
				row.append(random.choice(palette[header]))
			elif (type == DATE_TYPE):
				row.append(palette[header][0]) # Here we get the first element since its the first day dataset
			elif (type == MONEY_TYPE):
				row.append(random.randrange(cardinality))
			elif (type == COUNTER_TYPE):
				row.append(header + str(index))
		base.append(row)
		index += 1
		

# Replicate the base table with minor adjustments to the money typed dimension and daily increments to the date dimension
def replicateBase(rIndex): # rIndex will offset the replicated copy 
	replica = copy.deepcopy(base)
	dIndex = 0 # dIndex will offset the dimension in the schema
	for dimension in schema:
		type = dimension[TYPE_KEY]
		if type == DATE_TYPE:
			replica = incrementDates(replica, dimension, dIndex, rIndex)
		elif type == MONEY_TYPE:
			replica = adjustMoney(replica, dimension, dIndex, rIndex)
		dIndex += 1
	return replica
	

def main():
	recordGlobalParams()
	
	createPalette()
 	createBase()
 	results = copy.deepcopy(base)
 	
 	index = 1
 	while (index <= replication):
 		results.extend(replicateBase(index))
		index += 1
	
	publishFactTable(results)
	publishDimensions()
	
	

	
"""
******** Helper Methods *********
"""	



def publishFactTable(results):
 	file = open(directory + resultsFile, "w")
 	headers = []
 	for dimension in schema:
 		header = dimension[HEADER_KEY]
 		if header in headerMap:
 			headers.append(headerMap[header])
 		else:
 			headers.append(header)
 	headers = ",".join(map(str,headers))
 	file.write(headers + "\n")
 	
 	for row in results:
 		row = ",".join(map(str,row))
 		file.write(row + "\n")
  		# print row

	file.close()	
	
def publishDimensions():
	for dimension in schema:
		type = dimension[TYPE_KEY]
		header = dimension[HEADER_KEY]
		if type == CODE_TYPE:
			file = open(directory + headerMap[header], "w")
			file.write(headerMap[header] + "\n")
			for val in palette[header]:
				file.write(val + "\n")
			file.close()

# Create Header of base table
def getTypes():
	typeList = []
	for dimension in schema:
		typeList.append(dimension[TYPE_KEY])
	return typeList
		
# Increment dates of the given table by rIndex days
def incrementDates(replica, dimension, dIndex, rIndex):
	header = dimension[HEADER_KEY]
	for row in replica:
		row[dIndex] = palette[header][rIndex]
	return replica
	
	
# Increment money by a random amount, currently hardcoded at $2000 max
def adjustMoney(replica, dimension, dIndex, rIndex):
	header = dimension[HEADER_KEY]
	for row in replica:
		row[dIndex] = str(int(row[dIndex]) + random.randrange(MONEY_CHANGE_MAX))
	return replica
	
	# Given the key of a palette, return the corresponding dimension
def getSchema(key):
	for dimension in schema:
		header = dimension[HEADER_KEY]
		if header == key:
			return dimension
	return None
	
# To generate a list of values for a single dimension
def generateUniqueCodes(prefix, cardinality):
	list = []
	index = 0
	while (index < cardinality):
		list.append(prefix + str(index))
		index += 1
	return list

# Gives you the date time object
def generateDates(timeRange):
	list = []
	index = 0
	while (index <= timeRange): 
		date = datetime.date.today() + datetime.timedelta(index)
		list.append(date.strftime("%Y-%m-%d"))
		index += 1
	return list
	
# Create the references to the parameters in the parameter file
def recordGlobalParams():
	global baseSize 
	global replication
	global moneyIncrements
	global headerMap
	global schema
	global directory
	global resultsFile
	baseSize = Params.baseSize
	replication = Params.replication
	moneyIncrements = Params.moneyIncrements
	headerMap = Params.headerMap
	schema = Params.schema
	directory = Params.directory
	resultsFile = Params.resultsFile
	
	
"""
******** main() *********
"""


main()



