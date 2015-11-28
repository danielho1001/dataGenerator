"""
This is the parameter file for FFB's mock data

M = Market
P = Product
CC = CostCenter
BU = Business Unit
CA = Client Account
GL = GL Account
"""

directory = "FFB/"
resultsFile = "FFB_accountBalances.csv"
baseSize = 1000
replication = 30
moneyIncrements = 20
headerMap = {"M":"MarketID",
			 "P":"ProductID",
			 "CC":"CostCenterID",
			 "BU":"BusinessUnitID",
			 "CA":"ClientAccountID",
			 "GL":"GLAccountID"}
			 
schema = [{"header":"date","type":"DATE","cardinality":replication},
		  {"header":"balance","type":"MONEY","cardinality":10000},
		  {"header":"M","type":"CODE","cardinality":20}, 
		  {"header":"P","type":"CODE","cardinality":20}, 
		  {"header":"CC","type":"CODE","cardinality":20},
		  {"header":"BU","type":"CODE","cardinality":20},
		  {"header":"CA","type":"COUNTER","cardinality":baseSize},
		  {"header":"GL","type":"CODE","cardinality":20}]