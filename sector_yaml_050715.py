# Date: 05/07/2015
# Author: Miranda Lv
# Notes: creating yaml sectors

import pandas as pd
import csv
import json
import yaml

inf = r"/home/snowhui/itpir_repo/minerva-colombia-geocoded-dataset/scratch/sectors1.csv"
outf = r"/home/snowhui/itpir_repo/minerva-colombia-geocoded-dataset/scratch/sector1_yaml.tsv"

def subsector_yaml(incsv, outcsv):
	rawData = csv.DictReader(open(incsv, 'rb'), delimiter='\t')
	fieldnames = rawData.fieldnames
	data = pd.read_csv(incsv, encoding='utf-8', sep='\t')
	sectorlist = list()
	for row in rawData:
		print row
		dic = dict()
		numsec = row["sector"].count("|")
		numsubsec = row["subsector"].count("|")
		newnumsec = numsec +2
		newnumsubsec = numsubsec +2
		for i in range(1, newnumsec):
			sectornum = "sector" + str(i)
			dic[sectornum] = row["sector"].split("|")[(i-1)]
		for j in range(1, newnumsubsec):
			subsectornum = "subsector" + str(j)
			dic[subsectornum] = row["subsector"].split("|")[(j-1)]
		json_dic = json.dumps(dic).decode('unicode-escape').encode('utf-8')
		yml = yaml.dump(yaml.load(json_dic), default_flow_style=False).decode('unicode-escape').encode('utf-8')
		yml = yml.rstrip('\r\n')
		sectorlist.append(yml)
	
	data["sector_value"] = sectorlist
	data.to_csv(outcsv, sep='\t', index=False, encoding='utf-8') #encoding='utf-8'

def sector_yaml(incsv, outcsv):
	rawData = csv.DictReader(open(incsv, 'rb'), delimiter='\t')
	fieldnames = rawData.fieldnames
	data = pd.read_csv(incsv, encoding='utf-8', sep='\t')
	sectorlist = list()
	for row in rawData:
		print row
		dic = dict()
		numsec = row["sector"].count("|")
		#numsubsec = row["subsector"].count("|")
		newnumsec = numsec +2
		#newnumsubsec = numsubsec +2
		for i in range(1, newnumsec):
			sectornum = "sector" + str(i)
			dic[sectornum] = row["sector"].split("|")[(i-1)]
		json_dic = json.dumps(dic).decode('unicode-escape').encode('utf-8')
		yml = yaml.dump(yaml.load(json_dic), default_flow_style=False).decode('unicode-escape').encode('utf-8')
		yml = yml.rstrip('\r\n')
		sectorlist.append(yml)
	
	data["sector_value"] = sectorlist
	data.to_csv(outcsv, sep='\t', index=False, encoding='utf-8') #encoding='utf-8'

sector_yaml(inf, outf)
