# Date: 1/1/2015
# Author: Miranda Lv
# Purpose: Produce level 0 location product
# Input: inputs/data/toolkit export. 
# Notice: Be aware of the delimiter of your input toolkit export, the export might be comma separated.condescending

# Inputs: 1. toolkit export 2. output tsv file

import pandas
import urllib2
import json
from time import sleep
import sys
import getopt
#from datetime import datetime

#reload(sys)
#sys.setdefaultencoding('utf-8')

"""
opts, args= getopt.getopt(sys.argv[1:], "ho:v", ["help", "output="])
loc = args[0]
outf = args[1]
"""
loc=r"/home/snowhui/Downloads/asdb_input.csv"
outf = r"/home/snowhui/Downloads/AsDB_locations.csv"

data = pandas.read_csv(loc, sep='\t') # Locations Level 0 file
newdata = dict()
newtitle = list()
gid = sorted(list(set(list( data.GEONAME_ID ))))
place_names = [] # Will be populated with place names directly from GeoNames API
latls = list()
lngls = list()
loctypels = list()
countadmcodels = list()
countadmnamels = list()
geoname_id = list()

print gid
data_proj_prec = data[["PROJECT_ID", "GEONAME_ID", "PRECISION_CODE"]]
    
def main():
  count = 0
  for i in gid:
    count += 1
    valid_place_name = False
    sleep(0.6)

    url = "http://api.geonames.org/hierarchyJSON?geonameId=" + str(int(i)) + "&username=jpowell"

    error_count = 0

    while valid_place_name == False:

      try:
		  
		  response = urllib2.urlopen(url).read()
		  json_result = json.loads(response)
		  place = json_result['geonames'][-1]['name']
		  lat = json_result['geonames'][-1]['lat']
		  lng = json_result['geonames'][-1]['lng']
		  loctypecode = json_result['geonames'][-1]['fcode'] #PPL
		  countrycode = json_result['geonames'][-1]['countryCode'] #NP
		  countryname = json_result['geonames'][-1]['countryName'] # Nepal
		  admcode1 = json_result['geonames'][-1]['adminCode1'] #00
		  admname1 = json_result['geonames'][-1]['adminName1']
		  
		  if admname1 == "":
			  countadmname = countryname
		  else:
			  countadmname = countryname + "|" + admname1
			  
		  if admcode1 == "":
			  countadmcode = countrycode
		  else:
			  countadmcode = countrycode + "|" + admcode1
			  
		  valid_place_name = True
        #print(str(datetime.now())) # To keep track of time taken per request

      except ValueError: # TODO: tweak to catch specific errors (probably KeyError and ValueError)
        error_count += 1
        print "ERROR, Retrying..."

      except KeyError:
        error_count += 1
        print "ERROR, Retrying..."

      if error_count > 50:
        print "No results for: ", i
        place = "ERROR: Not Found"
        break


    place_names.append(place)
    latls.append(lat)
    lngls.append(lng)
    loctypels.append(loctypecode)
    countadmcodels.append(countadmcode)
    countadmnamels.append(countadmname)
    #admcode1ls.append(admcode1)
    #admname1ls.append(admname1)


    #print str(i) + " : " + place
    print str(i)
    print "line number:", count
    print "url: ", url
    print '-'*30
  
  
  #newdata['project_id'] = data.PROJECT_ID
  newdata['GEONAME_ID'] = gid
  #newdata['precision_code'] = data.PRECISION_CODE
  newdata['place_name'] = place_names
  newdata['latitude'] = latls
  newdata['longitude'] = lngls
  newdata['location_type_code'] = loctypels
  newdata['geoname_adm_code'] = countadmcodels
  newdata['geoname_adm_name'] = countadmnamels
  #newdata['ids'] = data.ids
  print newdata
  df = pandas.DataFrame.from_dict(newdata)
  
  #print data_proj_prec
  out_df = pandas.merge(data_proj_prec, df, how='left', on='GEONAME_ID')
  newdf = out_df.rename(columns={'PRECISION_CODE':'precision_code','GEONAME_ID': 'geoname_id', 'PROJECT_ID': 'project_id'})
  #newdf = out_df.rename(columns={'precision_code':'precision_code','geoname_id': 'geoname_id', 'project_id': 'project_id'})
  
  cols = ['project_id','geoname_id', 'precision_code','place_name', 'latitude', 'longitude', 'location_type_code', 'geoname_adm_code', 'geoname_adm_name']
  newdf = newdf[cols]
  newdf.to_csv(outf, sep='\t', columns=cols, encoding='utf-8', index=False)

  print "SUCCESS"


if __name__ == "__main__":
  main()
