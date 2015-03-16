"""
Revisions
Date: 2015-03-16
Function: __scrapeResults
Changes: Use the newly implemented SOAP response.

Date: 2014-07-11
Function: __scrapeResults
Changes: Verify securityParams object is valid before attempting to use it.

Date: 2013-07-09
Function: waterQualityAdvisor:processData
Changes: The web page we were scraping to get the actual sample data has been taken down. Added code to handle the
webquery when nothing is returned. We look use the historical data for this case as well.
"""
import sys
#import requests
import logging.config
import optparse
import ConfigParser
import copy
from lxml import etree    
import geojson
#import urllib
#import urllib2
import requests
import socket 
import datetime
from pytz import timezone

import csv
from decimal import *
import httplib

from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor

def docExtract(srcMap,doc):
  """
  Document extract.
    doc - html document to extract from
    map - data to extract
  """
  #Deepcopy the source map so we don't alter it.
  templateMap = copy.deepcopy(srcMap)
  
  if not isinstance(doc,etree._Element):
    doc = etree.HTML(doc)
  data = {}
  for k,v in templateMap.iteritems():
    if v is None: continue
    if isinstance(v,dict):
      if 'each' not in v: continue
      val = doc.xpath(v['each'])
      del v['each']
      data[k] = []
      for doc in val:
        result =  docExtract(v,doc)
        if(result):
          data[k].append(result)
    else:
      val = doc.xpath(v)
      if len(val) > 1:
        data[k] = []
        for v in val:
          #data[k].append(v.encode('ascii', 'xmlcharrefreplace') )
          data[k].append(v)
      else:
        for v in val:
          #data[k] = v.encode('ascii', 'xmlcharrefreplace')
          data[k] = v
  return data


  
"""
Class: waterQualityAdvisory
Purpose: Used to scrape the DHEC website for the station data as well as importing the station metadata from their csv file(from excel sheet).
"""
class waterQualityAdvisory(object):
  def __init__(self, baseUrl, logger=True):
    self.baseUrl = baseUrl    #The URL to the DHEC page to get the station data. 
                              #This is without the POST parameter 'station' which is added at the point the request is created.

    #httplib.HTTPConnection.debuglevel = 1                         
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)
      #requests_log = logging.getLogger("requests.packages.urllib3")
      #requests_log.setLevel(logging.DEBUG)
      #requests_log.propagate = True      
    
    self.pageDataDict = {}
    self.pageDataDict['results'] = {
        'each' : "//table[contains(@id, 'GridView1')]/tr",
        'date' : "td[@align='left']//text()",
        'value' : "td[@align='center']//text()"
    }
  """
  Function: createStationGeoJSON
  Purpose: Creates a geoJSON file containing all the station data read from a CSV file.
  Parameters:
    csvFile - String with the full path to the csv file we are going to import.
    geoJSONOutfile - String with full path to the geoJSON file we are creating from the csv file.
  Return:
    None
  """
  def createStationGeoJSON(self, csvFile, geoJSONOutfile):
    import csv
    import math
    if(self.logger):
      self.logger.info("Creating station geoJson file.")
    try:
      colNames = ['station',
                  'address',
                  'city-county',
                  'epabeachid',
                  'beachname',
                  'beachlen',
                  'latitude',
                  'longitude',
                  'permanentsign'
                 ]
      srcFileObj = open(csvFile, 'rU')
      srcFile = csv.DictReader(srcFileObj, colNames)
      if(self.logger):
        self.logger.info("Opened input file: %s" % (csvFile))
      destFile = open(geoJSONOutfile, "w")
      if(self.logger):
        self.logger.info("Opened output file: %s" % (geoJSONOutfile))
        
    except IOError,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      lineCnt = 0
      features = []
      try:
        locale = None
        beach = None
        for line in srcFile:
          #Bump past first 2 lines since they are the header rows.
          if(lineCnt > 1):
            #Verify we have valid coordinates.
            if(len(line['latitude']) and len(line['longitude'])):
              try:              
                latitude = float(line['latitude'].strip())
                longitude = float(line['longitude'].strip())

                geometry = geojson.Point(coordinates=[longitude, latitude])
              except ValueError,e:
                if(self.logger):
                  self.logger.error("Line: %d invalid value for either latitude or longitude" % (lineCnt))
                continue
              #Now let's get the rest of the properties.
              else:
                properties = {}
                if(len(line['city-county'].strip()) and locale != line['city-county']):
                  locale = line['city-county']
                if(len(line['beachname'].strip()) and beach != line['beachname']):
                  beach = line['beachname']
                properties['station'] = line['station']
                properties['desc'] = line['address']
                properties['locale'] = locale
                properties['epaid'] = line['epabeachid']
                properties['beach'] = beach
                properties['len'] = line['beachlen']
                properties['sign'] = False
                if(line['permanentsign'].lower() == 'yes'):
                  properties['sign'] = True
                #Create the geoJson feature object
                feature = geojson.Feature(id=properties['station'], geometry=geometry, properties=properties)
                features.append(feature)
                
                if(self.logger):
                  self.logger.info("Adding station: %s" % (properties['station']))
          lineCnt += 1 
        featureColl = geojson.FeatureCollection(features=features)
  
        destFile.write(geojson.dumps(featureColl, separators=(',', ':')))
        #destFile.write(geojson.dumps(featureColl, sort_keys=True, indent=4 * ' '))
        
        if(self.logger):
          self.logger.info("JSON results written to file.")
      except Exception,e:
        if(self.logger):
          self.logger.exception(e)
      destFile.close()
      srcFileObj.close()

  """
  Function: createHistoricalJSON
  Purpose: From a CSV file of historical water quality data, create/append to a JSON file of the data.
  Parameters: 
    inputFile - String with the filename of the CSV file to import
    outputFile - Filename of the JSON file to create/append to.
  Returns:
    JSON object with the data.
  """
  def createHistoricalJSON(self, inputFilename, outputFilename):
    waterQualityJson = None
    """
    fieldNames = ["Station",
                  "Inspection Date",
                  "Insp Time",
                  "Lab Number",
                  "Inspection Type",
                  "E Sign",
                  "ETCOC",
                  "Tide",
                  "Wind/Curr",
                  "Weather"]
    """
    fieldNames = [
      "COLLECTION DATE",
      "COLLECTOR",
      "STATION",
      "INSPECTION TIME",
      "LABSAMPLENUMBER",
      "ETCOCSIGN",
      "ETCOC",
      "RAINFALL",
      "WIND",
      "TIDE",
      "WEATHER",
      "INSPECTIONTYPE"
    ]
    try:
      inputFile = open(inputFilename, 'rU')
      dataFile = csv.DictReader(inputFile, fieldNames)
      outFile = open(outputFilename, 'w')
    except IOError, e:
      if(self.logger):
        self.logger.exception(e)
    else:
      jsonObj = {}
      lineNum = 0
      for line in dataFile:
        if(lineNum > 0):
          stationData = []
          if(line['STATION'] in jsonObj):
          #if(line['Station'] in jsonObj):
            stationData = jsonObj[line['STATION']]
            #stationData = jsonObj[line['Station']]
          else:
            jsonObj[line['STATION']] = stationData
            #jsonObj[line['Station']] = stationData
          
          #dateVal = datetime.datetime.strptime(line['Inspection Date'], "%d-%b-%y")
          #dateVal = datetime.datetime.strptime(line['Inspection Date'], "%m/%d/%Y")
          dateVal = datetime.datetime.strptime(line['COLLECTION DATE'], "%m/%d/%y %H:%M")
          
          #timeVal = datetime.datetime.strptime(line['Insp Time'], "%H%M")          
          #dateVal += (' ' + timeVal.strftime("%H:%M:00"))
          #dateVal = datetime.datetime.strptime(dateVal, "%Y-%m-%d %H:%M:00")
          #stationDict[dateVal] = line['ETCOC']
          wqObj = {'date' : dateVal, 'value' : line['ETCOC']}
          stationData.append(wqObj)
          
        lineNum += 1
    #Sort
    for index,dataObj in enumerate(jsonObj):
      stationEntries = jsonObj[dataObj]
      stationEntries.sort(key=lambda r: r['date'])
      #Convert the datetime objects into strings for the JSONification process.
      for index2,stationObj in enumerate(stationEntries):
        #stationObj['date'] = stationObj['date'].strftime("%Y-%m-%d %H:%M:00")
        stationObj['date'] = stationObj['date'].strftime("%Y-%m-%d")
    try:
      outFile.write(geojson.dumps(jsonObj, sort_keys=True, indent=4 * ' '))      
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    outFile.close()  
    inputFile.close()        
    return(jsonObj)
  """
  Function: processData
  Purpose: Scrapes the DHEC web pages for each station pulling in all the test results. The pages are created
    through a POST command and the data is in a table in the returned page.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
    jsonOutputFilepath - String with the output path where each stations results geoJSON file is created.
      These results differ from the station geoJSON object in that there is an extra 'properties' parameter, 'test', that has a 
      key 'beachadvisories' that is an array of results data.
  Return:
    None
  """
  def processData(self, stationNfoList, jsonOutputFilepath, historyWQ):
    if(self.logger):
      self.logger.info("Begin data processing.")
    resultsData = self.__scrapeResults(stationNfoList)
    #DWR 2012-12-05
    if(len(resultsData)):
      for index,stationName in enumerate(resultsData):      
        if(len(resultsData[stationName]['results']) == 0):
          if(self.logger):
            self.logger.debug("Station: %s no results from webquery, adding in historical." % (stationName))
          if(stationName in historyWQ):
            resultsData[stationName]['results'] = historyWQ[stationName]
    #DWR 2013-07-09
    #No result at all.
    else:
      resultsData = {}
      if(self.logger):
        self.logger.debug("Web query failed.")
      for feature in stationNfoList['features']:
        stationName = feature['id']
        resultsData[stationName] = {'results' : {}}
        if(self.logger):
          self.logger.debug("Station: %s no results from webquery, adding in historical." % (stationName))
        if(stationName in historyWQ):
            resultsData[stationName]['results'] = historyWQ[stationName]
        else:
          if(self.logger):
            self.logger.debug("Station: %s not found in historical." % (stationName))
            
      
    self.__outputGeoJson(stationNfoList, resultsData, jsonOutputFilepath)
    if(self.logger):
      self.logger.info("Data processing completed.")
  """
  Function: findSecurityParams
  Purpose: The web page was re-written implementing crappy dot net security to prevent injection.
  """  
  def findSecurityParams(self, htmlDoc):
    params = None
    try:
      params =  { '__VIEWSTATE' : str(htmlDoc.xpath("//input[@id='__VIEWSTATE'] /@value")[0]),
                  '__EVENTVALIDATION' : str(htmlDoc.xpath("//input[@id='__EVENTVALIDATION' ] /@value")[0])
                }
    except Exception,e:
      if self.logger :
        self.logger.exception(e)
    return params
  """
  Function: __scrapeResults
  Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
    station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
  Return:
    None
  """
  #2015-03-16 DWR
  #Implement function to use the SOAP response.
  def __scrapeResults(self, stationNfoList):
    results = {}
    if self.logger:
      logging.getLogger('suds.client').setLevel(logging.DEBUG)
      self.logger.info("SOAP request for beach data.")
    try:
      schema_url = "http://gisweb01.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
      schema_import = Import(schema_url)
      schema_doctor = ImportDoctor(schema_import)

      soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
      if self.logger:
        self.logger.debug("Client: %s" % (soap_client))
      for year_cnt in range(1, -1, -1):
        year = datetime.datetime.now()
        if self.logger:
          self.logger.debug("SOAP GetBeachData request for year: %d" % (year.year - year_cnt))
        response = soap_client.service.GetBeachData(year=year.year - year_cnt)

        for diffgram in response.diffgram:
          for DocumentElement in diffgram.DocumentElement:
            for beachTable in DocumentElement.beachTable:
              data = {
                'date': "",
                'value': ""
              }
              """
              data = {
                'id': "",
                'date': "",
                'value': "",
                'sign': "",
                'station': ""
              }
              """
              data['station'] = beachTable.STATION[0]
              if data['station'] not in results:
                results[data['station']] = {'results': []}

              #data['id'] = beachTable._id
              #2014-05-13T00:00:00-04:00
              date_parts = beachTable.START_DATE[0].split('T')
              data['date'] = date_parts[0]
              data['value'] = beachTable.ETCOC[0]
              #data['sign'] = beachTable.ETCOC_MOD[0]
              results[data['station']]['results'].append(data)

        #with open("/Users/danramage/tmp/response.txt", "w") as outfile:
        #  outfile.write(str(response))
    except Exception, e:
      if self.logger:
        self.logger.exception(e)
    if self.logger:
      self.logger.debug("SOAP request completed.")
    return results
  """
  def __scrapeResults(self, stationNfoList):
    if(self.logger):
      self.logger.info("Scraping web pages.")
    results = {}
    #Get the starting page so we can get the "security" crap.
    if(self.logger):
      self.logger.debug("Requesting initial page: %s" % (self.baseUrl))
    # Open the url
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:24.0) Gecko/20100101 Firefox/24.0"}
    req = requests.get(self.baseUrl, headers=headers)
    if(req.status_code == 200):
      initDoc = etree.HTML(req.text)
      #if(self.logger):
      #  self.logger.debug(req.text)
      securityParams = self.findSecurityParams(initDoc)
      #These are the parameters that are used by the dot Net reuqest handler to insure someone isn't
      #trying to inject crap. The one thing that changes is the DropDownList1, this is the parameter
      #where the station of interest is set.
      #2014-07-11 DWR
      #Check we have valid object before attempting to use.
      if securityParams is not None:
        params = {'DropDownList1' : '',
          '__EVENTARGUMENT' : '',
          '__EVENTTARGET' :  'DropDownList1',
          '__LASTFOCUS' : '',
          '__EVENTVALIDATION' : securityParams["__EVENTVALIDATION"],
          '__VIEWSTATE' : securityParams["__VIEWSTATE"]
        }
        #Without the trailing '/', the server rejects the request.
        url = self.baseUrl + "/"
        for station in stationNfoList['features']:
          try:
            stationName = station['properties']['station']
            params['DropDownList1'] = stationName
            #create the url and the request
            if(self.logger):
              self.logger.debug("Requesting station: %s Base url: %s" % (stationName, url))
            stationReq = requests.post(url, data=params, headers=headers)
          except Exception,e:
            if(self.logger):
              self.logger.exception(e)
          else:
            try:
              if(stationReq.status_code == 200):
                #if(self.logger):
                #  self.logger.debug("Page rcvd: %s" % (stationReq.text))
                parseResult = docExtract(self.pageDataDict, stationReq.text)
                #Loop through and fixup the date to be ISO centric.
                #for resultNfo in parseResult['results']:
                #  isoDate = datetime.datetime.strptime(resultNfo['date'], "%Y-%m-%d")
                #  resultNfo['date'] = isoDate.strftime("%Y-%m-%d")
                if(parseResult):
                  results[stationName] = parseResult

                if(self.logger):
                  self.logger.debug(results[stationName])
              else:
                if(self.logger):
                  self.logger.error("Status Code: %d received, unable to process station data." %(stationReq.status_code))
            except Exception,e:
              if(self.logger):
                self.logger.exception(e)
    else:
      if(self.logger):
        self.logger.error("Error: %s when requesting page: %s" % (req.reason, self.baseUrl))
    return(results)
  """
  """
  Function: __scrapeResults
  Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
    station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
    resultsData - A dictionary keyed on the station name. Contains an array of date,value entries for the sampling results available.
    jsonOutputFilepath - A String that is the path to store the individual station geoJSON files as well as the comprehensive file.
  Return:
    None
  """
  def __outputGeoJson(self, stationNfoList, resultsData, jsonOutputFilepath):
    if(self.logger):
      self.logger.info("Outputting JSON file.")    
    features = []
    missing_stations = []
    for stationData in stationNfoList['features']:
      stationName = stationData['properties']['station']
      if self.logger:
        self.logger.debug("Station: %s" % (stationName))
      properties = {}
      properties['station'] = stationData['properties']['station']
      properties['desc'] = stationData['properties']['desc']
      properties['locale'] = stationData['properties']['locale']
      properties['epaid'] = stationData['properties']['epaid']
      properties['beach'] = stationData['properties']['beach']
      properties['len'] = stationData['properties']['len']
      properties['sign'] = stationData['properties']['sign']
      if stationName in resultsData:
        properties['test'] = {'beachadvisories' : resultsData[stationName]['results']}
        if(len(resultsData[stationName]['results']) == 0):
          properties['message'] = "Bacteria sample data currently unavailable."
      else:
        properties['test'] = {'beachadvisories' : []}
        properties['message'] = "Bacteria sample data currently unavailable."
        #2015-03-16 DWR
        #Log out the missing stations. SInce an email is sent on any errors, we only
        #want to send one email if we have multiple misses.
        missing_stations.append(stationName)
      geometry = stationData['geometry']
      feature = geojson.Feature(id=stationName, geometry=geometry, properties=properties)
      features.append(feature)

      try:
        fullPath = "%s/%s.json" % (jsonOutputFilepath, stationName)
        jsonFile = open(fullPath, "w") 
        if(self.logger):
          self.logger.info("Opened JSON file: %s" % (fullPath))
        jsonFile.write(geojson.dumps(feature, separators=(',', ':')))
        #jsonFile.write(geojson.dumps(feature, indent=4 * ' '))
        if(self.logger):
          self.logger.info("JSON results written to file.")
        jsonFile.close()
      except IOError,e:
        if(self.logger):
          self.logger.exception(e)
          
      #After the individual station file is written, we reset the properties['test']['beachadvisories'] to contain
      #only the latest sample data.
      if stationName in resultsData:
        if(len(resultsData[stationName]['results'])):
          properties['test']['beachadvisories'] = resultsData[stationName]['results'][-1]
    #2015-03-16 DWR
    #Log out the missing stations. SInce an email is sent on any errors, we only
    #want to send one email if we have multiple misses.
    if len(missing_stations):
      if self.logger:
        self.logger.error("Stations: %s not in results data." % (" ,".join(missing_stations)))

    collection = geojson.FeatureCollection(features=features)
    try:
      fullPath = "%s/beachAdvisoryResults.json" % (jsonOutputFilepath)
      jsonFile = open(fullPath, "w") 
      if(self.logger):
        self.logger.info("Opened JSON file: %s" % (fullPath))
      jsonFile.write(geojson.dumps(collection, separators=(',', ':')))
      if(self.logger):
        self.logger.info("JSON results written to file.")
      jsonFile.close()
    except IOError,e:
      if(self.logger):
        self.logger.exception(e)
def main():      
    
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportStationsFile", dest="importStations",
                    help="Stations file to import." )
  parser.add_option("-t", "--ImportStationsTestResultsFile", dest="importStationsTestResultsFile",
                    help="Stations file to import." )
  (options, args) = parser.parse_args()
  
  if(options.configFile is None):
    parser.print_help()
    sys.exit(-1)
    
  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.configFile)

    logger = None
    logConfFile = configFile.get('logging', 'scraperConfigFile')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger("dhec_beach_advisory_app")
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    print("No log configuration file given, logging disabled.")
  except Exception,e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
      
  try:  
    #Base URL to the page that house an individual stations results. 
    baseUrl = configFile.get('websettings', 'baseAdvisoryPageUrl')
        
    #Database file.
    #dhecDBName = configFile.get('dhecDatabaseSettings', 'sqliteDB')
    
    #output Filename for the JSON data.
    jsonFilepath = configFile.get('output', 'outputDirectory')
    
    #Filepath to the geoJSON file that contains the station data for all the stations.
    stationGeoJsonFile = configFile.get('stationData', 'stationGeoJsonFile')
    
    #The past WQ results.
    stationWQHistoryFile = configFile.get('stationData', 'stationWQHistoryFile')
  
  except ConfigParser.Error, e:
    if(logger):
      logger.exception(e)
  
  else:
    """
    #Connect to the DHEC sqlite database. We query the monitoring station platforms so we have a list
    #to then use to query the website with.
    dbObj = dhecDB(dhecDBName,"dhec_beach_advisory_app")
    platformWhere = "WHERE platform_handle LIKE '%.%.monitorstation'"
    monitorStationsCursor = dbObj.getPlatforms(platformWhere)
    
    if(monitorStationsCursor):
      stationList = []
      for row in monitorStationsCursor:
        nfo = {}
        nfo['short_name'] = row['short_name']
        nfo['fixed_latitude'] = row['fixed_latitude'] 
        nfo['fixed_longitude'] = row['fixed_longitude'] 
        nfo['description'] = row['description']
        stationList.append(nfo)
        
      monitorStationsCursor.close()
    else:
      if(logger):
        logger.error("Unable to retrieve the monitoring stations from the database, cannot continue.")
        sys.exit(-1)
        
    dbObj.DB.close()
    """  
    try:      
      advisoryObj = waterQualityAdvisory(baseUrl, True)
      if(options.importStationsTestResultsFile):
        advisoryObj.createHistoricalJSON(options.importStationsTestResultsFile, "/Users/danramage/tmp/dhec/monitorstations/historicalWQ.json")
      if(options.importStations):
        advisoryObj.createStationGeoJSON(options.importStations, stationGeoJsonFile)      
      else:
        stationDataFile = open(stationGeoJsonFile, "r")
        stationList = geojson.load(stationDataFile)
        stationDataFile.close()
        #See if we have a historical WQ file, if so let's use that as well.
        historyWQFile = open(stationWQHistoryFile, "r")
        historyWQ = geojson.load(historyWQFile)
        
        advisoryObj.processData(stationList, jsonFilepath, historyWQ)
    except IOError,e:
      if(logger):
        logger.exception(e)
    except Exception,e:
      if(logger):
        logger.exception(e)
    if(logger):
      logger.info("Closing logfile.") 
if __name__ == "__main__":
  main()      
      
