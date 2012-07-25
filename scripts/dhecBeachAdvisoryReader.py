import sys
#import requests
import logging.config
import optparse
import ConfigParser
import copy
from lxml import etree    
import geojson
import urllib
import urllib2
import socket 
import datetime
from dhecDB import dhecDB

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
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)
    
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
        for line in srcFile:
          #Bump past first 2 lines since they are the header rows.
          if(lineCnt > 1):
            #Verify we have valid coordinates.
            if(len(line['latitude']) and len(line['longitude'])):
              try:              
                geometry = geojson.Point(coordinates=[float(line['longitude']), float(line['latitude'])])
              except ValueError,e:
                if(self.logger):
                  self.logger.error("Line: %d invalid value for either latitude or longitude" % (lineCnt))
                continue
              #Now let's get the rest of the properties.
              else:
                properties = {}
                properties['station'] = line['station']
                properties['desc'] = line['address']
                properties['locale'] = line['city-county']
                properties['epaid'] = line['epabeachid']
                properties['beach'] = line['beachname']
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
  def processData(self, stationNfoList, jsonOutputFilepath):
    if(self.logger):
      self.logger.info("Begin data processing.")
    resultsData = self.__scrapeResults(stationNfoList)
    self.__outputGeoJson(stationNfoList, resultsData, jsonOutputFilepath)
    if(self.logger):
      self.logger.info("Data processing completed.")
    
  """
  Function: __scrapeResults
  Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
    station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
  Parameters:
    stationNfoList - A FeatureCollection object that has geoJSON data for all the stations. 
  Return:
    None
  """
  def __scrapeResults(self, stationNfoList):
    if(self.logger):
      self.logger.info("Scraping web pages.")
    results = {}
    for station in stationNfoList['features']:
      try:
        stationName = station['properties']['station']
        #params = {'station' : station['short_name']}
        params = {'station' : stationName}
        params = urllib.urlencode(params)
        #create the url and the request
        requestUrl = self.baseUrl + '?' + params
        if(self.logger):
          self.logger.debug("Requesting page: %s" % (requestUrl))
        req = urllib2.Request(requestUrl)
        # Open the url
        #Set the timeout so we don't hang on the urlopen calls.
        socket.setdefaulttimeout(30)
        connection = urllib2.urlopen(req)
        pageResults = connection.read()
        
        """
        stationPage = "%s%s" % (self.baseUrl, station['short_name'])
        if(self.logger):
          self.logger.debug("Requesting page: %s" % (stationPage))
        pageResults = requests.get(stationPage)
      except requests.exceptions.RequestException,e:
        if(self.logger):
          self.logger.exception(e)
      """
      except Exception,e:
        if(self.logger):
          self.logger.exception(e)
      else:
        try:
          if(connection.code == 200): 
            #if(self.logger):
            #  self.logger.debug("Page rcvd: %s" % (pageResults.text))
            #parsedResult = etree.HTML(pageResults.text)
            #results = parsedResult.xpath("//table[contains(@id,'GridView1')]")
            parseResult = docExtract(self.pageDataDict, pageResults)
            #Loop through and fixup the date to be ISO centric.
            for resultNfo in parseResult['results']:
              isoDate = datetime.datetime.strptime(resultNfo['date'], "%m/%d/%Y")
              resultNfo['date'] = isoDate.strftime("%Y-%m-%d") 
            if(parseResult):
              results[stationName] = parseResult
              
            if(self.logger):
              self.logger.debug(results[stationName])
          else:
            if(self.logger):
              self.logger.error("Status Code: %d received, unable to process station data." %(connection.code))
        except Exception,e:
          if(self.logger):
            self.logger.exception(e) 
          
    return(results)

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
    for stationData in stationNfoList['features']:
      stationName = stationData['properties']['station']
      properties = {}
      properties['station'] = stationData['properties']['station']
      properties['desc'] = stationData['properties']['desc']
      properties['locale'] = stationData['properties']['locale']
      properties['epaid'] = stationData['properties']['epaid']
      properties['beach'] = stationData['properties']['beach']
      properties['len'] = stationData['properties']['len']
      properties['sign'] = stationData['properties']['sign']      
      properties['test'] = {'beachadvisories' : resultsData[stationName]['results']}
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
      if(len(resultsData[stationName]['results'])):
        properties['test']['beachadvisories'] = resultsData[stationName]['results'][-1]
     
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
      if(options.importStations):
        advisoryObj.createStationGeoJSON(options.importStations, stationGeoJsonFile)
      else:
        stationDataFile = open(stationGeoJsonFile, "r")
        stationList = stationJson = geojson.load(stationDataFile)
        stationDataFile.close()
        advisoryObj.processData(stationList, jsonFilepath)
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
      
