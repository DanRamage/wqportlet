#!/usr/bin/python

_USE_HANDLER = True
if(_USE_HANDLER):
  from mod_python import apache
  from mod_python import util


def handler(req):
#if __name__ == '__main__':
    
  import logging.config
  import ConfigParser
  #from decimal import *
  import geojson   
  import datetime
  
  if(_USE_HANDLER):
    configFilepath = '/home/xeniaprod/config/dhecBeachAdvisoryApp.ini'
    req.log_error('handler')
    #req.add_common_vars()
    params = util.FieldStorage(req)
  else:
    configFilepath = '/Users/danramage/Documents/workspace/WaterQuality/dhecBeachAdvisoryApp-Debug.ini'
    params = {}
    params['station'] = 'WAC-001'
    params['startdate'] = "2012-06-01"
    params['enddate'] = None
    
  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(configFilepath)
    logger = None
    logConfFile = configFile.get('logging', 'handlerConfigFile')

    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger("dhec_water_advisory_handler")
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    print("No log configuration file given, logging disabled.")
  except Exception,e:
    print(e)
  
  try:
    jsonMonitorStationDir = configFile.get('output', 'outputDirectory')
  except ConfigParser.Error, e:
    if(logger):
      logger.exception(e)
  else:
    station = None
    startDate = None
    endDate = None  
    if('station' in params):
      station = params['station']
    if('startdate' in params):
      startDate = params['startdate']
    if('enddate' in params):
      endDate = params['enddate']
      
    feature = None
    try:
      if(logger):
        logger.info("Remote host info: %s" % (req.get_remote_host(apache.REMOTE_NOLOOKUP)))
        logger.info("URL params: %s" % (params))
      
      filepath = "%s/%s.json" % (jsonMonitorStationDir, station)
      jsonDataFile = open(filepath, "r")
      stationJson = geojson.load(jsonDataFile)
      if(logger):
        logger.debug(stationJson)

      resultList = []
      #If the client passed in a startdate parameter, we return only the test dates >= to it.
      if(startDate):        
        startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")            
        advisoryList = stationJson['properties']['test']['beachadvisories']
        for ndx in range(len(advisoryList)):
          tstDate = datetime.datetime.strptime(advisoryList[ndx]['date'], "%Y-%m-%d")
          if(tstDate >= startDate):
            resultList = advisoryList[ndx:]
            break
      else:
        resultList = stationJson['properties']['test']['beachadvisories'][-1]
          
      properties = {}  
      properties['desc'] = stationJson['properties']['desc']
      properties['station'] = stationJson['properties']['station']
      properties['locale'] = stationJson['properties']['locale']
      properties['epaid'] = stationJson['properties']['epaid']
      properties['beach'] = stationJson['properties']['beach']
      properties['len'] = stationJson['properties']['len']
      properties['sign'] = stationJson['properties'] ['sign']     
      properties['test'] = {'beachadvisories' : resultList}
      geometry = stationJson['geometry']      
      if(logger):
        logger.debug(geometry)
      
      
      feature = geojson.Feature(id=station, geometry=geometry, properties=properties)      
    except IOError, e:
      if(logger):
        logger.exception(e)
    except ValueError, e:
      if(logger):
        logger.exception(e)
    except Exception, e:
      if(logger):
        logger.exception(e)
    try:
      if(feature is None):
        feature = geojson.Feature(id=station)

      jsonData = geojson.dumps(feature, separators=(',', ':'))
      if(_USE_HANDLER):
        req.content_type = 'application/json;' 
        req.set_content_length(len(jsonData))
        logger.debug("Json: %s" %(jsonData))
        req.write(jsonData)
        req.status = apache.HTTP_OK
      else:
        print(jsonData)       
  
      logger.info("Closing log file.")
      
    except Exception, e:
      if(logger):
        logger.exception(e)
