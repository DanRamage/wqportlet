import sys
import optparse
import logging
import logging.config
import time
import datetime
from pytz import timezone
from suds import WebFault
from datetime import tzinfo
import math
from dhecDB import dhecDB
from pprint import pformat
from xeniatools.NOAATideData import noaaTideData
from xeniatools.xenia import dbXenia,dbTypes
from xeniatools.xmlConfigFile import xmlConfigFile


class wqDataError(Exception):
  def __init__(self, value):
      self.value = value
  def __str__(self):
      return repr(self.value)


class predictionLevels(object):
  NO_TEST = -1
  LOW = 1
  MEDIUM = 2
  HIGH = 3
  def __init__(self, value):
    self.value = value
  def __str__(self):    
    if(self.value == self.LOW):
      return "LOW"
    elif(self.value == self.MEDIUM):
      return "MEDIUM"
    elif(self.value == self.HIGH):
      return "HIGH"
    else:
      return "NO TEST"
""" 
Class wqTest
Purpose: This is the base class for the actually water quality prediction process.
 Each watershed area has its own MLR and CART tests, so this base class doesn't implement
 anything other than stub functions for them.
"""
class wqEquations(object):
  def __init__(self, station):
    self.station = station  #The station that this object represents.
    self.log10MLRResult = -1 #The natural log result from the mlr/regression formula.
    self.mlrResult = -1 #The exp(log10MLRResult) result. 
    self.mlrPrediction = predictionLevels.NO_TEST  #The prediction for the regression formula.
    self.cartPrediction = predictionLevels.NO_TEST #The prediction for the cart model.
    self.ensemblePrediction = predictionLevels.NO_TEST #The overall prediction result.
    self.data = {} #Data used for the tests.

  """
  Function: runTests
  Purpose: Runs the suite of tests, current a regression formula and CART model, then tabulates
    the overall prediction. 
  Parameters:
    regressionFormula is the formula to use to for the regression calculation. The observation data
      is in a print type format, %(radar_precipitation_24)f. The data is applied from the regressionDataDict
      and then evaluated. 
    regressionDataDict - A data dictionary keyed on the variable names in the formula. String subsitution
      is done then the formula is evaled.
    cartModel is the formula to use to for the Cart prediction. The observation data
      is in a print type format, %(radar_precipitation_24)f. The data is applied from the cartDataDict
      and then evaluated. 
    cartDataDict - A data dictionary keyed on the variable names in the CART tree. String subsitution
      is done then the formula is evaled.
  Return:
    A predictionLevels value.
  """    
  def runTests(self, regressionFormula, regressionDataDict, cartModel, cartDataDict):
      self.data = regressionDataDict.copy()
      self.mlrTest(regressionFormula, regressionDataDict)
      self.cartTest(cartModel, cartDataDict)
      return(self.overallPrediction())

  """
  Function: overallPrediction
  Purpose: From the models used, averages their predicition values to come up with the overall value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """      
  def overallPrediction(self):
    result = predictionLevels.NO_TEST
    if(self.mlrPrediction != predictionLevels.NO_TEST and self.cartPrediction != predictionLevels.NO_TEST):
      self.ensemblePrediction = round(((self.mlrPrediction + self.cartPrediction) / 2.0))
    return(self.ensemblePrediction)
  
  """
  Function: mlrCategorize
  Purpose: For the regression formula, this catergorizes the value. 
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def mlrCategorize(self):
    self.mlrPrediction = predictionLevels.NO_TEST
    if(self.mlrResult != None):
      if(self.mlrResult < 104.0):
        self.mlrPrediction = predictionLevels.LOW
      elif(self.mlrResult >= 500.0):
        self.mlrPrediction = predictionLevels.HIGH
      else:      
        self.mlrPrediction = predictionLevels.MEDIUM
    return(self.mlrPrediction)
  
  """
  Function: mlrTest
  Purpose; This is the base function that child objects will override to put in the specific regression formula
    for that area.
  Parameters:
    data - A data dictionary keyed on the variable names in the formula. String subsitution
      is done then the formula is evaled.
  Return:
    The prediction value, LOW, MEDIUM or HIGH.
  """
  def mlrTest(self, regressionFormula, data):
    formula = regressionFormula % (data)
    self.log10MLRResult = eval(formula)
    self.mlrResult = math.pow(10,self.log10MLRResult)            
    return(self.mlrCategorize())

  """
  Function: cartTest
  Purpose: performs the CART decision tree calculations. 
    data - is a dictionary keyed by the various data names used defined in the tree nodes. We use
      variable substitution to build the tree nodes, then call eval to evaluate it.
  Return:
    The prediction value, LOW, MEDIUM or HIGH.
  """
  
  def cartTest(self, cartModel, data):
    decisionTree = cartModel % (data)
    exec decisionTree
    self.cartPrediction = cartPrediction                         
    return(self.cartPrediction)

"""
Class wqDataAccess
Purpose: This is the base class for retrieving the data for the tests. This class interfaces
  with the databases that house the observation and NEXRAD data.

"""
class wqDataAccess(object):
  NO_DATA = -9999.0
  def __init__(self, configFile, obsDb, nexradDb, regionName="", logger=None):
    self.obsDb = obsDb
    self.nexradDb = nexradDb
    self.logger = logger
    self.configFile = configFile
    self.regionName = regionName
    self.results = {}
  
  """
  Function: getAverageForObs\
  Purpose: For the given observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    obsName string representing the observation we are calcing the average for
    uom the units of measurement the observation is stored in
    platformHandle the platform the observation was made on
    startDate the date/time to start the average
    endDate the date/time to stop the average
  """
  def getAverageForObs(self, obsName, uom, platformHandle, startDate, endDate):
    avg = -9999
    
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    sensorID = self.obsDb.dbConnection.sensorExists(obsName, uom, platformHandle)
    if(sensorID != None and sensorID != -1):
      #Get calc the dateOffset from current time - lastNHours we want to query for.
      if(self.obsDb.dbConnection.dbType == dbTypes.SQLite):
        sql = "SELECT AVG(m_value) as m_value_avg  FROM multi_obs\
               WHERE sensor_id = %d AND\
               (m_date >= '%s' AND \
               m_date < '%s') AND \
               sensor_id = %d"\
              %(beginDate, endDate, sensorID)
      else:
        sql = "SELECT AVG(m_value) as m_value_avg  FROM multi_obs\
               WHERE \
               (m_date >= '%s' AND \
               m_date < '%s') AND \
               sensor_id = %d"\
              %(beginDate, endDate, sensorID)
         
      dbCursor = self.obsDb.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row['m_value_avg'] != None):         
          avg = float(row['m_value_avg'])
        return(avg)
    else:
      self.logger.error("No sensor ID found for observation: %s(%s) on platform: %s" %(obsName, uom, platformHandle))
    return(None)

  """
  Function: getData
  Purpose: This is the base function to retrieve the specific data for a given region.
  Parameters:
    startDate - a datetime object with the date to start the data query
    endDate - a datetime object with the date to end the data query
  Return:
    A dictionary keyed on the with the specific parameters used in the regression and cart models.
  """
  def getData(self, beginDate, endDate):
    return(None)
    
  def processData(self, beginDate, endDate):
    self.logMsg(logging.INFO,
                "Processing stations for region: %s BeginDate: %s EndDate: %s"\
                %(self.regionName, beginDate.strftime("%Y-%m-%d %H:%M:%S"),endDate.strftime("%Y-%m-%d %H:%M:%S")))
    #Get the data used for this area's models
    data = {}
    try:
      data = self.getData(beginDate, endDate)
    except wqDataError,e:
      if(self.logger != None):
        self.logger.exception(e)
        #sys.exit(-1)
      return(False)
    except Exception, e:
      if(self.logger != None):
        self.logger.exception(e)
        #sys.exit(-1)
      return(False)
    
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/intercept" % (self.regionName)
    intercept = self.configFile.getEntry(tag)
    if(intercept != None):
      data['intercept'] = float(intercept)
      self.logMsg(logging.DEBUG, "%s intercept value %s." %(self.regionName, intercept))
    else:
      self.logMsg(logging.ERROR, "Intercept value not available for %s, cannot continue." %(self.regionName))
      return(False)
    
    #Get the regression equation
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/regressionFormula" % (self.regionName)
    regressionFormula = self.configFile.getEntry(tag)
    regressionFormula = regressionFormula.lstrip()
    regressionFormula = regressionFormula.rstrip()
    if(regressionFormula == None):
      self.logMsg(logging.ERROR, "No regression formula found for %s, cannot continue." %(self.regionName))
      return(False)
    
    #Get the CART model
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/cartModel" % (self.regionName)
    cartModel = self.configFile.getEntry(tag)
    cartModel = cartModel.lstrip()
    cartModel = cartModel.rstrip()
    if(cartModel == None):
      self.logMsg(logging.ERROR, "No cart Model found for %s, cannot continue." %(self.regionName))
      return(False)
    
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/stations" % (self.regionName)
    stationList = self.configFile.getListHead(tag)
    for station in self.configFile.getNextInList(stationList):
      stationName = station.get('id')   
      if(stationName == None):
        self.logMsg(logging.ERROR, "No station name found for %s, skipping to next station." %(self.regionName))
        continue
      coefficient = self.configFile.getEntry('coefficient', station)
      if(coefficient != None):
        data['station_coefficient'] = float(coefficient)
        self.logMsg(logging.DEBUG, "%s %s coefficient value %s." %(self.regionName, stationName, coefficient))
      else:
        self.logMsg(logging.ERROR, "coefficient value not available for %s station: %s cannot continue." %(self.regionName, stationName))
        continue
      self.runTests(stationName, regressionFormula, cartModel, data)  
    return(True)
    
  def runTests(self, stationName, regressionFormula, cartModel, data):
    wqTest = wqEquations(stationName)
    #Run through the data, if any of the values are -9999, we didn't get a value so we
    #cannot run the tests. Let's log that out.
    dataValid = True
    for dataKey in data:
      if(data[dataKey] == -9999):
        self.logMsg(logging.ERROR, "%s has a value of -9999, we cannot run the tests." %(dataKey))
        dataValid = False
    
    if(dataValid):
      try:
        wqTest.runTests(regressionFormula, data, cartModel, data)
        self.logMsg(logging.DEBUG, "Regression value: %4.2f prediction: %d Cart Prediction: %d" %(wqTest.mlrResult, wqTest.mlrPrediction, wqTest.cartPrediction))
      except Exception, e:
        if(self.logger != None):
          self.logger.exception(e)
          sys.exit(-1)
    #Figure out a better way to handle this. Since the data is not valid we aren't calling runTests. In runTests
    #we make a copy of the data used by the object, so when we are writing out our emails/other info we can see
    #what the object was using for data.  
    else:
      wqTest.data = data.copy()
    #Store the equation object for each station so we can run through them to send out results.
    self.results[stationName] = wqTest   

    return(dataValid)

  
  def logMsg(self, msgLevel, msg):
    if(self.logger != None):
      self.logger.log(msgLevel, msg)
    return
    

class wqDataNMB2(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="NMB2", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_preceeding_dry_day_cnt'] = dryCnt
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
        #self.logMsg(logging.ERROR, "Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
        #return(None)
    data['sun2_salinity'] = sun2_salinity
    
    #Get the water temperature from SUN2
    data['sun2_water_temp'] = self.NO_DATA    
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp == None):
      self.logger.error("Error retrieving water_temperature from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
      #self.logMsg(logging.ERROR, "Error retrieving water_temperature from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
      #return(None)
    data['sun2_water_temp'] = sun2_water_temp
    
    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level

    #Get the tide data
    try:
      data['range'] = self.NO_DATA;
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      range = tideData['HH']['value'] - tideData['LL']['value']   
      data['range'] =  range;
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)
 
class wqDataNMB3(wqDataAccess):
  
  def __init__(self, configFile, obsDb, nexradDb, regionName="NMB3", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_48'] = radar_rain_summary_48
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity
       
    #Get the tide data
    try:
      data['range'] = self.NO_DATA;
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      range = tideData['HH']['value'] - tideData['LL']['value']   
      data['range'] =  range;
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataMB1(wqDataAccess):
  
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB1", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, regressionFormula, cartModel, data):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, regressionFormula, cartModel, data)
    
  def getData(self, beginDate, endDate):
    data = {}
       
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 == None):
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_48'] = radar_rain_summary_48

    data['radar_rain_summary_144'] = self.NO_DATA
    radar_rain_summary_144 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 144)
    if(radar_rain_summary_144 == None):
      self.logger.error("Error retrieving radar_rain_summary_144. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_144'] = radar_rain_summary_144
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level

    try:
      data['lowFt'] = self.NO_DATA
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      data['lowFt'] = tideData['LL']['value']   
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    
    
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataMB2(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB2", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
          
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_24'] = self.NO_DATA
    radar_rain_summary_24 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 24)
    if(radar_rain_summary_24 == None):
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_24'] = radar_rain_summary_24

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_preceeding_dry_day_cnt'] = dryCnt
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level

    data['nos8661070_wind_dir'] = self.NO_DATA
    nos8661070_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_wind_dir == None):
      self.logger.error("Error retrieving nos8661070_wind_dir from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    if(nos8661070_wind_dir != self.NO_DATA):        
      nos8661070_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(nos8661070_wind_dir)
      data['nos8661070_wind_dir'] = nos8661070_compass_dir
    
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataMB3(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB3", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, regressionFormula, cartModel, data):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, regressionFormula, cartModel, data)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_24'] = self.NO_DATA
    radar_rain_summary_24 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 24)
    if(radar_rain_summary_24 == None):
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_24'] = radar_rain_summary_24

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 == None):
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_48'] = radar_rain_summary_48

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_preceeding_dry_day_cnt'] = dryCnt
        
    #Get the salinity from SUN2
    data['sun2_wind_speed'] = self.NO_DATA
    sun2_wind_speed = self.getAverageForObs('wind_speed', 'm_s-1', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_speed == None):
      self.logger.error("Error retrieving sun2_wind_speed from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    #Convert to knots.
    if(sun2_wind_speed != self.NO_DATA):
      sun2_wind_speed *= 1.9438444924406    
      data['sun2_wind_speed'] = sun2_wind_speed

    #Get the salinity from SUN2
    data['sun2_wind_dir'] = self.NO_DATA
    sun2_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_dir == None):
      self.logger.error("Error retrieving sun2_wind_dir from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    if(sun2_wind_dir != self.NO_DATA):
      sun2_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(sun2_wind_dir)
      data['sun2_wind_dir'] = sun2_compass_dir

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level
           
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataMB4(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB4", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, regressionFormula, cartModel, data):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, regressionFormula, cartModel, data)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 == None):
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_48'] = radar_rain_summary_48

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_preceeding_dry_day_cnt'] = dryCnt
        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp == None):
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_water_temp'] = sun2_water_temp

    data['sun2_wind_dir'] = self.NO_DATA
    sun2_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_dir == None):
      self.logger.error("Error retrieving sun2_wind_dir from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    if(sun2_wind_dir != self.NO_DATA):
      sun2_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(sun2_wind_dir)
      data['sun2_wind_dir'] = sun2_compass_dir

    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level

    
    #Get the tide data
    try:
      data['range'] = self.NO_DATA
      data['highFt'] = self.NO_DATA
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      range = tideData['HH']['value'] - tideData['LL']['value']   
      data['range'] =  range
      data['highFt'] = tideData['HH']['value']
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
           
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataSS(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="Surfside", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, regressionFormula, cartModel, data):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, regressionFormula, cartModel, data)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_24'] = self.NO_DATA
    radar_rain_summary_24 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 24)
    if(radar_rain_summary_24 == None):
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_24'] = radar_rain_summary_24

    data['radar_rainfall_intensity_24'] = self.NO_DATA
    radar_rainfall_intensity_24 = self.nexradDb.calcRadarRainfallIntensity(self.regionName.lower(), startDate, 60)
    if(radar_rainfall_intensity_24 == None):
      self.logger.error("Error retrieving radar_rainfall_intensity_24. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rainfall_intensity_24'] = radar_rainfall_intensity_24
        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp == None):
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_water_temp'] = sun2_water_temp

    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity

    data['nos8661070_water_temp'] = self.NO_DATA
    nos8661070_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_temp == None):
      self.logger.error("Error retrieving nos8661070_water_temp from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_temp'] = nos8661070_water_temp

    
    #Get the tide data
    try:
      data['range'] = self.NO_DATA
      data['lowFt'] = self.NO_DATA
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      range = tideData['HH']['value'] - tideData['LL']['value']   
      data['range'] =  range
      data['lowFt'] = tideData['LL']['value']
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
           
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

class wqDataGC(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="Gardcty", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, regressionFormula, cartModel, data):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, regressionFormula, cartModel, data)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 == None):
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_rain_summary_48'] = radar_rain_summary_48

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt == None):
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
    data['radar_preceeding_dry_day_cnt'] = dryCnt

        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp == None):
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_water_temp'] = sun2_water_temp

    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity == None):
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    data['sun2_salinity'] = sun2_salinity

    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level == None):
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    data['nos8661070_water_level'] = nos8661070_water_level

    #Get the tide data
    try:
      data['range'] = self.NO_DATA
      data['highFt'] = self.NO_DATA
      tide = noaaTideData(logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD
      #Times passed in as UTC, we want local tide time, so we convert.
      tideBegin = beginDate.astimezone(timezone('US/Eastern') )
      tideEnd = beginDate.astimezone(timezone('US/Eastern') )
      tideData = tide.calcTideRange(beginDate = tideBegin.strftime('%Y%m%d'),
                         endDate = tideEnd.strftime('%Y%m%d'),
                         station='8661070',
                         datum='MLLW',
                         units='feet',
                         timezone='Local Time',
                         smoothData=False)
      range = tideData['HH']['value'] - tideData['LL']['value']   
      data['range'] =  range
      data['highFt'] = tideData['HH']['value']
    except WebFault, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      self.logger.error("Error retrieving tide data. Error: %s" %(e))
           
    self.logMsg(logging.DEBUG, pformat(data))
    
    return(data)

"""
Class: testSuite
Purpose: This class runs through the watersheds in the configuration file and runs the prediction tests.
"""
class testSuite(object):
  def __init__(self, xmlConfigObj, logger=None):
    #self.objectList = {'wqDataNMB2' : wqDataNMB2, 'wqDataNMB3' : wqDataNMB3, 'wqDataMB1' : wqDataMB1, 'wqDataMB2' : wqDataMB2, 'wqDataMB3' : wqDataMB2} 
    self.configFile = xmlConfigObj      # The xmlConfigFile object.
    self.logger = logger                # The logging object.
    self.testObjects = []               # A list of wqDataAccess objects. These perform the tests for each watershed area.
    self.varMapping = {}                # If we are outputting the data in the email and KML files, this is a mapping from variable name to 
                                        # a more explanatory display name.
  
  def logMsg(self, msgLevel, msg):
    if(self.logger != None):
      self.logger.log(msgLevel, msg)
    return
  
  """
  Function: runTests
  Purpose: Creates the database objects, then loops through the watersheds creating the processing objects and
    runs the tests.
  Parameters:
    beginDate - datetime object that specifies the beginning date/time for the processing run. Should be
      less than the endDate.
    endDate - datetime object that specifies the end date/time for the processing run. Should be greater than
      the beginDate.
    
  """
  def runTests(self, beginDate, endDate):
    testRunDate = datetime.datetime.now(timezone('US/Eastern'))

    obsDBSettings = self.configFile.getDatabaseSettingsEx('//environment/stationTesting/database/obsDatabase/')
    obsDB = dbXenia()
    #def connect(self, dbFilePath=None, user=None, passwd=None, host=None, dbName=None ):
    if(obsDB.connect(None, obsDBSettings['dbUser'], obsDBSettings['dbPwd'], obsDBSettings['dbHost'], obsDBSettings['dbName']) != True):
      self.logMsg(logging.ERROR, "Unable to connect to observation database: %s @ %s" %(obsDBSettings['dbName'], obsDBSettings['dbHost']))
      sys.exit(-1)
    else:
      self.logMsg(logging.INFO, "Connected to %s @ %s" %(obsDBSettings['dbName'], obsDBSettings['dbHost']))

    nexradDBSettings = self.configFile.getDatabaseSettingsEx('//environment/stationTesting/database/nexradDatabase/')
    nexradDB = dhecDB(nexradDBSettings['dbName'],"dhec_testing_logger")
        
    tag = "//environment/stationTesting/watersheds"
    regionList = self.configFile.getListHead(tag)
    for watershed in self.configFile.getNextInList(regionList):
      watershedName = watershed.get('id')
      testObjName = self.configFile.getEntry('testObject', watershed)
      self.logMsg(logging.INFO, "Region: %s processing object: %s" %(watershedName, testObjName))
      #Determine if the test object provided in the config file exists in the dictionary of available processing
      #objects.
      if(testObjName in globals()):
        #Retrieve the processing class      
        testObj = globals()[testObjName]
        #Now we instantiate the object.
        wqObj = testObj(configFile=configFile, obsDb=obsDB, nexradDb=nexradDB, logger=logger)
        wqObj.processData(beginDate, endDate)
        self.testObjects.append(wqObj)
      else:
        if(self.logger != None):
          self.logger.error("Region: %s using invalid testObject: %s, cannot process." %(watershedName, testObjName))
    
    tag = "//environment/stationTesting/results/outputDataUsed"
    outputData = self.configFile.getEntry(tag)
    if(outputData == None):
      outputData = 0
    
    #If we are going to put the data out in the email and KML file, build our mapping.
    if(outputData):  
      tag = "//environment/stationTesting/results/inputVariableNames"
      variableList = self.configFile.getListHead(tag)
      if(variableList != None):
        for variable in self.configFile.getNextInList(variableList):
          entry = {}
          variableName = variable.get('id')
          displayName = self.configFile.getEntry('displayName', variable)
          if(displayName != None):
            entry['displayName'] = displayName
          else:
             entry['displayName'] = variableName
          desc = self.configFile.getEntry('description', variable)
          if(desc != None):
            entry['description'] = desc
          else:
            entry['description'] = variableName            
          self.varMapping[variableName] = entry

    
    testBeginDate = beginDate.astimezone(timezone('US/Eastern'))
    testEndDate = endDate.astimezone(timezone('US/Eastern'))
    self.createMapOutput(outputData, nexradDB, testBeginDate, testEndDate, testRunDate)
    self.sendResultsEmail(outputData, testBeginDate, testEndDate, testRunDate)
    obsDB.dbConnection.DB.close()
    nexradDB.DB.close()
    
  def sendResults(self):
    self.createMapOutput()
    self.sendResultsEmail()
    return    

  """
  Function: writeKMLFile
  Purpose: Creates a KML file with the latest hour, 24, and 48 hour summaries.
  """
  def createMapOutput(self, outputData, nexradDB, beginDate, endDate, testRunDate):
    from pykml import kml
    tag = "//environment/stationTesting/results/kmlFilePath"
    kmlFilepath = self.configFile.getEntry(tag)

    if(kmlFilepath != None):
      
      pmTableBegin = """<table>"""
      pmTableEnd = """</table>"""
      pmTemplate = """<tr><td>%(region)s</td></tr>
        <tr><td>Station:</td><td>%(station)s</td><td>%(description)s</td></tr>
        <tr><td>Prediction for Date:</td><td>%(endDate)s</td></tr>        
        <tr><td>Date Tests Run:</td><td>%(testRunDate)s</td></tr>        
        <tr><td>Overall Prediction:</td><td>%(ensemblePrediction)s</td></tr>
        <tr><td>MLR:</td><td>%(mlrPrediction)s</td><td>log10(etcoc): %(log10MLRResult)4.2f etcoc %(mlrResult)4.2f</td></tr>
        <tr><td>Cart:</td><td>%(cartPrediction)s</td></tr>"""
        
      try:        
        self.logMsg(logging.INFO, "Creating DHEC ETCOC Prediction KML file: %s" % (kmlFilepath))
        etcocKML = kml.KML()
        doc = etcocKML.createDocument("DHEC ETCOC Predictions")
        doc.appendChild(etcocKML.createStyle(
            id="no_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://rcoos.org/resources/images/default/no_light16x16.png"))
        ))
        doc.appendChild(etcocKML.createStyle(
            id="low_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://rcoos.org/resources/images/default/green_light16x16.png"))
            
        ))
        doc.appendChild(etcocKML.createStyle(
            id="med_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://rcoos.org/resources/images/default/yellow_light16x16.png"))            
        ))
        doc.appendChild(etcocKML.createStyle(
            id="hi_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://rcoos.org/resources/images/default/red_light16x16.png"))                        
        ))
        for wqObj in self.testObjects:      
          
          #The stationKeys are the names of the stations, let's sort them so they'll be in an increasing
          #alpha numeric order.          
          stationKeys = wqObj.results.keys()
          stationKeys.sort()
          
          for station in stationKeys:
            desc = ""
            #Get the geo location information for the station
            platformHandle = "dhec.%s.monitorstation" % (station)
            dbCursor = nexradDB.getPlatformInfo(platformHandle)
            latitude = 0.0
            longitude = 0.0
            stationDesc = ""
            if(dbCursor != None):
              row = dbCursor.fetchone()
              if(row != None):
                latitude = row['fixed_latitude'] 
                longitude = row['fixed_longitude'] 
                stationDesc = row['description']
            else:
              self.logMsg(logging.ERROR, "ERROR: Unable to get platform: %s data. %s" % (platformHandle, nexradDB.getErrorInfo()))
            desc += pmTableBegin
            tstObj = wqObj.results[station]
            tmpltDict = { 'region' : wqObj.regionName,
              'station' : station,
              'ensemblePrediction' : predictionLevels(tstObj.ensemblePrediction).__str__(),
              'mlrPrediction' : predictionLevels(tstObj.mlrPrediction).__str__(),
              'log10MLRResult' : tstObj.log10MLRResult,
              'mlrResult' : tstObj.mlrResult,
              'cartPrediction' : predictionLevels(tstObj.cartPrediction).__str__(),
              'testRunDate' : testRunDate.strftime('%Y-%m-%d %H:%M'),
              'endDate' : endDate.strftime('%Y-%m-%d %H:%M'),
              'description' : stationDesc}
            
            desc += pmTemplate % (tmpltDict)
            tmpltDict.clear()
            desc += pmTableEnd        
            predictionStyle = "#no_prediction"
            if(tstObj.ensemblePrediction == predictionLevels.LOW):
              predictionStyle = "#low_prediction"
            elif(tstObj.ensemblePrediction == predictionLevels.MEDIUM):
              predictionStyle = "#med_prediction"
            elif(tstObj.ensemblePrediction == predictionLevels.HIGH):
              predictionStyle = "#hi_prediction"              
            pm = etcocKML.createPlacemark(station, latitude, longitude, desc, predictionStyle)
            #Build a custom xml section to hold the data
            dataKML = etcocKML.xml.createElement("ExtendedData")
            dataKmlTag = etcocKML.xml.createElement('Data')
            dataKmlTag.setAttribute("name", "station")
            dataValueTag = etcocKML.xml.createElement('value')
            dataValueTag.appendChild(etcocKML.xml.createTextNode(station))
            dataKmlTag.appendChild(dataValueTag)
            dataKML.appendChild(dataKmlTag)
            
            if(outputData):
              #Get the region specific variables, we want to skip over the station name and coefficient.
              dataKmlTag = etcocKML.xml.createElement('Data')
              dataKmlTag.setAttribute("name", "data")
              dataValueTag = etcocKML.xml.createElement('value')
              dataUsed = "<table>"
              for key in tstObj.data:
                if(key != "station"):
                  varInfo = self.varMapping[key]
                  val = tstObj.data[key]
                  if(val == -9999):
                    val = "Data unavailable."                  
                  if(type(val) != str):
                    dataUsed += "<tr><td>%s</td><td>%f</td></tr>" % (varInfo['displayName'], val)
                  else:
                    dataUsed += "<tr><td>%s</td><td>%s</td></tr>" % (varInfo['displayName'], val)
              dataUsed += "</table>"
              dataUsed = dataUsed.lstrip()
              dataUsed = dataUsed.rstrip()
              dataValueTag.appendChild(etcocKML.xml.createTextNode(dataUsed))
              dataKmlTag.appendChild(dataValueTag)
              dataKML.appendChild(dataKmlTag)
            pm.appendChild(dataKML)
            
            doc.appendChild(pm)
        etcocKML.root.appendChild(doc)  
        kmlFile = open(kmlFilepath, "w")
        kmlFile.writelines(etcocKML.writepretty())
        kmlFile.close()
      except Exception, e:
        if(self.logger != None):
          self.logger.exception(e)      
          sys.exit(-1)
    else:
      self.logMsg(logging.DEBUG, "Cannot write KML file, no filepath provided in config file.")
    
    return
  def sendResultsEmail(self, outputData, beginDate, endDate, testRunDate):
    from xeniatools.utils import smtpClass 
    import string
    tag = "//environment/stationTesting/results/outputDataUsed"
    outputData = self.configFile.getEntry(tag)
    if(outputData == None):
      outputData = 0
    subjectTmpl = "[DHEC] Water Quality Prediction Results - %(endDate)s"
    header =  """Predictions for Date: %(endDate)s
Test Execution Date: %(testRunDate)s
"""

    regionHdr = """--------%s--------
    
"""    
    msgTemplate = """Station: %(station)s
      Overall Prediction: %(ensemblePrediction)s
"""
              #MLR: %(mlrPrediction)s log10(etcoc): %(log10MLRResult)4.2f etcoc %(mlrResult)4.2f
              #Cart: %(cartPrediction)s"""
    if(outputData):
    #  msgTemplate += """
    #          Coefficient: %(station_coefficient)f
    #          
#"""
      dataTemplate = """
Data used for station tests: 
%(data)s
    

"""
    else:
      msgTemplate += """
"""      
      
    try:   
      emailSettings = self.configFile.getEmailSettingsEx('//environment/stationTesting/results/')
      #Loop through the results objects to get the individual station test results.
      body = header % ({'endDate' : endDate.strftime('%Y-%m-%d'), 'testRunDate' : testRunDate.strftime('%Y-%m-%d %H:%m')})
      for wqObj in self.testObjects:
        body += regionHdr % (wqObj.regionName)
        #The stationKeys are the names of the stations, let's sort them so they'll be in an increasing
        #alpha numeric order.          
        stationKeys = wqObj.results.keys()
        stationKeys.sort()
        dataUsed = ""
        for station in stationKeys:
          tstObj = wqObj.results[station]

          #If we want to output the data, and we have not already populate the station non-specific variables.
          if(outputData and len(dataUsed) == 0):
            #Get the region specific variables, we want to skip over the station name and coefficient.
            for key in tstObj.data:
              if(key != "station" and key != "station_coefficient"):
                val = tstObj.data[key]
                varInfo = self.varMapping[key]                
                if(val == -9999):
                  val = "Data unavailable."
                if(type(val) != str):
                  dataUsed += "%s: %f\n" % (varInfo['displayName'], val)
                else:
                  dataUsed += "%s: %s\n" % (varInfo['displayName'], val)
          
          #Build the string substitution dictionary for the message template.
          if('station_coefficient' in tstObj.data):
            stationCo = tstObj.data['station_coefficient']
          else:
            stationCo = 0.0
          tmpltDict = {'station' : station,
            'ensemblePrediction' : predictionLevels(tstObj.ensemblePrediction).__str__(),
            'mlrPrediction' : predictionLevels(tstObj.mlrPrediction).__str__(),
            'log10MLRResult' : tstObj.log10MLRResult,
            'mlrResult' : tstObj.mlrResult,
            'cartPrediction' : predictionLevels(tstObj.cartPrediction).__str__(),
            'station_coefficient' : stationCo}
          body += (msgTemplate % (tmpltDict))
          tmpltDict.clear()
        if(outputData):
          body += (dataTemplate % {'data' : dataUsed})                      
      subject =  subjectTmpl % ({'endDate' : endDate.strftime('%Y-%m-%d')})
      smtp = smtpClass(emailSettings['server'], emailSettings['from'], emailSettings['pwd'])
      smtp.from_addr("%s@%s" % (emailSettings['from'],emailSettings['server']))
      smtp.rcpt_to(emailSettings['toList'])
      smtp.subject(subject)
      smtp.message(body)
      smtp.send()      
      if(self.logger != None):
        self.logger.info("Sending email alerts.")
    except Exception,e:
      if(self.logger != None):
        self.logger.exception(e)                             
    return
  
if __name__ == '__main__':
  try:
    import psyco
    psyco.full()
        
    parser = optparse.OptionParser()
    parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                      help="Configuration file." )
    parser.add_option("-s", "--StartDateTime", dest="startDateTime",
                      help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS." )
    (options, args) = parser.parse_args()
    if( options.xmlConfigFile == None ):
      parser.print_usage()
      parser.print_help()
      sys.exit(-1)
  
    configFile = xmlConfigFile(options.xmlConfigFile)
    
    logFile = configFile.getEntry('//environment/stationTesting/logConfigFile')
    logging.config.fileConfig(logFile)
    logger = logging.getLogger("dhec_testing_logger")
    logger.info("Session started")
    
    if(options.startDateTime != None):
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.  
      eastern = timezone('US/Eastern')  
      est = eastern.localize(datetime.datetime.strptime(options.startDateTime, "%Y-%m-%d %H:%M:%S"))
      est = est - datetime.timedelta(days=1)
      #Convert to UTC
      beginDate = est.astimezone(timezone('UTC'))
      endDate = beginDate + datetime.timedelta(hours=24)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.    
      est = datetime.datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      est = est - datetime.timedelta(days=1)
      #Convert to UTC
      beginDate = est.astimezone(timezone('UTC'))
      endDate = beginDate + datetime.timedelta(hours=24)
    
    
    testingObj = testSuite(configFile, logger)
    testingObj.runTests(beginDate, endDate)
    
    logger.info("Processing run finished.")
    
  except Exception, E:
    import traceback
    print( traceback.print_exc() )
