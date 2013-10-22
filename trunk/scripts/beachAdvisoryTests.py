"""
Revisions
Date: 2013-10-22
Author: DWR
Function: calcAvgWindSpeedAndDir
Changes: Added function to do a better job of averaging the wind speed and direction. Direction is a special
case in that its values range from 0-360, so trying to do a straight average will result in incorrect results 
if the direction is near the inflection.

Function: wqDataMB2::getData, wqDataMB3::getData, wqDataMB4::getData
Changes: Use the new calcAvgWindSpeedAndDir function.
 
Date: 2013-02-04
Author: DWR
Function: wqDataAccess:getAverageForObs
Changes: Added qc_level condition to make sure we get valid data for platforms where we QC check.

Date: 2011-10-11
Author: DWR
Function: wqEquations.overallPrediction
Changes: If a test has a NO_TEST result, we do not use it in the ensemble prediction, we skip it.
Previously if any test was NO_TEST, we considered the ensemble a NO_TEST.
"""
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
from xeniatools.xenia import dbXenia,dbTypes,qaqcTestFlags
from xeniatools.xmlConfigFile import xmlConfigFile
from xeniatools.stats import vectorMagDir



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
Class: predictionTest
Purpose: This is the base class for our various prediction tests.
"""    
class predictionTest(object):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters: 
    formula - a string with the appropriate string substitution parameters that the runTest function will
      apply the data against.
    name - A string identifier for the test.
  Return:
  """
  def __init__(self, formula, name=None):
    self.formula = formula
    self.predictionLevel = predictionLevels.NO_TEST
    self.name = name
  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
  Parameters: 
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    return(predictionLevels.NO_TEST)
  
  """
  Function: getResults
  Purpose: Returns a dictionary with the computational variables that went into the predictionLevel. For instance, for an
    MLR calculation, there are intermediate results such as the log10 result and the final result.
  Parameters:
    None
  Return: A dictionary.
  """
  def getResults(self):
    results = {'predictionLevel' : self.predictionLevel}
    return(results)

"""
Class: mlrPredictionTest
Purpose: Prediction test for a linear regression formula.  
"""
class mlrPredictionTest(predictionTest):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters: 
  formula - a string with the appropriate string substitution parameters that the runTest function will
    apply the data against.
  lowCategoryLimit - A float that defines the lower limit which categorizes the test result as a LOW probability.
  highCategoryLimit - A float that defines the high limit which categorizes the test result as a HIGH probability.
  Return:
  """
  def __init__(self, formula, name):
    predictionTest.__init__(self, formula, name)
    self.lowCategoryLimit = 104.0
    self.highCategoryLimit = 500.0
    self.mlrResult = None
    self.log10MLRResult = None
  
  """
  Function: setCategoryLimits
  Purpose: To catecorize MLR results, we use a high and low limit.
  Parameters:
    lowLimit - Float representing the value, equal to or below, which is considered a low prediction.
    highLimit  - Float representing the value, greater than,  which is considered a high prediction.
  """
  def setCategoryLimits(self, lowLimit, highLimit):
    self.lowCategoryLimit = lowLimit
    self.highCategoryLimit = highLimit
  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
    Prediction is a log10 formula.
  Parameters: 
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    formula = self.formula % (data)
    self.log10MLRResult = eval(formula)
    self.mlrResult = math.pow(10,self.log10MLRResult)            
    self.mlrCategorize()
    return(self.predictionLevel)

  """
  Function: mlrCategorize
  Purpose: For the regression formula, this catergorizes the value. 
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def mlrCategorize(self):
    self.predictionLevel = predictionLevels.NO_TEST
    if(self.mlrResult != None):
      if(self.mlrResult < self.lowCategoryLimit):
        self.predictionLevel = predictionLevels.LOW
      elif(self.mlrResult >= self.highCategoryLimit):
        self.predictionLevel = predictionLevels.HIGH
      else:      
        self.predictionLevel = predictionLevels.MEDIUM
  """
  Function: getResults
  Purpose: Returns a dictionary with the variables that went into the predictionLevel.
  Parameters:
    None
  Return: A dictionary.
  """
  def getResults(self):
    name = "%sPrediction" % (self.name)
    results = {
               name : predictionLevels(self.predictionLevel).__str__(),
               #'mlrPrediction' : predictionLevels(self.predictionLevel).__str__(), 
               'log10MLRResult' : self.log10MLRResult,
               'mlrResult' : self.mlrResult}
    return(results)
    

"""
Class: cartPredictionTest
Purpose: DHEC CART prediction.  
"""
class cartPredictionTest(predictionTest):
  def __init__(self, formula, name):
    predictionTest.__init__(self, formula, name)
  
  """
  Function: runTest
  Purpose: Excutes the if/then decision tree code to calculate the CART prediction level. 
    NOTE: Any CART code needs to have a variable named "cartPrediction" which contains the predicted level
    after the exec function is run on the code.
  Parameter:
    data - A dictionary populated with all the variables needed for the string substitutions.
  Return:
    Prediction level: LOW, MEDIUM, or HIGH.
  """
  def runTest(self, data):
    decisionTree = self.formula % (data)
    exec decisionTree
    self.predictionLevel = cartPrediction                         
    return(self.predictionLevel)

  def getResults(self):
    name = "%sPrediction" % (self.name)    
    results = {name : predictionLevels(self.predictionLevel).__str__()}
               #'cartPrediction' : predictionLevels(self.predictionLevel).__str__()}
    return(results)

"""
Class: outputResults
Purpose: Base class for creating objects to output the results in various formats, email, kml, ect.
"""
class outputResults(object):
  def __init__(self, xmlConfigFile, logger=None):
    self.configFile = xmlConfigFile
    self.logger = logger
    #Are we outputing the data used in the predictions?
    tag = "//environment/stationTesting/results/outputDataUsed"
    self.outputData = self.configFile.getEntry(tag)    
    if(self.outputData != None):
      self.outputData = int(self.outputData)
      self.varMapping = {}
      #If we are going to ouput the data used for the predictions, we get the readable variable names/descriptions
      #and create a mapping.
      try:
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
      except Exception, e:
        if(self.logger != None):
          self.logger.exception(e)
    else:
      self.outputData = 0

  def createOutput(self, testObjects, beginDate, endDate, testRunDate):
    return(False)
  
class outputKMLResults(outputResults):
  def __init__(self, xmlConfigFile, logger=None):
    outputResults.__init__(self, xmlConfigFile, logger)

  def createOutput(self, testObjects, beginDate, endDate, testRunDate):
    from pykml import kml
    tag = "//environment/stationTesting/results/outputResultList/outputType[@id=\"kml\"]/kmlFilePath"
    kmlFilepath = self.configFile.getEntry(tag)

    if(kmlFilepath != None):

      nexradDBSettings = self.configFile.getDatabaseSettingsEx('//environment/stationTesting/database/nexradDatabase/')
      nexradDB = dhecDB(nexradDBSettings['dbName'],"dhec_testing_logger")
          
      pmTableBegin = """<table>"""
      pmTableEnd = """</table>"""
      pmTemplate = """<tr><td>%(region)s</td></tr>
        <tr><td>Station:</td><td>%(station)s</td><td>%(description)s</td></tr>
        <tr><td>Prediction for Date:</td><td>%(endDate)s</td></tr>        
        <tr><td>Date Tests Run:</td><td>%(testRunDate)s</td></tr>        
        <tr><td>Overall Prediction:</td><td>%(ensemblePrediction)s</td></tr>
        <tr><td>MLR:</td><td>%(dhecMLRPrediction)s</td><td>log10(etcoc): %(log10MLRResult)4.2f etcoc %(mlrResult)4.2f</td></tr>
        <tr><td>Cart:</td><td>%(dhecCARTPrediction)s</td></tr>"""
        
        
      try:        
        self.logger.info("Creating DHEC ETCOC Prediction KML file: %s" % (kmlFilepath))
        etcocKML = kml.KML()
        doc = etcocKML.createDocument("DHEC ETCOC Predictions")
        doc.appendChild(etcocKML.createStyle(
            id="no_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://secoora.org/resources/images/default/no_light16x16.png"))
        ))
        doc.appendChild(etcocKML.createStyle(
            id="low_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://secoora.org/resources/images/default/green_light16x16.png"))            
        ))
        doc.appendChild(etcocKML.createStyle(
            id="med_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://secoora.org/resources/images/default/yellow_light16x16.png"))            
        ))
        doc.appendChild(etcocKML.createStyle(
            id="hi_prediction",
            children = etcocKML.createIconStyle(scale=0.5, icon=etcocKML.createIcon(iconUrl="http://secoora.org/resources/images/default/red_light16x16.png"))                        
        ))
        for wqObj in testObjects:      
          
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
              self.logger.error("ERROR: Unable to get platform: %s data. %s" % (platformHandle, nexradDB.getErrorInfo()))
            desc += pmTableBegin
            tstObj = wqObj.results[station]
            tmpltDict = { 'region' : wqObj.regionName,
              'station' : station,
              'ensemblePrediction' : predictionLevels(tstObj.ensemblePrediction).__str__(),
              'testRunDate' : testRunDate.strftime('%Y-%m-%d %H:%M'),
              'endDate' : endDate.strftime('%Y-%m-%d %H:%M'),
              'description' : stationDesc}
            
            #Get any specific computed variables from the test object. This is not the data that
            #the equations used, but any intermediate values calculated while coming up with the prediction.
            for test in tstObj.tests:
              results = test.getResults()
              for resultType in results:
                result = results[resultType]
                if(result == None):
                  result = -9999               
                tmpltDict[resultType] = result
                
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
            
            if(self.outputData):
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
        nexradDB.DB.close()

        return(True)
      except Exception, e:
        if(self.logger != None):
          self.logger.exception(e)
                
    else:
      self.logger.debug("Cannot write KML file, no filepath provided in config file.")
    return(False)
    
class outputEmailResults(outputResults):
  def __init__(self, xmlConfigFile, logger=None):
    outputResults.__init__(self, xmlConfigFile, logger)
        
  def createOutput(self, testObjects, beginDate, endDate, testRunDate):
    from xeniatools.utils import smtpClass 
    import string
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
    if(self.outputData):
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
      emailSettings = self.configFile.getEmailSettingsEx('//environment/stationTesting/results/outputResultList/outputType[@id=\"email\"]/emailSettings')
      #Loop through the results objects to get the individual station test results.
      body = header % ({'endDate' : endDate.strftime('%Y-%m-%d'), 'testRunDate' : testRunDate.strftime('%Y-%m-%d %H:%M')})
      for wqObj in testObjects:
        body += regionHdr % (wqObj.regionName)
        #The stationKeys are the names of the stations, let's sort them so they'll be in an increasing
        #alpha numeric order.          
        stationKeys = wqObj.results.keys()
        stationKeys.sort()
        dataUsed = ""
        for station in stationKeys:
          tstObj = wqObj.results[station]

          #If we want to output the data, and we have not already populate the station non-specific variables.
          if(self.outputData and len(dataUsed) == 0):
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
            'station_coefficient' : stationCo}
          #Get any specific computed variables from the test object. This is not the data that
          #the equations used, but any intermediate values calculated while coming up with the prediction.
          for test in tstObj.tests:
            results = test.getResults()
            for resultType in results:
              result = results[resultType]
              if(result == None):
                result = -9999
              tmpltDict[resultType] = result
          
          body += (msgTemplate % (tmpltDict))
          tmpltDict.clear()
        if(self.outputData):
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
      return(True)
    except Exception,e:
      if(self.logger != None):
        self.logger.exception(e)                             
    return(False)


class outputJSONResults(outputResults):
  def __init__(self, xmlConfigFile, logger=None):
    outputResults.__init__(self, xmlConfigFile, logger)

  def createOutput(self, testObjects, beginDate, endDate, testRunDate):
    import simplejson as json
    
    try:
      tag = "//environment/stationTesting/results/outputResultList/outputType[@id=\"json\"]/filePath"
      jsonFilepath = self.configFile.getEntry(tag)
  
      if(jsonFilepath != None):    
        jsonResults = []
        resultsInfo = {}
        icons = {}
        icons['NO TEST'] = "http://secoora.org/resources/images/default/no_light16x16.png"
        icons['LOW'] = "http://secoora.org/resources/images/default/green_light16x16.png"
        icons['MEDIUM'] = "http://secoora.org/resources/images/default/yellow_light16x16.png"
        icons['HIGH'] = "http://secoora.org/resources/images/default/red_light16x16.png"
        resultsInfo['icons'] = icons
        
        resultsInfo['testDate'] = ""
        resultsInfo['runDate'] = ""
        nexradDBSettings = self.configFile.getDatabaseSettingsEx('//environment/stationTesting/database/nexradDatabase/')
        nexradDB = dhecDB(nexradDBSettings['dbName'],"dhec_testing_logger")
        features = []
        for wqObj in testObjects:
          stationKeys = wqObj.results.keys()
          stationKeys.sort()
          dataUsed = ""
          for station in stationKeys:
            feature = {}
            feature['type'] = 'Feature'
            tstObj = wqObj.results[station]
            platformHandle = "dhec.%s.monitorstation" % (station)
            dbCursor = nexradDB.getPlatformInfo(platformHandle)
            latitude = 0.0
            longitude = 0.0
            stationDesc = ""
            geometry = {}
            if(dbCursor != None):
              row = dbCursor.fetchone()
              if(row != None):
                stationDesc = row['description']
                geometry['type'] = "Point"
                geometry['coordinates'] = [row['fixed_longitude'],row['fixed_latitude']]
                feature['geometry'] = geometry
            else:
              self.logger.error("ERROR: Unable to get platform: %s data. %s" % (platformHandle, nexradDB.getErrorInfo()))
              
            property = {}
            """
            stationData = {
              'station' : station,
              'lat' : latitude,
              'lon' : longitude,
              'region' : wqObj.regionName,
              'ensemble' : predictionLevels(tstObj.ensemblePrediction).__str__(),
              #'testDate' : testRunDate.strftime('%Y-%m-%d %H:%M'),
              #'endDate' : endDate.strftime('%Y-%m-%d %H:%M'),
              'desc' : stationDesc}
            
            #All the tests are run at the same time and for the same time period, so we just store them once.
            if(resultsInfo['testDate'] == ""):
              resultsInfo['testDate'] = endDate.strftime('%Y-%m-%d %H:%M')
            if(resultsInfo['runDate'] == ""):
              resultsInfo['runDate'] = testRunDate.strftime('%Y-%m-%d %H:%M')
              
            #Get any specific computed variables from the test object. This is not the data that
            #the equations used, but any intermediate values calculated while coming up with the prediction.
            testData = {}
            for test in tstObj.tests:
              results = test.getResults()
              name = test.name
              testData[name] = results
            stationData['tests'] = testData
            """
            #All the tests are run at the same time and for the same time period, so we just store them once.
            if(resultsInfo['testDate'] == ""):
              resultsInfo['testDate'] = endDate.strftime('%Y-%m-%d %H:%M')
            if(resultsInfo['runDate'] == ""):
              resultsInfo['runDate'] = testRunDate.strftime('%Y-%m-%d %H:%M')

            property['station'] = station
            property['region'] = wqObj.regionName
            property['ensemble'] = predictionLevels(tstObj.ensemblePrediction).__str__()
            property['desc'] = stationDesc
              
            #Get any specific computed variables from the test object. This is not the data that
            #the equations used, but any intermediate values calculated while coming up with the prediction.
            testData = {}
            for test in tstObj.tests:
              results = test.getResults()
              name = test.name
              testData[name] = results
            property['tests'] = testData
            
            property['icon'] = tstObj.ensemblePrediction
            
            feature['properties'] = property
            features.append(feature)
        
        stationData = {}
        stationData['type'] = "FeatureCollection";
        stationData['features'] = features
            
        resultsInfo['stationData'] = stationData
            
      jsonPlatformFile = open(jsonFilepath, "w")
      jsonPlatformFile.write(json.dumps(resultsInfo, sort_keys=True))
      jsonPlatformFile.close()
      
    except Exception, e:
      if(self.logger != None):
        self.logger.exception(e)
      return(False)
  
""" 
Class wqTest
Purpose: This is the base class for the actually water quality prediction process.
 Each watershed area has its own MLR and CART tests, so this base class doesn't implement
 anything other than stub functions for them.
"""
class wqEquations(object):
  """
  Function: __init__
  Purpose: Initializes the object with all the tests to be performed for the station.
  Parameters:
    station - The name of the station this object is being setup for.
    testsSetup - A list of objects that detail the tests. Each test object must contain the following format:
        {'testId' : testsSetupInfo, 
         'testString' : predictionTestString, 
         'testObject' : predictionTstObj})
    logger - A reference to the logging object to use.
  """
  def __init__(self, station, testsSetup, logger=None):
    self.station = station  #The station that this object represents.
    self.tests = []
    self.ensemblePrediction = predictionLevels.NO_TEST
    for testNfo in testsSetup:
      #Retrieve the processing class      
      testObj = globals()[testNfo['testObject']]
      #Now we instantiate the object.
      predictionObj = testObj(testNfo['testString'], testNfo['testId'])      
      self.tests.append(predictionObj)
    self.data = {} #Data used for the tests.
    self.logger = logger

  """
  Function: addTest
  Purpose: Adds a prediction test to the list of tests.
  Parameters:
    predictionTestObj -  A predictionTest object to use for testing. 
  """
  def addTest(self, predictionTestObj):
    self.tests.append(predictionTestObj)
  
  """
  Function: runTests
  Purpose: Runs the suite of tests, current a regression formula and CART model, then tabulates
    the overall prediction. 
  Parameters:
    dataDict - A data dictionary keyed on the variable names in the CART tree. String subsitution
      is done then the formula is evaled.
  Return:
    A predictionLevels value representing the overall prediction level. This is the average of the individual 
    prediction levels.
  """    
  def runTests(self, dataDict):
    self.data = dataDict.copy()
    for testObj in self.tests:
      testObj.runTest(dataDict)
      if(self.logger != None):
        self.logger.debug("Test: %s Prediction: %d" %(testObj.name, testObj.predictionLevel))
      
    self.overallPrediction()
  """
  Function: overallPrediction
  Purpose: From the models used, averages their predicition values to come up with the overall value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """      
  def overallPrediction(self):
    allTestsComplete = True
    executedTstCnt = 0
    if(len(self.tests)):
      sum = 0
      for testObj in self.tests:
        #DWR 2011-10-11
        #If a test wasn't executed, we skip using it.
        if(testObj.predictionLevel != predictionLevels.NO_TEST):
          sum += testObj.predictionLevel
          executedTstCnt += 1
      if(executedTstCnt):
        self.ensemblePrediction = int(round(sum / float(executedTstCnt)))
        
           
    if(self.logger != None):
      self.logger.debug("Overal Prediction: %d(%s)" %(self.ensemblePrediction, predictionLevels(self.ensemblePrediction).__str__()))
    return(self.ensemblePrediction)
  

"""
Class wqDataAccess
Purpose: This is the base class for retrieving the data for the tests. This class interfaces
  with the databases that house the observation and NEXRAD data.

"""
class wqDataAccess(object):
  NO_DATA = -9999.0
  def __init__(self, configFile, obsDb, nexradDb, regionName="", logger=None):
    self.obsDb = obsDb              #The database object connected to the observations database.
    self.nexradDb = nexradDb        #Database object connected to the NEXRAD database.
    self.logger = logger            #The logger object.    
    self.configFile = configFile    #A reference to an xmlConfigFile object.
    self.regionName = regionName    #The region this data object is for.
    self.results = {}               #Dictionary of wqEquation objects keyed on the stations for the test results.
  
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
        #DWR 2013-02-04 Added qc_level check.
        sql = "SELECT AVG(m_value) as m_value_avg  FROM multi_obs\
               WHERE \
               (m_date >= '%s' AND \
               m_date < '%s') AND \
               sensor_id = %d AND \
               (qc_level = %d OR qc_level IS NULL)"\
              %(beginDate, endDate, sensorID, qaqcTestFlags.DATA_QUAL_GOOD)
         
      dbCursor = self.obsDb.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row['m_value_avg'] != None):         
          avg = float(row['m_value_avg'])
          if(self.logger):
            self.logger.debug("SQL: %s" % (sql))
            self.logger.debug("platformHandle: %s obsName: %s(%s) avg: %s(%f)" % (platformHandle, obsName, uom, row['m_value_avg'], avg))                  
        return(avg)
    else:
      self.logger.error("No sensor ID found for observation: %s(%s) on platform: %s" %(obsName, uom, platformHandle))
    return(None)

  """
  Function: calcAvgWindSpeedAndDir
  Purpose: Wind direction is measured from 0-360 degrees, so around the inflection point trying to do an average can
  have bad results. For instance 250 degrees and 4 degrees are roughly in the same direction, but doing a straight 
  average will result in something nowhere near correct. This function takes the speed and direction and converts
  to a vector.
  Parameters:
    platName - String representing the platform name to query
    startDate the date/time to start the average
    endDate the date/time to stop the average
  Returns:
    A tuple setup to contain [0][0] = the vector speed and [0][1] direction average
      [1][0] - Scalar speed average [1][1] - vector direction average with unity speed used.
  """
  def calcAvgWindSpeedAndDir(self, platName, startDate, endDate):    
    windComponents = []
    dirComponents = []
    vectObj = vectorMagDir();
    #Get the wind speed and direction so we can correctly average the data.    
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    windSpdId = self.obsDb.dbConnection.sensorExists('wind_speed', 'm_s-1', platName)
    windDirId = self.obsDb.dbConnection.sensorExists('wind_from_direction', 'degrees_true', platName)
    sql = "SELECT m_date ,m_value FROM multi_obs\
           WHERE sensor_id = %d AND\
           (m_date >= '%s' AND \
           m_date < '%s' ) ORDER BY m_date"\
          %(windSpdId, startDate, endDate)
    if(self.logger):
      self.logger.debug("Wind Speed SQL: %s" % (sql))

    windSpdCursor = self.obsDb.executeQuery(sql)
    sql = "SELECT m_date ,m_value FROM multi_obs\
           WHERE sensor_id = %d AND\
           (m_date >= '%s' AND \
           m_date < '%s' ) ORDER BY m_date"\
          %(windDirId, startDate, endDate) 
    if(self.logger):
      self.logger.debug("Wind Dir SQL: %s" % (sql))
    windDirCursor = self.obsDb.executeQuery(sql)
    scalarSpd = None
    spdCnt = 0
    for spdRow in windSpdCursor:
      if(scalarSpd == None):
        scalarSpd = 0
      scalarSpd += spdRow['m_value']
      spdCnt += 1
      for dirRow in windDirCursor:
        if(spdRow['m_date'] == dirRow['m_date']):
          #print("Calculating vector for Speed(%s): %f Dir(%s): %f" % (spdRow['m_date'], spdRow['m_value'], dirRow['m_date'], dirRow['m_value']))
          #Vector using both speed and direction.
          windComponents.append(vectObj.calcVector(spdRow['m_value'], dirRow['m_value']))
          #VEctor with speed as constant(1), and direction.
          dirComponents.append(vectObj.calcVector(1, dirRow['m_value']))
          break
    #Get our average on the east and north components of the wind vector.
    spdAvg = None  
    dirAvg = None
    scalarSpdAvg = None
    vectorDirAvg = None
    if(len(windComponents)):
      eastCompAvg = 0
      northCompAvg = 0
      scalarSpdAvg = scalarSpd / spdCnt
      
      for vectorTuple in dirComponents:
        eastCompAvg += vectorTuple[0]
        northCompAvg += vectorTuple[1]
      if(eastCompAvg != None and northCompAvg != None):
        eastCompAvg = eastCompAvg / len(dirComponents)
        northCompAvg = northCompAvg / len(dirComponents)
        spdAvg,vectorDirAvg = vectObj.calcMagAndDir(eastCompAvg, northCompAvg)
        if(self.logger):
          self.logger.debug("Platform: %s Scalar Speed Avg: %f Vector DirAvg: %f" % (platName,scalarSpdAvg,vectorDirAvg))      
      
      for vectorTuple in windComponents:
        eastCompAvg += vectorTuple[0]
        northCompAvg += vectorTuple[1]
        
      if(eastCompAvg != None and northCompAvg != None):
        eastCompAvg = eastCompAvg / len(windComponents)
        northCompAvg = northCompAvg / len(windComponents)
        #Calculate average with speed and direction components.
        spdAvg,dirAvg = vectObj.calcMagAndDir(eastCompAvg, northCompAvg)      
        if(self.logger):
          self.logger.debug("Platform: %s Vector Speed Avg: %f DirAvg: %f" % (platName,spdAvg,dirAvg))      
        
    return(((spdAvg, dirAvg), (scalarSpdAvg, vectorDirAvg)))

  """
  Function: addTest
  Purpose: Adds a prediction test to the list of tests.
  Parameters:
    predictionTest - A string that represents the prediction formula to use. Uses parameter substition to populate
      the formala.
    predictionTestObj -  A string that represents the object to create for the predictionTest formula.
  """
  def addTest(self, predictionTestObj):
    self.tests.append(predictionTestObj)
    
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
    if(self.logger != None):
      self.logger.info("Processing stations for region: %s BeginDate: %s EndDate: %s"\
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
      if(self.logger != None):
        self.logger.debug("%s intercept value %s." %(self.regionName, intercept))
    else:
      if(self.logger != None):
        self.logger.error("Intercept value not available for %s, cannot continue." %(self.regionName))
      return(False)
          
    #Get the test equations.
    testsSetupInfo = []
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/tests" % (self.regionName)
    predictionTestList = self.configFile.getListHead(tag)
    for predictionTest in self.configFile.getNextInList(predictionTestList):
      testId = predictionTest.get('id')
      predictionTestString = self.configFile.getEntry('predictionTest', predictionTest)
      if(predictionTestString == None):
        if(self.logger != None):        
          self.logger.error("No formula found for %s, cannot continue." %(self.regionName))
        return(False)      
      predictionTestString = predictionTestString.lstrip()
      predictionTestString = predictionTestString.rstrip()
      predictionTstObj = self.configFile.getEntry('predictionTestObj', predictionTest)
      if(predictionTstObj == None):
        if(self.logger != None):        
          self.logger.error("No formula object for %s, cannot continue." %(self.regionName))
        return(False)
      #Check to make sure the predictionTstObj actually exists in our namespace.
      if(predictionTstObj in globals()):
        testsSetupInfo.append( {'testId' : testId, 
                                'testString' : predictionTestString, 
                                'testObject' : predictionTstObj})
        #Retrieve the processing class      
        #testObj = globals()[predictionTstObj]
        #Now we instantiate the object.
        #predictionObj = testObj(predictionTestString, testId)      
        #self.addTest(predictionObj)
      else:
        if(self.logger != None):
          self.logger.error("Object: %s not found in the global namespace, cannot create a test object." %(predictionTstObj))
    
    tag = "//environment/stationTesting/watersheds/watershed[@id=\"%s\"]/stations" % (self.regionName)
    stationList = self.configFile.getListHead(tag)
    for station in self.configFile.getNextInList(stationList):
      stationName = station.get('id')   
      if(stationName == None):
        if(self.logger != None):
          self.logger.error("No station name found for %s, skipping to next station." %(self.regionName))
        continue
      coefficient = self.configFile.getEntry('coefficient', station)
      if(coefficient != None):
        data['station_coefficient'] = float(coefficient)
        if(self.logger != None):
          self.logger.debug("%s %s coefficient value %s." %(self.regionName, stationName, coefficient))
      else:
        if(self.logger != None):
          self.logger.error("coefficient value not available for %s station: %s cannot continue." %(self.regionName, stationName))
        continue            
      self.runTests(stationName, data, testsSetupInfo)  
    return(True)
    
  def runTests(self, stationName, data, testsSetupInfo):
    wqTest = wqEquations(stationName, testsSetupInfo, self.logger)
            
    #Run through the data, if any of the values are -9999, we didn't get a value so we
    #cannot run the tests. Let's log that out.
    dataValid = True
    for dataKey in data:
      if(data[dataKey] == -9999):
        if(self.logger != None):
          self.logger.error("%s has a value of -9999, we cannot run the tests." %(dataKey))
        dataValid = False
    
    if(dataValid):
      try:
        wqTest.runTests(data)
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
    if(dryCnt != None):
      data['radar_preceeding_dry_day_cnt'] = dryCnt
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    
    #Get the water temperature from SUN2
    data['sun2_water_temp'] = self.NO_DATA    
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp != None):
      data['sun2_water_temp'] = sun2_water_temp
    else:
      self.logger.error("Error retrieving water_temperature from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    
    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      

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
      if(self.logger != None):
        self.logger.error("Error retrieving tide data. Error: %s" %(e))
    except Exception, e:
      if(self.logger != None):
        self.logger.error("Error retrieving tide data. Error: %s" %(e))
    
    if(self.logger != None):
      self.logger.debug(pformat(data))
    
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
    if(radar_rain_summary_48 != None):
      data['radar_rain_summary_48'] = radar_rain_summary_48
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
       
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
    
    self.logger.debug(pformat(data))
    
    return(data)

class wqDataMB1(wqDataAccess):
  
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB1", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, data, testsSetupInfo):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, data, testsSetupInfo)
    
  def getData(self, beginDate, endDate):
    data = {}
       
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 != None):
      data['radar_rain_summary_48'] = radar_rain_summary_48
    else:
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_rain_summary_144'] = self.NO_DATA
    radar_rain_summary_144 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 144)
    if(radar_rain_summary_144 != None):
      data['radar_rain_summary_144'] = radar_rain_summary_144
    else:
      self.logger.error("Error retrieving radar_rain_summary_144. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))     

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      

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
    
    
    self.logger.debug(pformat(data))
    
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
    if(radar_rain_summary_24 != None):
      data['radar_rain_summary_24'] = radar_rain_summary_24
    else:
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt != None):
      data['radar_preceeding_dry_day_cnt'] = dryCnt
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    #Get the salinity from SUN2
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
    
    #2013-10-22 DWR
    #Use the new windspeed/direction averaging.
    data['nos8661070_wind_dir'] = self.NO_DATA
    avgWindComponents = self.calcAvgWindSpeedAndDir('nos.8661070.WL', startDate, stopDate)
    if(avgWindComponents[1][1] != None):
      data['nos8661070_wind_dir'] = self.obsDb.dbConnection.compassDirToCardinalPt(avgWindComponents[1][1])
    else:
      self.logger.error("Error retrieving nos8661070_wind_dir from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    """  
    nos8661070_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_wind_dir == None):
      self.logger.error("Error retrieving nos8661070_wind_dir from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    elif(nos8661070_wind_dir != self.NO_DATA):        
      nos8661070_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(nos8661070_wind_dir)
      data['nos8661070_wind_dir'] = nos8661070_compass_dir
    """
    
    self.logger.debug(pformat(data))
    
    return(data)

class wqDataMB3(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB3", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, data, testsSetupInfo):
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, data, testsSetupInfo)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data
    data['radar_rain_summary_24'] = self.NO_DATA
    radar_rain_summary_24 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 24)
    if(radar_rain_summary_24 != None):
      data['radar_rain_summary_24'] = radar_rain_summary_24
    else:
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 != None):
      data['radar_rain_summary_48'] = radar_rain_summary_48
    else:
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt != None):
      data['radar_preceeding_dry_day_cnt'] = dryCnt
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    #Get the wind speed/direction from SUN2
    #2013-10-22 DWR
    #Use the new windspeed/direction averaging.
    data['sun2_wind_speed'] = self.NO_DATA
    data['sun2_wind_dir'] = self.NO_DATA
    avgWindComponents = self.calcAvgWindSpeedAndDir('carocoops.SUN2.buoy', startDate, stopDate)
    if(avgWindComponents[1][0] != None and avgWindComponents[1][1] != None):
      data['sun2_wind_speed'] = avgWindComponents[1][0] * 1.9438444924406
      data['sun2_wind_dir'] = self.obsDb.dbConnection.compassDirToCardinalPt(avgWindComponents[1][1])
    else:
      self.logger.error("Error retrieving wind speed/direciton from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    
    """
    data['sun2_wind_speed'] = self.NO_DATA
    sun2_wind_speed = self.getAverageForObs('wind_speed', 'm_s-1', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_speed != None and sun2_wind_speed != self.NO_DATA):
      sun2_wind_speed *= 1.9438444924406    
      data['sun2_wind_speed'] = sun2_wind_speed
    #Convert to knots.
    else:
      self.logger.error("Error retrieving sun2_wind_speed from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    data['sun2_wind_dir'] = self.NO_DATA
    sun2_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_dir == None):
      self.logger.error("Error retrieving sun2_wind_dir from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    elif(sun2_wind_dir != self.NO_DATA):
      sun2_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(sun2_wind_dir)
      data['sun2_wind_dir'] = sun2_compass_dir
    """
    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      
           
    self.logger.debug(pformat(data))
    
    return(data)

class wqDataMB4(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="MB4", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, data, testsSetupInfo):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, data, testsSetupInfo)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 != None):
      data['radar_rain_summary_48'] = radar_rain_summary_48
    else:
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt != None):
      data['radar_preceeding_dry_day_cnt'] = dryCnt
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp != None):
      data['sun2_water_temp'] = sun2_water_temp
    else:
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    
    #2013-10-22 DWR
    #Use the new wind speed/direction averaging.
    data['sun2_wind_dir'] = self.NO_DATA
    avgWindComponents = self.calcAvgWindSpeedAndDir('carocoops.SUN2.buoy', startDate, stopDate)
    if(avgWindComponents[1][1] != None):
      data['sun2_wind_dir'] = self.obsDb.dbConnection.compassDirToCardinalPt(avgWindComponents[1][1])
    else:
      self.logger.error("Error retrieving wind speed/direction from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    """
    data['sun2_wind_dir'] = self.NO_DATA
    sun2_wind_dir = self.getAverageForObs('wind_from_direction', 'degrees_true', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_wind_dir == None):
      self.logger.error("Error retrieving sun2_wind_dir from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    elif(sun2_wind_dir != self.NO_DATA):
      sun2_compass_dir = self.obsDb.dbConnection.compassDirToCardinalPt(sun2_wind_dir)
      data['sun2_wind_dir'] = sun2_compass_dir
    """
    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))
    
    #Get NOS water level
    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      

    
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
           
    self.logger.debug(pformat(data))
    
    return(data)

class wqDataSS(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="Surfside", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, data, testsSetupInfo):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, data, testsSetupInfo)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_24'] = self.NO_DATA
    radar_rain_summary_24 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 24)
    if(radar_rain_summary_24 != None):
      data['radar_rain_summary_24'] = radar_rain_summary_24
    else:
      self.logger.error("Error retrieving radar_rain_summary_24. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_rainfall_intensity_24'] = self.NO_DATA
    radar_rainfall_intensity_24 = self.nexradDb.calcRadarRainfallIntensity(self.regionName.lower(), startDate, 60)
    if(radar_rainfall_intensity_24 != None):
      data['radar_rainfall_intensity_24'] = radar_rainfall_intensity_24
    else:
      self.logger.error("Error retrieving radar_rainfall_intensity_24. Error: %s" %(self.nexradDb.getErrorInfo()))
        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp != None):
      data['sun2_water_temp'] = sun2_water_temp
    else:
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    data['nos8661070_water_temp'] = self.NO_DATA
    nos8661070_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_temp != None):
      data['nos8661070_water_temp'] = nos8661070_water_temp
    else:
      self.logger.error("Error retrieving nos8661070_water_temp from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      

    
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
           
    self.logger.debug(pformat(data))
    
    return(data)

class wqDataGC(wqDataAccess):
  def __init__(self, configFile, obsDb, nexradDb, regionName="Gardcty", logger=None):
    wqDataAccess.__init__(self,configFile, obsDb, nexradDb, regionName, logger)
      
  def runTests(self, stationName, data, testsSetupInfo):    
    data['station'] = stationName
    wqDataAccess.runTests(self, stationName, data, testsSetupInfo)
    
  def getData(self, beginDate, endDate):
    data = {}
    
    startDate = beginDate.strftime('%Y-%m-%dT%H:%M:%S')
    stopDate = endDate.strftime('%Y-%m-%dT%H:%M:%S')
    #Get the NEXRAD data

    data['radar_rain_summary_48'] = self.NO_DATA
    radar_rain_summary_48 = self.nexradDb.getLastNHoursSummaryFromRadarPrecip(startDate, self.regionName.lower(), 48)
    if(radar_rain_summary_48 != None):
      data['radar_rain_summary_48'] = radar_rain_summary_48
    else:
      self.logger.error("Error retrieving radar_rain_summary_48. Error: %s" %(self.nexradDb.getErrorInfo()))

    data['radar_preceeding_dry_day_cnt'] = self.NO_DATA
    dryCnt = self.nexradDb.getPrecedingRadarDryDaysCount(startDate, self.regionName.lower())
    if(dryCnt != None):
      data['radar_preceeding_dry_day_cnt'] = dryCnt
    else:
      self.logger.error("Error retrieving Radar Preceeding Dry Day Cnt. Error: %s" %(self.nexradDb.getErrorInfo()))

        
    data['sun2_water_temp'] = self.NO_DATA
    sun2_water_temp = self.getAverageForObs('water_temperature', 'celsius', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_water_temp != None):
      data['sun2_water_temp'] = sun2_water_temp
    else:
      self.logger.error("Error retrieving sun2_water_temp from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    data['sun2_salinity'] = self.NO_DATA
    sun2_salinity = self.getAverageForObs('salinity', 'psu', 'carocoops.SUN2.buoy', startDate, stopDate)
    if(sun2_salinity != None):
      data['sun2_salinity'] = sun2_salinity
    else:
      self.logger.error("Error retrieving salinity from SUN2. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))

    data['nos8661070_water_level'] = self.NO_DATA
    nos8661070_water_level = self.getAverageForObs('water_level', 'm', 'nos.8661070.WL', startDate, stopDate)
    if(nos8661070_water_level != None):
      data['nos8661070_water_level'] = nos8661070_water_level
    else:
      self.logger.error("Error retrieving water_level from nos8661070. Error: %s" %(self.obsDb.dbConnection.getErrorInfo()))      

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
           
    self.logger.debug(pformat(data))
    
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
      self.logger.error("Unable to connect to observation database: %s @ %s" %(obsDBSettings['dbName'], obsDBSettings['dbHost']))
      sys.exit(-1)
    else:
      self.logger.info("Connected to %s @ %s" %(obsDBSettings['dbName'], obsDBSettings['dbHost']))

    nexradDBSettings = self.configFile.getDatabaseSettingsEx('//environment/stationTesting/database/nexradDatabase/')
    nexradDB = dhecDB(nexradDBSettings['dbName'],"dhec_testing_logger")
        
    tag = "//environment/stationTesting/watersheds"
    regionList = self.configFile.getListHead(tag)
    for watershed in self.configFile.getNextInList(regionList):
      watershedName = watershed.get('id')
      testObjName = self.configFile.getEntry('testObject', watershed)
      self.logger.info("Region: %s processing object: %s" %(watershedName, testObjName))
      #Determine if the test object provided in the config file exists in the dictionary of available processing
      #objects.
      if(testObjName in globals()):
        #Retrieve the processing class      
        testObj = globals()[testObjName]
        #Now we instantiate the object.
        wqObj = testObj(configFile=configFile, obsDb=obsDB, nexradDb=nexradDB, logger=logger)
        wqObj.processData(beginDate, endDate)
        self.testObjects.append(wqObj)
        #Store the results in the db
        self.storeResults(nexradDB, wqObj, endDate.strftime('%Y-%m-%dT%H:%M:%S'), testRunDate.strftime('%Y-%m-%d %H:%M:%S'))
      else:
        if(self.logger != None):
          self.logger.error("Region: %s using invalid testObject: %s, cannot process." %(watershedName, testObjName))
    
    tag = "//environment/stationTesting/results/outputDataUsed"
    outputData = self.configFile.getEntry(tag)
    if(outputData == None):
      outputData = 0
       
    testBeginDate = beginDate.astimezone(timezone('US/Eastern'))
    testEndDate = endDate.astimezone(timezone('US/Eastern'))
    obsDB.dbConnection.DB.close()
    nexradDB.DB.close()
    self.sendResults(testBeginDate, testEndDate, testRunDate)

  def storeResults(self, dbObj, wqObj, endDate, testRunDate):    
    for station in wqObj.results:      
      if(self.logger != None):
        self.logger.info("Storing %s results to database." %(station))
      platformHandle = 'dhec.%s.monitorstation' % (station)
      platformCursor = dbObj.getPlatformInfo(platformHandle)
      if(platformCursor != None):
        nfo = platformCursor.fetchone()
        if(nfo != None):                  
          #Check to see if the result type exists as a sensor on the platform, if not add it.
          sensorId = dbObj.addSensor('overall_result', 'units', platformHandle, 1, 0, 1, None, True)        
          if(sensorId != None and sensorId != -1):
            mVals = []
            mVals.append(wqObj.results[station].ensemblePrediction)            
            if(dbObj.addMeasurement('overall_result', 'units',
                                               platformHandle,
                                               endDate,
                                               nfo['fixed_latitude'], nfo['fixed_longitude'],
                                               0,
                                               mVals,
                                               1,
                                               False,
                                               testRunDate) != True):
              if(self.logger != None):
                self.logger.error("Unable to add overall_result to database. %s" % (dbObj.getErrorInfo()))
            else:
              self.logger.debug("Added overall_result: %d to database." % (wqObj.results[station].ensemblePrediction))
          else:
            if(self.logger != None):
                self.logger.error("Unable to find or add sensor id for platform: %s obs: overall_result(units). %s"\
                                   % (platformHandle, dbObj.getErrorInfo()))
          #Now loop through the tests and add the results to the database.
          for testObj in wqObj.results[station].tests:
            obsName = None
            if(testObj.name == 'dhecMLR'):
              obsName = 'mlr_result'
            elif(testObj.name == 'dhecCART'):
              obsName = 'cart_result'
                
            #Check to see if the result type exists as a sensor on the platform, if not add it.
            sensorId = dbObj.addSensor(obsName, 'units', platformHandle, 1, 0, 1, None, True)        
            if(sensorId != None and sensorId != -1):
              del mVals[:]
              mVals.append(testObj.predictionLevel)            
              if(dbObj.addMeasurement(obsName, 'units',
                                                 platformHandle,
                                                 endDate,
                                                 nfo['fixed_latitude'], nfo['fixed_longitude'],
                                                 0,
                                                 mVals,
                                                 1,
                                                 False,
                                                 testRunDate) != True):
                if(self.logger != None):
                  self.logger.error("Unable to add %s to database. %s" % (obsName, dbObj.getErrorInfo()))
              else:
                self.logger.debug("Added %s: %d to database." % (obsName, testObj.predictionLevel))
            else:
              if(self.logger != None):
                  self.logger.error("Unable to find or add sensor id for platform: %s obs: %s(units). %s"\
                                     % (platformHandle, obsName, dbObj.getErrorInfo()))
                
    dbObj.commit()
    return

  def sendResults(self, testBeginDate, testEndDate, testRunDate):
    tag = "//environment/stationTesting/results/outputResultList"
    resultList = self.configFile.getListHead(tag)
    for result in self.configFile.getNextInList(resultList):
      objName = self.configFile.getEntry('object', result)
      if(objName in globals()):
        resultObj = globals()[objName]
        #Now we instantiate the object.
        outObj = resultObj(self.configFile, self.logger)
        #Create the output.
        outObj.createOutput(self.testObjects, testBeginDate, testEndDate, testRunDate)      
    return    

  
if __name__ == '__main__':
  try:
    #import psyco
    #psyco.full()
        
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
