#!/usr/bin/python
import sys
import os
import datetime
import optparse
import logging.config
import ConfigParser
from processNEXRAD import nexradProcess  
from xmrgFile import xmrgDB
import datetime
from datetime import tzinfo
from pytz import timezone
import csv
from lxml import objectify

class horryCountyNexradProcess(nexradProcess):
  def __init__(self, bbox, dbObj, logger, outputFilename, regionName, startSummaryHour, outputInches=True):
    nexradProcess.__init__(self, bbox, dbObj, logger, outputFilename, outputInches)
    self.currentRegionName = regionName        #Name of the overall watershed we are processing.
    self.startSummaryHour = startSummaryHour      #The 24 hour summary period starting hour.
    self.lastHourProcessed = None
  def polygonDictionaryFromKML(self, polygonDataSrc, utcDate):
    try:
      kmlFile = open(polygonDataSrc, 'r')
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      try:
        kmlRoot = objectify.parse(kmlFile).getroot()
        
        for child in kmlRoot.Document.iterchildren():
          pmCnt = 0
          for pm in child.Placemark:
            polypoints = []
            watershedName = pmCnt
            for simpleData in pm.ExtendedData.SchemaData.iterchildren():
              if(simpleData.attrib):
                if 'name' in simpleData.attrib:
                  if(simpleData.attrib['name'] == "HUC_12"):
                    watershedName = simpleData.text
                    break
            polygon = pm.Polygon.outerBoundaryIs.LinearRing.coordinates
            points = polygon.text.split(' ')
            for point in points:
              parts = point.split(',')
              polypoints.append(parts[0] + ' ' + parts[1])

            #Add the watershed into the database.
            self.addWatershedToDatabase(watershedName, polypoints, utcDate)
            self.polygonNames.append(watershedName)  
            pmCnt += 1      
        return(True)
      except Exception,e:
        if(self.logger):
          self.logger.exception(e)
      return(False)
    
  def doCalcs(self, outputFile, startTime, endTime):
    #Convert the times to EST, internally we are UTC
    utcTZ = timezone('UTC')  
    utcDate = utcTZ.localize(datetime.datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S"))
    estStartTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    utcDate = utcTZ.localize(datetime.datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S"))
    estEndTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    
    if(self.lastHourProcessed and self.lastHourProcessed == estStartTime):
      i = 0
    #Loop through our various watershed polygons and calculate the weighted average on each.
    for polygonName in self.polygonNames:        
      data = ""
      weightedAvg = self.dbObj.calculateWeightedAvg2(polygonName, startTime, endTime)
      if(self.logger):
        self.logger.debug("Polygon: %s Weighted Avg: %f StartTime: %s EndTime: %s, %s" % (polygonName,weightedAvg,startTime,endTime,data))
      if(self.dataInInches):
        #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
        #inches , need to divide by 2540.
        weightedAvg /= (25.4 * 100.0)
      #Convert to mm
      else:
        weightedAvg /= 100.0      
      outputFile.write("%s,%s,%s,%f\n" %(polygonName,estStartTime,estEndTime,weightedAvg))
    outputFile.flush()
    
    self.lastHourProcessed = estStartTime
    
    return

  def finishProcessing(self, outputFile):
    filename = outputFile.name 
    baseWatershedFilename = os.path.splitext(filename)
    outputFile.close()
    try:
      columnNames = ['Area', 'Start Time', 'End Time', 'Weighted Average']
      dataFileObj = open(filename, 'r')
      dataFile = csv.DictReader(dataFileObj, columnNames)
    except IOError,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      startTimeParts = self.startSummaryHour.split(':')
      startSummaryTime = datetime.time(int(startTimeParts[0]), int(startTimeParts[1]))
      for polygonName in self.polygonNames:
        try:
          watershedOutFilename = "%s_%s.csv" % (baseWatershedFilename[0], polygonName.replace(' ', '-'))
          if(self.logger):
            self.logger.info("Processing watershed: %s, output file: %s." % (polygonName, watershedOutFilename))
          watershedFile = open(watershedOutFilename, "w")
          watershedFile.write('Start,End,Weight Average Total,Number of Hours\n')
        except IOError,e:
          if(self.logger):
            self.logger(e)
        else:
          headerLine = None
          compareDate = None
          rainTotal = 0.0
          hourCnt = 0
          #Loop through the file processing the current watershed.
          for line in dataFile:
            if(headerLine == None):
              headerLine = line
            else:
              if(line['Area'] == polygonName):
                entryDate = datetime.datetime.strptime(line['Start Time'], "%Y-%m-%dT%H:%M:%S")
                #Initialize the compareDate.
                if(compareDate == None):
                  compareDate = entryDate;
                  compareDate.replace(hour = startSummaryTime.hour, minute = startSummaryTime.minute, second = startSummaryTime.second)
                
                delta = entryDate - compareDate
                #We've hit our daily summary, write out the results.
                if(delta.days >= 1):
                  if(hourCnt != 24):
                    hourCnt
                  outBuf = "%s,%s,%f,%d\n"\
                     % (compareDate.strftime("%Y-%m-%dT%H:%M:%S"),
                        entryDate.strftime("%Y-%m-%dT%H:%M:%S"), 
                        rainTotal, 
                        hourCnt)
                  watershedFile.write(outBuf)
                  compareDate = entryDate
                  rainTotal = 0.0
                  hourCnt = 0
                  
                rainTotal += float(line['Weighted Average'])
                hourCnt += 1
          #Finished with the watershed, close the file and move on to the next.
          watershedFile.close()
          #Reset the date file we are reading to the beginning of the file.
          dataFileObj.seek(0)
          if(self.logger):
            self.logger.info("Finished processing watershed: %s." % (polygonName))

    #CLose the source data file object      
    dataFileObj.close()
          
  def cleanDB(self):

    nowTime = datetime.datetime.now(timezone('UTC'))
    nowTime = nowTime.strftime("%Y-%m-%dT%H:%M:%S")
    if(self.logger):
      self.logger.debug("Cleaning up database, removing all dates older than: %s" % (nowTime)) 
    
    if(self.dbObj.cleanUp(nowTime)):
      
      sql = "DELETE FROM watershed_boundary;"
      cursor = self.dbObj.executeQuery(sql)
      if(cursor):
        self.dbObj.db.commit()
      else:
        self.logger.error( self.dbObj.lastErrorMsg )
        self.dbObj.db.rollback()

      if(self.logger):
        self.logger.debug("Vacuuming database.") 
      if(self.dbObj.vacuumDB() == False):
        if(self.logger):
          self.logger.error(self.dbObj.lastErrorMsg)
    else:
      if(self.logger):
        self.logger.error(self.dbObj.lastErrorMsg)

  def buildOuputFilename(self, paramSubs):
    paramSubs['watershed'] = self.currentRegionName
    return(nexradProcess.buildOuputFilename(self, paramSubs))
    
class dateControlFile:
  def __init__(self, dateFilename,logger=True):
    self.dateControlfilename = dateFilename
    self.logger = None
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)      
      
  def getReportingDates(self, dateToCheck):
    try:
      dataSendFile = open(self.dateControlfilename, "r")
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      sendDates = []
      for line in dataSendFile:
        line = line.rstrip()
        if(len(line)):
          if(self.logger):
            self.logger.debug("Processing line: %s" % (line))
          sendDate = datetime.datetime.strptime(line, '%B %d, %Y')
          sendDate = sendDate.replace()
          sendDates.append(sendDate)
      
      
      i = 0
      endDate = None
      startDate = None
      while( i < len(sendDates)):
        endDate = sendDates[i]
        #Compare only the month and day. This keeps us from having to update the file once a year to just change years.
        if((endDate.month == dateToCheck.month) and
           (endDate.day == dateToCheck.day)):
          #Now get the previous date since it is when this period starts. If i = 0 then get the
          #last date in the list.
          j = -1
          if(i > 0):
            j = i - 1
          startDate = sendDates[j]
          #Change the year to whatever the current year is if we are not at the first bi-week of the new year.
          if(startDate.month != 11 and endDate.month != 1):
            startDate = startDate.replace(year=dateToCheck.year)
          endDate = endDate.replace(year=dateToCheck.year)
          if(self.logger):
            self.logger.debug("Start Date: %s End Date: %s" % (endDate.strftime("%B %d, %Y"), startDate.strftime("%B %d, %Y")))
          break
        i += 1
      
      if(endDate != None and startDate != None):
        return(startDate,endDate)
    
    return(None,None)


if __name__ == '__main__':
  retVal = ""
  try:
    import psyco
    psyco.full()
  except Exception, E:
    print("Psyco package not available")
    
  logger = None

  try:    
    parser = optparse.OptionParser()  
    parser.add_option("-c", "--ConfigFile", dest="configFile",
                      help="INI file containing various parameters for processing." )

    (options, args) = parser.parse_args()

    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.configFile)
    
    logConfFile = configFile.get('logging', 'configFile')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger("horrycnt_nexrad_proc_logger")
      logger.info("Session started")

    try:
      watershedList = configFile.get("settings", "watersheds")
    except ConfigParser.Error, e:
      if(logger):
        logger.exception(e)
      sys.exit(-1)  
    else:
      watershedList = watershedList.split(',')
      
      #Process the individual watersheds.
      for watershed in watershedList:
        try:
          #Get the file that contains the dates to send out the data.
          dateControlFilename = configFile.get(watershed, 'dateControlFile')          
        except ConfigParser.Error, e:
          if(logger):
            logger.exception(e)
        else:
          #We want to check todays date against the control file.
          today = datetime.datetime.now()
          today = today.replace(hour=0,minute = 0,second = 0,microsecond = 0)
          
          checkDate = dateControlFile(dateControlFilename, True)
          startDate,endDate = checkDate.getReportingDates(today)
          if(startDate and endDate):
            #Get the required settings from the ini file.
            try:
              dbSettingsSection = watershed + '_databasesettings'
              nexradDbFile = configFile.get(dbSettingsSection, 'nexradDBFile')
              spatialiteLib = configFile.get(dbSettingsSection,'spatialiteLibPath')
              cleanDB = bool(configFile.get(dbSettingsSection, 'cleanDB'))
              nexradDataDir = configFile.get(watershed, 'NexradDataDir')
              removeRawNexradFiles = bool(configFile.get(watershed, 'removeRawDataFiles'))
              watershedPolygonSrc = configFile.get(watershed, 'AreaPolygonFile')
              outputInInches = bool(configFile.get(watershed, 'OutputInInches'))
              outputFilename = configFile.get(watershed, 'OutputFile')
            except ConfigParser.Error, e:
              if(logger):
                logger.exception(e)
            else:
              try:
                importBBOX = None
                writeImportKMLFile = False
                startSummaryHour = '07:00:00'
                #Params that aren't required
                importBBOX = configFile.get(watershed, 'importBBOX')
                writeImportKMLFile = configFile.get(watershed, 'writeImportKMLFile')
                startSummaryHour = configFile.get(watershed, 'startSummaryHour')
              except ConfigParser.Error, e:
                if(logger):
                  logger.exception(e)
              
              db = xmrgDB()
              if(db.connect(nexradDbFile, spatialiteLib) != True):
                if(logger):
                  logger.debug("Unable to connect to database: %s, cannot continue" % (nexradDbFile))   
                  sys.exit(-1)

               
              nexradProc = horryCountyNexradProcess(bbox = importBBOX, 
                                         dbObj = db, 
                                         logger=True,
                                         outputFilename = outputFilename,
                                         outputInches = outputInInches,
                                         regionName=watershed,
                                         startSummaryHour=startSummaryHour)
              nexradProc.writeImportKMLFile = nexradProc
              #Check to see if we want to clean the database before processing.       
              if(cleanDB):
                nexradProc.cleanDB()                
              
              nexradProc.buildPolygonDictionary(watershedPolygonSrc)
              nexradProc.importFilesIntoDB(nexradDataDir, removeRawNexradFiles)
          else:
            if(logger):
              logger.info("Date: %s Watershed: %s is not a reporting day." % (today.strftime("%Y-%m-%d"), watershed))
        
      
  except Exception, E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)
  
  sys.exit(retVal)