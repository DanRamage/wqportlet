#!/usr/bin/python
import sys
import os
import datetime
import optparse
import logging.config
import ConfigParser
from processNEXRAD import nexradProcess  
from xmrgFile import xmrgDB, xmrgCleanup
import datetime
from datetime import tzinfo
from pytz import timezone
import csv
from lxml import objectify
from ftplib import FTP

class horryCountyNexradProcess(nexradProcess):
  def __init__(self, bbox, dbObj, logger, outputFilename, regionName, startSummaryHour, outputInches, reportDay, startReportPeriod, endReportPeriod):
    nexradProcess.__init__(self, bbox, dbObj, logger, outputFilename, outputInches)
    self.currentRegionName = regionName        #Name of the overall watershed we are processing.
    self.startSummaryHour = startSummaryHour      #The 24 hour summary period starting hour.
    self.reportDay = reportDay
    self.startDate = startReportPeriod
    self.endDate = endReportPeriod
    self.lastHourProcessed = None
    self.ftpAddress = None
    self.ftpUser = None
    self.ftpPwd = None
    self.ftpDir = None    
  
  
  """
  Function: ftpSettings
  Purpose: Provides the info to log in to the FTP server where the files are placed when we have a reporting day.    
  Parameters:
    address - String for the host address to the FTP server.
    user - The user name used to login.
    pwd - THe password for the user.
    directory - The directory to place the files.
  Return:
    None
  """  
  def ftpSettings(self, address, user, pwd, directory):
    self.ftpAddress = address
    self.ftpUser = user
    self.ftpPwd = pwd
    self.ftpDir = directory    

  """
  Function: writeKMLFile
  Purpose: If we want to write a KML file out for each file processed that contains the grids, we call this function.    
  Parameters:
    writeKMLFile - True to write the KML file.
  Return:
    None
  """  
  def writeKMLFile(self, writeKMLFile):
    self.writeImportKMLFile = writeKMLFile
    
  """
  Function: polygonDictionaryFromKML
  Purpose: KML file that is HUC compliant. Contains the individual polygon areas that make up the entire watershed.
     Polygons are saved into the nexrad database file in the watershed_boundaries table.
  Parameters:
    polygonDataSrc - The full file path to the KML file.
    utcDate - Date used to denote when a polygon was saved into the database table.
  Return:
    True if successful in importing polygons, otherwise false.
    
  """  
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
    
  """
  Function: doCalcs
  Purpose: For each file processed, this function is called. We run through the polygons for the watershed doing the weighted average
    calculation, storing the results into the aggregate outputFile.
  Parameters:
    outputFile - file object where the results are written. 
    startTime - The starting date for the data we want to query and calculate the weighted average for. The time period
      represented by startTime and endTIme for this project is a single hour.
    endTime - Ending data for the data.
  Return:
    None    
  """  
  def doCalcs(self, outputFile, startTime, endTime):
    #Convert the times to EST, internally we are UTC
    utcTZ = timezone('UTC')  
    utcDate = utcTZ.localize(datetime.datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S"))
    estStartTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    utcDate = utcTZ.localize(datetime.datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S"))
    estEndTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    
    if(self.lastHourProcessed and self.lastHourProcessed == estStartTime):
      if(self.logger):
        self.logger.error("Processing same hour twice: %s" % (self.lastHourProcessed))
        
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
   
  """
  Function: finishProcessing
  Purpose: WHen we are done processing all the XMRG files, this function is called to complete the processing run.
    On reporting days, the outputFile is processed for each polygon in the watershed creating a daily summary from the hourly data.
    Each polygon will have a file created with the daily summaries for the reporting period.
  Parameters:
    outputFile - file object where the results are written.   
  Return:
    None    
  """  
  def finishProcessing(self, outputFile):
    
    #Are we on a reporting date?
    if(self.reportDay):
      filename = outputFile.name 
      baseWatershedFilename = os.path.splitext(filename)
      baseFilepath = os.path.dirname(filename)
      outputFile.close()
      try:
        columnNames = ['Area', 'Start Time', 'End Time', 'Weighted Average']
        dataFileObj = open(filename, 'r')
        dataFile = csv.DictReader(dataFileObj, columnNames)
      except IOError,e:
        if(self.logger):
          self.logger.exception(e)
      else:
        #THis is the list of the files we're going to FTP.
        ftpFileList = []      
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
            
            #ftpFileList.append(("%s_%s-%s.csv" % (self.startDate.strftime("%Y-%m-%d"), self.endDate.strftime("%Y-%m-%d"), polygonName.replace(' ', '-'))))
            ftpFileList.append(os.path.split(watershedOutFilename)[1])
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
            #Testing code for parital day.      
            if(hourCnt):
              outBuf = "%s,%s,%f,%d\n"\
                 % (compareDate.strftime("%Y-%m-%dT%H:%M:%S"),
                    entryDate.strftime("%Y-%m-%dT%H:%M:%S"), 
                    rainTotal, 
                    hourCnt)
              watershedFile.write(outBuf)
              
            #Finished with the watershed, close the file and move on to the next.
            watershedFile.close()
            #Reset the date file we are reading to the beginning of the file.
            dataFileObj.seek(0)
            if(self.logger):
              self.logger.info("Finished processing watershed: %s." % (polygonName))
              
        if(self.ftpAddress):
          try:
            if(self.logger):
              self.logger.info("Logging onto FTP server.")
            ftp = FTP()
            ftp.connect(self.ftpAddress)
            ftp.login(self.ftpUser, self.ftpPwd)
            #Have to change to active mode to get the transfer working.
            ftp.set_pasv(False)
            ftp.cwd(self.ftpDir)
            if(self.logger):
              self.logger.info("In dir: %s" % (ftp.pwd()))

            for fileName in ftpFileList:
              if(self.logger):
                self.logger.info("FTPing file: %s" % (fileName))
              fullFilePath = "%s/%s" % (baseFilepath, fileName)
              ftp.storlines("STOR " + fileName, open(fullFilePath, 'r'))
              
            if(self.logger):
              self.logger.info("Finished FTPing files.")
          except Exception, e:
            if(self.logger):
              self.logger.exception(e)
                
              
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
    paramSubs["start"] = self.startDate.strftime("%Y-%m-%d")
    paramSubs["end"] = self.endDate.strftime("%Y-%m-%d")    
    paramSubs['watershed'] = self.currentRegionName
    return(nexradProcess.buildOuputFilename(self, paramSubs))
    
class dateControlFile:
  def __init__(self, dateFilename,logger=True):
    self.logger = None
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)      
    self.sendDates = []
    try:
      dataSendFile = open(dateFilename, "r")
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      for line in dataSendFile:
        line = line.rstrip()
        if(len(line)):
          if(self.logger):
            self.logger.debug("Processing line: %s" % (line))
          sendDate = datetime.datetime.strptime(line, '%B %d, %Y')
          sendDate = sendDate.replace()
          self.sendDates.append(sendDate)
      
  
  def getDatesFromFile(self):
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
          return(sendDates)
    return(None)

  def getCurrentReportingPeriod(self, dateToCheck):
    i = 0
    endDate = None
    startDate = None
    datesListLen = len(self.sendDates) 
    while(i < datesListLen):
      testDate = self.sendDates[i] 
      #Dates are in chronological order, so once we our current date is greater or equal to the test date, we're at the period.
      if(dateToCheck.month <= testDate.month and dateToCheck.day <= testDate.day):
        #If we are at the beginning of the list we'll get an index of -1, that's acceptable in python as it translates
        #to the end of the list. 
        startDate = self.sendDates[i-1]
        endDate = self.sendDates[i]
        break        
      #At the end of the list with no match? Then we are between the last entry in the list and the first entry.
      elif((i+1) == datesListLen):
        startDate = self.sendDates[i]
        endDate = self.sendDates[0]
      i += 1
      
    return(startDate,endDate)

      
  #def getReportingDates(self, dateToCheck):
  def isReportingDay(self, dateToCheck):
    #Get the reporting period we are currently in.
    startDate, endDate = self.getCurrentReportingPeriod(dateToCheck)
    if(startDate and endDate):
      #Check to see if the dateToCheck matches the reporting day.
      if(endDate.month == dateToCheck.month and endDate.day == dateToCheck.day):
        return(True,startDate,endDate)
    return(False, startDate,endDate)
    """      
    if(self.sendDates):   
      i = 0
      endDate = None
      startDate = None
      while( i < len(self.sendDates)):
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
    """

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
        #Get the required ini settings.
        try:
          dbSettingsSection = watershed + '_databasesettings'
          #SQLite database file we save the data into and do our spatial processing on.
          nexradDbFile = configFile.get(dbSettingsSection, 'NexradDBFile')
          #THe spatial processing library for SQLite.
          spatialiteLib = configFile.get(dbSettingsSection,'SpatialiteLibPath')
          #Flag that specifies whether or not to delete all the records in the database. 
          cleanDB = bool(configFile.get(dbSettingsSection, 'CleanDB'))
          #Directory where the NEXRAD XMRG data files are for processing.
          nexradDataDir = configFile.get(watershed, 'NexradDataDir')
          #After processing, this flag specifies if we are to delete the XMRG file we just processed.
          removeRawNexradFiles = bool(configFile.get(watershed, 'RemoveRawDataFiles'))
          #After processing, this flag specifies if we are to delete the XMRG file we just processed.
          #removeCompressedNexradFiles = bool(configFile.get(watershed, 'RemoveCompressedDataFiles'))
          #File, either KML or CSV that contains the polygon(s) defining the watershed.
          watershedPolygonSrc = configFile.get(watershed, 'AreaPolygonFile')
          #If set, the data is output in inches, native is 100th of mm.
          outputInInches = bool(configFile.get(watershed, 'OutputInInches'))
          #While processing, this is the file the data is saved to. Each polygon in the watershed is processed, then
          #the result stored here for each hour processed. When we are on a reporting day, this file is then processed
          #per polygon and individual files created for each polygon.
          outputFilename = configFile.get(watershed, 'OutputFile')
          #Get the file that contains the dates to send out the data.
          dateControlFilename = configFile.get(watershed, 'dateControlFile')          
          
        except ConfigParser.Error, e:
          if(logger):
            logger.exception(e)
        else:
          try:
            #Get optional settings.
            importBBOX = None
            writeImportKMLFile = False
            startSummaryHour = '07:00:00'
            #Params that aren't required
            importBBOX = configFile.get(watershed, 'ImportBBOX')
            writeImportKMLFile = configFile.getboolean(watershed, 'WriteImportKMLFile')
            startSummaryHour = configFile.get(watershed, 'StartSummaryHour')
          except ConfigParser.Error, e:
            if(logger):
              logger.exception(e)
          
          #Make out database object and connect.
          db = xmrgDB()
          if(db.connect(nexradDbFile, spatialiteLib) != True):
            if(logger):
              logger.debug("Unable to connect to database: %s, cannot continue" % (nexradDbFile))   
              sys.exit(-1)

          #We want to check todays date against the control file.
          today = datetime.datetime.now()
          today = today.replace(hour=0,minute = 0,second = 0,microsecond = 0)
          
          checkDate = dateControlFile(dateControlFilename, True)          
          #startDate,endDate = checkDate.getReportingDates(today)
          reportDay, startDate, endDate = checkDate.isReportingDay(today)  
           
          nexradProc = horryCountyNexradProcess(bbox = importBBOX, 
                                     dbObj = db, 
                                     logger=True,
                                     outputFilename = outputFilename,
                                     outputInches = outputInInches,
                                     regionName=watershed,
                                     startSummaryHour=startSummaryHour,
                                     reportDay=reportDay,
                                     startReportPeriod=startDate,
                                     endReportPeriod=endDate)
          #If it is a reporting day, then getting the FTP settings. We transfer the results via FTP once the processing is 
          #completed.
          if(reportDay):
            try:
              ftpAddress = configFile.get(watershed, 'FtpHost')
              ftpDir = configFile.get(watershed, 'FtpDirectory')
              ftpUser = configFile.get(watershed, 'FtpUsername')
              ftpPwd = configFile.get(watershed, 'FtpPassword')              
            except ConfigParser.Error, e:
              if(logger):
                logger.exception(e)
            else:
              nexradProc.ftpSettings(address=ftpAddress, user=ftpUser, pwd=ftpPwd, directory=ftpDir)
              
          #Set flag on whether or not to create a KML file that contains the grids processed.
          if(writeImportKMLFile):
            nexradProc.writeKMLFile(writeImportKMLFile)
            #nexradProc.writeImportKMLFile = writeImportKMLFile
            
          #Check to see if we want to clean the database before processing.       
          if(cleanDB):
            nexradProc.cleanDB()                
          
          #Build the dictionary of polygons that represent the pieces inside the watershed.
          nexradProc.buildPolygonDictionary(watershedPolygonSrc)
          nexradProc.importFilesIntoDB(nexradDataDir, removeRawNexradFiles)
          
          if(logger):
            logger.info("Closing logfile.")
        
      
  except Exception, E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)
  
  sys.exit(retVal)