import os
import sys
import array
import struct
import csv
import time
import optparse
import logging
import logging.handlers
from collections import defaultdict  
from lxml import etree
from ftplib import FTP
from pysqlite2 import dbapi2 as sqlite3
from xeniatools.xmlConfigFile import xmlConfigFile
if(sys.platform == "win32"):
  sys.path.insert(0, "C:\Documents and Settings\dramage\workspace\BeachAdvisory")
from dhecDB import dhecDB

def procTraceback(excInfo):   
  import traceback
  if(excInfo == None):
    excInfo = sys.exc_info()
  exceptionType, exceptionValue, exceptionTraceback = excInfo

  excMsgs = traceback.format_exception(exceptionType, 
                                  exceptionValue,
                                  exceptionTraceback)
  return(excMsgs[0] + excMsgs[1] + excMsgs[2])


"""
Class: processTideData
Purpose: This class reads a file from the tidesandcurrents NOAA site and parses the data line by line.
In the future, I would like to expand this to directly query the website to get the data and save the file
automatically.
The data comes from: http://tidesandcurrents.noaa.gov/data_menu.shtml?stn=8661070%20Springmaid%20Pier,%20SC&type=Historic+Tide+Data
"""
class processTideData(object):
  def __init__(self):
    self.tideFile = None
    self.lineNo= 0

  def openFile(self, filePath):
    try:
      self.tideFilePath = filePath
      self.tideFile = open(filePath, 'r')
      return(True)
    except IOError, e:
      self.errorMsg = procTraceback(sys.exc_info())
    return(False)        
  
  def readLine(self):
    line = self.tideFile.readline()
    self.lineNo += 1
    if(len(line)):
      #Now break apart the pieces
      parts = line.split()
      stationID = ""
      date = ""
      level = None
      tideType = None
      if(len(parts) == 5):
        stationID = parts[0]
        date = "%s %s" %(parts[1],parts[2])
        date = time.strptime(date, '%Y%m%d %H:%M')
        date = time.strftime('%Y-%m-%dT%H:%M:00', date)
        level = float(parts[3])
        tideType = parts[4]
      return(stationID, date, level, tideType)
    else:
      return(None,None,None,None)
################################################################################################################  
"""
  Class: rainGaugeData
  Purpose: Simple class that represents a processed row from a dhec rain gauge file.
"""
class rainGaugeData:
  def __init__(self):
    self.ID = - 1
    self.dateTime = - 1
    self.batteryVoltage = - 1
    self.programCode = - 1
    self.rainfall = - 1
    self.windSpeed = - 1
    self.windDir = - 1
################################################################################################################  
"""
  Class: readRainGaugeData
  Purpose: Given a dhec rain gauge csv file, this class will process the file line by line.
"""
class readRainGaugeData:
    
  """
    Function: init
    Purpose: Initializes the class.
    Parameters: None
    Return: None
  """
  def __init__(self):
    self.lastErrorMsg = ''
    #Create and hook into the logger.
    self.logger = logging.getLogger("dhec_logger.readRainGaugeData")
    self.logger.info("creating an instance of readRainGaugeData")
    
  
  """
  Function: openFile
  Purpose: Opens the dhec csv file using a Python csv object.
  Parameters:
    filePath is the fully qualified path to the file we want to process
  Return: True if the file was successfully opened, otherwise false. If an exception is thrown,
    an error message will be in self.lastErrorMsg
  """
  def openFile(self, filePath):
    self.filePath = filePath
    try:
      #Open the file for reading in ascii mode.
      self.file = csv.reader(open(self.filePath, "rb"))      
    except csv.Error, e:
      self.lastErrorMsg = ('file %s, line %d: %s' % (filename, self.file.line_num, e))
      if(self.logger != None):
        self.logger.error( "Exception occured:", exc_info=1 )
      else:
        print(traceback.print_exc())
      return(False)
    return(True) 
  
  """
  Function: processLine
  Purpose: Reads and decodes a single row from the opened csv file. Returns a rainGaugeData class
    that contains the processed data. When the EOF is hit, a StopIteration exception is thrown. 
  Parameters: None
  Return: A rainGaugeData class. If an error occurs, this data will be set to None.
  """  
  def processLine(self):
    dataRow = rainGaugeData() 
    try:
      row = self.file.next()
      if(len(row) == 0):
        self.logger.debug('Empty row on line: %d moving to next row' % (self.file.line_num))
        return(dataRow)
      elif(len(row) < 7):
        self.logger.error("Row: '%s' does not have enough columns to process on line: %d moving to next row" % (row, self.file.line_num))
        return(dataRow)
      elif(len(row)>9):
        self.logger.error("Row: '%s' has too many columns to process on line: %d moving to next row" % (row, self.file.line_num))
        return(dataRow)        
      #1st entry is rain gauge ID
      if(len(row[0])):
          dataRow.ID = int(row[0])
      else:
        dataRow.ID = 0
        self.logger.error("ID field empty on line: %d in row: '%s'" % (self.file.line_num, row))
      #Array entries 1-3 are: Year, Julien day, time in military minutes. We convert these
      #into a datetime format. There are 2400 time entries, one of them is the days summary, the other
      #is another 10minute interval.   
      if(len(row[1]) and len(row[2]) and len(row[3])):   
        hour = int(row[3]) / 100    #Get the hours
        minute = int(row[3]) - (hour * 100) # Get the minutes
        #There are entries for the 2400 hour that are for the previous day, not the day that would start
        #at 0000. These rows are a 24 hour rainfall summary and one row is the final 10 minute sample for the day.
        if(hour == 24):
          hour = 23
          minute = 59
        datetime = "%d-%d %02d:%02d" % (int(row[1]), int(row[2]), hour, minute)
        datetime = time.strptime(datetime, '%Y-%j %H:%M')
        datetime = time.strftime('%Y-%m-%dT%H:%M:00', datetime)      
        dataRow.dateTime = datetime
      else:
        self.logger.error("Missing a date field on line: %d in row: '%s', moving to next row" % (self.file.line_num, row))
        return(dataRow)
        
      if(len(row[4])):
          dataRow.batteryVoltage = float(row[4])
      #else:
      #  self.logger.debug( "Battery voltage field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )

      if(len(row[5])):
        dataRow.programCode = float(row[5])
      #else:
      #  self.logger.debug( "Program Code field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )
      if(len(row[6])):
        dataRow.rainfall = float(row[6])
      else:
        self.logger.error("Rainfall field empty on line: %d in row: '%s'" % (self.file.line_num, row))
      #print( 'Processing line: %d %s' % ( self.file.line_num, row ) )
      #Are there more than rainfall values?
      if(len(row) > 7):
        if(len(row[7])):
          dataRow.windSpeed = float(row[7])         
        if(len(row[8])):
          dataRow.windDir = float(row[8])
      else:
        dataRow.windSpeed = None
        dataRow.windDir = None
        
      return(dataRow)
   
    except csv.Error, e:
      self.lastErrorMsg = ('File %s. Line %d: %s' % (self.filePath, self.file.line_num, e))
      if(self.logger != None):
        self.logger.error( "Exception occured:", exc_info=1 )
      else:
        print(traceback.print_exc())
            
    return(dataRow)
################################################################################################################  
################################################################################################################  
class dhecConfigSettings(xmlConfigFile):
  def __init__(self, xmlConfigFilename):
    try:
      #Call parents __init__
      xmlConfigFile.__init__(self, xmlConfigFilename)
      
      #Log file settings
      self.logFile = self.getEntry('//logging/logDir')
      self.maxBytes = self.getEntry('//logging/maxBytes')
      if(self.maxBytes == None):
        self.maxBytes = 100000
      else:
        self.maxBytes = int(self.maxBytes)
        
      self.backupCount = self.getEntry('//logging/backupCount')
      if(self.backupCount == None):
        self.backupCount = 5
      else:
        self.backupCount = int(self.backupCount)
      
      #DB settings
      self.dbSettings = self.getDatabaseSettings()
      #This is the root file path where the rollover/backup database is to be created.
      self.dbBackupFile = self.getEntry('//environment/database/db/backup/filePath')
      #For non-existent db's, this is the path to the SQL statement that will create the schema.
      self.dbBackupSQLSchemaFile= self.getEntry('//environment/database/db/backup/sqlSchemaFile')
      
      #Directory where to store rain gauge files.
      self.rainGaugeFileDir = self.getEntry('//rainGaugeProcessing/rainGaugeFileDir')
      
      #FTP address to pull rain gauge files from.
      self.rainGaugeFTPAddy = self.getEntry('//rainGaugeProcessing/ftp/ip')
      #FTP login settings.
      self.rainGaugeFTPUser = self.getEntry('//rainGaugeProcessing/ftp/user')
      self.rainGaugeFTPPwd = self.getEntry('//rainGaugeProcessing/ftp/passwd')
      self.rainGaugeFTPDir = self.getEntry('//rainGaugeProcessing/ftp/fileDir')
      
      #Flag that specifies if logfiles should be deleted after processing.
      self.delServerFiles = self.getEntry('//rainGaugeProcessing/ftp/delServerFile')
      if(self.delServerFiles != None):
        self.delServerFiles = int(self.delServerFiles)
      else:
        self.delServerFiles = 0
      
      #File path for KML file creation.
      self.kmlFilePath = self.getEntry('//rainGaugeProcessing/outputs/kml/filePath')    
  
      self.spatiaLiteLib = self.getEntry('//database/db/spatiaLiteLib')
    except Exception, e:
      print('ERROR: ' + str(e) + ' Terminating script')
      sys.exit(- 1)

    
  
################################################################################################################  
"""
Class: processDHECRainGauges
Purpose: Given a list of dhec rain gauge files, this class will process them all and store the results into
  the database connected to the dbConnection parameter passed in the __init__ function.
"""
class processDHECRainGauges:
  """
  Function: __init__
  Purpose: Initializes the class. 
  Parameters:
    workingFileDir is a path to the directory where the dhec csv files live.
    dbConnection is a connected connection to the database we are going to store the data in.
  """
  def __init__(self, xmlConfigFile):
    self.lastErrorFile = ''    
    self.lastErrorLineNo = ''    
    self.lastErrorFunc = ''    
    
    self.totalLinesProcd = 0        #Total number of lines processed from all files.
    self.totalLinesUnprocd = 0      #Total number of lines unable to be processed for some reason/
    self.totalTime = 0.0            #Total execution time.
    try:
      #xmlTree = etree.parse(xmlConfigFile)
      self.configSettings = dhecConfigSettings(xmlConfigFile)
      
      #Create our logging object.
      if(self.configSettings.logFile == None):
        print('ERROR: //logging/logDir not defined in config file. Terminating script')
        sys.exit(- 1)     

      self.logger = logging.getLogger("dhec_logger")
      self.logger.setLevel(logging.DEBUG)
      # create formatter and add it to the handlers
      formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(lineno)d,%(message)s")

      #Create the log rotation handler.
      handler = logging.handlers.RotatingFileHandler(self.configSettings.logFile, "a", self.configSettings.maxBytes, self.configSettings.backupCount)
      handler.setLevel(logging.DEBUG)
      handler.setFormatter(formatter)    
      self.logger.addHandler(handler)
      # add the handlers to the logger
      self.logger.info('Log file opened')
      
      if(self.configSettings.dbSettings['dbName'] == None):
        self.logger.error('ERROR: //database/db/name not defined in config file. Terminating script')
        sys.exit(- 1)                      
      self.logger.debug('Database path: %s' % (self.configSettings.dbSettings['dbName']))
      self.db = dhecDB(self.configSettings.dbSettings['dbName'],"dhec_logger")
            
      #Get a file list for the directory.
      if(self.configSettings.rainGaugeFileDir == None):
        self.logger.error('ERROR: //rainGaugeProcessing/rainGaugeFileDir not defined in config file. Terminating script')
        sys.exit(- 1)      
      
      self.logger.debug('Directory for rain gauge data: %s' % self.configSettings.rainGaugeFileDir)
      
      #Check the settings for ftping the rain gauge data
      if(self.configSettings.rainGaugeFTPAddy == None):       
        self.logger.error('ERROR: //rainGaugeProcessing/ftp/ip not defined in config file. Terminating script')
        sys.exit(- 1)      
      if(self.configSettings.rainGaugeFTPUser == None): 
        self.logger.error('ERROR: ///rainGaugeProcessing/ftp/user not defined in config file. Terminating script')
        sys.exit(- 1)      
      if(self.configSettings.rainGaugeFTPPwd == None):
        self.logger.error('ERROR: //rainGaugeProcessing/ftp/passwd not defined in config file. Terminating script')
        sys.exit(- 1)      
      if(self.configSettings.rainGaugeFTPDir == None):
        self.logger.error('ERROR: //rainGaugeProcessing/ftp/fileDir not defined in config file. Terminating script')
        sys.exit(- 1)      
      if(self.configSettings.delServerFiles == None):
        self.logger.error('ERROR: //rainGaugeProcessing/ftp/delServerFile not defined in config file. Terminating script')
        sys.exit(- 1)      
        
      self.logger.debug('Raingauge FTP Info: IP: %s User: %s Pwd: %s Dir: %s Delete Server Files: %d' % 
                         (self.configSettings.rainGaugeFTPAddy, self.configSettings.rainGaugeFTPUser, self.configSettings.rainGaugeFTPPwd, self.configSettings.rainGaugeFTPDir, self.configSettings.delServerFiles))
      
      if(self.configSettings.kmlFilePath == None):
        self.logger.error('ERROR: //rainGaugeProcessing/outputs/kml/filePath, cannot output KML file')
        
      self.emailList = []
      emailList = self.configSettings.getEntry('//rainGaugeProcessing/alert/emailList')
      if(emailList != None):
        #List of email addresses to send the alert to.
        emailList = emailList.split(',')
        for email in emailList:
          self.emailList.append(email) 
        val = self.configSettings.getEntry('//rainGaugeProcessing/alert/lagTimeAlert')
        if(val != None):
          #This is the number of hours we can miss updates for the rain gauge data before we send an email alert.
          #Convert it into seconds.
          self.lagAlertTime = (float(val) * 3600.0)
        else:
          self.logger.error("ERROR: //rainGaugeProcessing/alert/lagTimeAlert missing, cannot send email alert if data is missing")      
      else:
        self.logger.debug("//rainGaugeProcessing/alert/emailList missing, cannot send email alert if data is missing")
                              
    except OSError, e:
      print('ERROR: ' + str(e) + ' Terminating script')      
      sys.exit(- 1)
    except Exception, e:
      print('ERROR: ' + str(e) + ' Terminating script')
      sys.exit(- 1)
  """
  Function: __del__
  Purpose: Destructor. Used to make sure the logger gets completely shutdown. 
  """
  def __del__(self):
    #Cleanup the logger.
    if(self.logger != None):
      logging.shutdown()
      
  def procTraceback(self,excInfo):   
    import traceback
    if(excInfo == None):
      excInfo = sys.exc_info()
    exceptionType, exceptionValue, exceptionTraceback = excInfo
  
    excMsgs = traceback.format_exception(exceptionType, 
                                    exceptionValue,
                                    exceptionTraceback)
    return(excMsgs[0] + excMsgs[1] + excMsgs[2])
      
  """
  Function: setFileList
  Purpose: Allows us to override the fileList of csv files to process. 
  Parameters:
    fileList is a list of csv files to process.
  """
  def setFileList(self, fileList):
    self.fileList = fileList  
    
  """
  Function: deleteRainGaugeDataFiles
  Purpose: Deletes the rain gauge files in the rain gauge dir.
  Parameters:
  """
  def deleteRainGaugeDataFiles(self):
    self.fileList = os.listdir(self.configSettings.rainGaugeFileDir)      
    try:
      for file in self.fileList:
        fullPath = self.configSettings.rainGaugeFileDir + file
        
        #Make sure we are trying to delete a file and not a directory.
        if(os.path.isfile(fullPath) != True):
          self.logger.debug("%s is not a file, skipping" % (fullPath))
          continue  
        else:
          os.remove(fullPath)
          self.logger.debug("Deleted rain gauge file: %s" % (fullPath))
          
    except Exception, e:
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)
          
        
  """
  Function: ftpRainGaugeData
  Purpose: FTPs the rain gauge csv files onto our local machine for processing. 
  Parameters:
  """
  def ftpRainGaugeData(self):
    try:      
      ftp = FTP(self.configSettings.rainGaugeFTPAddy)
      ftp.login(self.configSettings.rainGaugeFTPUser, self.configSettings.rainGaugeFTPPwd)
      ftp.cwd(self.configSettings.rainGaugeFTPDir)
      #Get a list of the files in the dir
      fileList = ftp.nlst()
      for file in fileList:
        if(file.find('.csv') > 0):
          Filename = self.configSettings.rainGaugeFileDir + file         
          outFile = open(Filename, 'wt')
          
          startTime = 0;
          if(sys.platform == 'win32'):
            startTime = time.clock()
          else:
            startTime = time.time()            
          #Download the file into the local file.
          ftp.retrlines("RETR " + file, lambda s, w=outFile.write: w(s + "\n"))

          if(sys.platform == 'win32'):
            endTime = time.clock()
          else:
            endTime = time.time()
          
          self.logger.debug("FTPd file: %s to %s in %.2f ms" % (file, Filename, (endTime - startTime) * 1000.0))          
          outFile.close()
          if(self.configSettings.delServerFiles):
            ftp.delete(file)
          
        else:
          self.logger.debug("File: %s is not a csv file" % (file))
        
      return(True)
      
    except Exception, e:
      msg = self.procTraceback()
      self.logger.critical(msg)
    return(False)
   
  """
  Function: processFiles
  Purpose: Loops through the fileList and processes the dhec data files. The data is then stored
    into the database.
  Parameters:None
  Return:    
  """
  def processFiles(self):        
    fileProcdCnt = 0
    try:
      self.logger.info("Begin rain gauge file processing.")
      self.fileList = os.listdir(self.configSettings.rainGaugeFileDir)      
      for file in self.fileList:
        self.linesSkipped = 0
        self.dbRowsNotInserted = 0
        startTime = 0.0
        if(sys.platform == 'win32'):
          startTime = time.clock()
        else:
          startTime = time.time()
        try:
          fullPath = self.configSettings.rainGaugeFileDir + file
          
          #self.logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
          #Make sure we are trying to process a file and not a directory.
          if(os.path.isfile(fullPath) != True):
            self.logger.debug("%s is not a file, skipping" % (fullPath))
            continue
            
          self.logger.info("Begin processing file: %s" % fullPath)
          rainGaugeFile = readRainGaugeData()
          rainGaugeFile.openFile(fullPath)
          dataRow = rainGaugeFile.processLine()
          #Get the row id and the summary id.
          rainGaugeId = file.split('.')
          self.writeSummaryTable = False
          while(dataRow != None):
            if(dataRow.ID > 0):
              #DHEC doesn't reset the time on the rain gauges to deal with daylight savings.
              #Since we want to store the data using GMT times, we need to correct when we aren't in 
              #DST.
              dataTime = time.strptime(dataRow.dateTime, '%Y-%m-%dT%H:%M:%S')
              dataTime = time.mktime(time.strptime(dataRow.dateTime, '%Y-%m-%dT%H:%M:%S'))              
              #If it is DST, we need to add an hour to get the correct time when we convert to GMT, otherwise
              #we'll be an hour off.             
              if(time.localtime(dataTime)[-1]):
                #3600 = number of seconds in 1 hour.
                dataTime += 3600 
              dataRow.dateTime = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(dataTime))      
                
              #The idea behind this is that there are 2 ID types in the data. One with a signature of xx1 is a normal
              #10 minute interval sample, one with an xx2 signature is the 24 hour summary. So if the first bit is
              #set, my assumption is that it's a 10 minute sample.
              updateType = dataRow.ID & 1
              if(updateType == 1):
                if(self.db.writePrecip(dataRow.dateTime, rainGaugeId[0].lower(), dataRow.batteryVoltage, dataRow.programCode, dataRow.rainfall, dataRow.windSpeed, dataRow.windDir) == False):
                  self.logger.error('Failed to write the precipitation data into the database. File Line: %d' % rainGaugeFile.file.line_num)
                  self.dbRowsNotInserted += 1                           
              elif(updateType == 0):
                if(self.db.write24HourSummary(dataRow.dateTime, rainGaugeId[0].lower(), dataRow.rainfall) == False):
                  self.logger.error('Failed to write the summary precipitation data into the database. File Line: %d' % rainGaugeFile.file.line_num)
                  self.dbRowsNotInserted += 1                           
              else:
                  self.logger.error('File Line: %d ID: %d is not valid' % (rainGaugeFile.file.line_num, dataRow.ID))
                  self.linesSkipped += 1                           
            else:
              self.logger.error('No record processed from line: %d' % rainGaugeFile.file.line_num)
              self.linesSkipped += 1                           
                                      
            dataRow = rainGaugeFile.processLine()
         
        except StopIteration, e:
          if(self.linesSkipped):
            self.logger.error('Unable to process: %d lines out of %d lines' % (self.linesSkipped, rainGaugeFile.file.line_num))
            self.totalLinesUnprocd += self.linesSkipped
          else:
            self.logger.info('Total lines processed: %d' % rainGaugeFile.file.line_num)
          self.logger.info('EOF file: %s.' % file)
          self.totalLinesProcd += rainGaugeFile.file.line_num
          
        fileProcdCnt += 1
        try:
          #self.db.writeJunkTest( '00-00-00T00:00:00', 'TEST',-1,-1,-1)
          #Commit all the entries into the database for this file.
          self.logger.debug('Committing SQL inserts')
          self.db.commit()
          if(self.dbRowsNotInserted):
            self.logger.error('Unable to insert: %d rows into the database.' % self.dbRowsNotInserted)
          endTime = 0.0
          if(sys.platform == 'win32'):
            endTime = time.clock()
          else:
            endTime = time.time()
          self.logger.debug('Total time to process file: %f msec' % ((endTime - startTime) * 1000.0))
          #self.logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
         
          self.totalTime += (endTime - startTime) * 1000.0
        except sqlite3.Error, e:
          self.logger.critical(e.args[0] + ' Terminating execution')
          sys.exit(- 1)
    except Exception, e:
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)
    
    #Log various statistics out.
    self.logger.info("Final Statistics Rain Gauge Processing")
    self.logger.info("Finished processing file list. Processed: %d of %d files." % (fileProcdCnt, len(self.fileList)))
    if(self.totalLinesUnprocd):
      self.logger.error("Unable to process: %d of %d lines." % (self.totalLinesUnprocd, self.totalLinesProcd))      
    else:
      self.logger.debug("Total Lines Processed: %d" % (self.totalLinesProcd))
    if(self.db.rowErrorCnt):
      self.logger.error("Unable to insert: %d of %d rows into the database" % (self.db.rowErrorCnt, self.db.totalRowsProcd))
    else:
      self.logger.debug("Inserted %d rows into the database" % (self.db.totalRowsProcd))
      
    self.logger.debug("Total Processing Time: %f msecs" % (self.totalTime))
    
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """  
  def vacuumDB(self):
    db = dhecDB(self.configSettings.dbSettings['dbName'], 'dhec_logger')
    if(db.vacuumDB() != None):
      self.logger.debug("Vacuumed database.")
      return(True)
    else:
      self.logger.error("Database vacuum failed: %s" % (db.lastErrorMsg))
      db.lastErrorMsg = ""
    return(False)
      
    
  
  """
  Function: checkDataFlow
  Purpose: Checks the rain gauges to see if the last date/time entry is within the time interval specified in the log
  file. Used to alert people on the email list if we don't seem to be getting the data files.
  Config file entries need:
  <rainGaugeProcessing>
    <alert>
      <lagTimeAlert></lagTimeAlert>       Number of hours data not collected for a given rain gauge to send alert.
      <emailList></emailList>             Comma delimited list of email addresses to send alert to.
    </alert>
  </rainGaugeProcessing>  
  """
  def checkDataFlow(self):
    sendAlertEmail = False
    if(len(self.emailList)):
      sendAlertEmail = True
    dbCursor = self.db.getRainGauges()
    if(dbCursor != None):
      emailMsg = ''
      rainGaugeList = ''
      #To get the current epoch time in our time zone, we use the timezone value + the dstOffset.
      #timezone does not take daylight savings into account, so we manually do that.
      isdst = time.localtime()[ - 1]
      dstOffset = 0
      if(isdst):
        dstOffset = 3600
      curEpochTime = (time.time())                 
      for row in dbCursor:
        sql = "SELECT max(date) AS date from precipitation WHERE rain_gauge = '%s'" % (row['name'])
        dateCursor = self.db.dbCon.cursor()
        dateCursor.execute(sql)
        dateRow = dateCursor.fetchone() 
        if(dateRow != None):          
          dataEpochTime = time.mktime(time.strptime(dateRow['date'], '%Y-%m-%dT%H:%M:%S'))
          dif = curEpochTime - dataEpochTime
          if(dif > self.lagAlertTime):
            if(sendAlertEmail):
              rainGaugeList += "<li>Rain Gauge: %s last entry date: %s is older than %f hour</li>" % (row['name'], dateRow['date'], (self.lagAlertTime / 3600.0))   
            self.logger.error("Rain Gauge: %s last entry date: %s is older than %4.1f hours" % (row['name'], dateRow['date'], (self.lagAlertTime / 3600.0)))            
        dateCursor.close()
      dbCursor.close()
      if(len(rainGaugeList)):
        import smtplib
        from email.MIMEMultipart import MIMEMultipart
        from email.MIMEText import MIMEText   
        emailMsg = "<ul>%s</ul>" % (rainGaugeList)
        SERVER = "inlet.geol.sc.edu"  
        FROM = "dan@inlet.geol.sc.edu"
        TO = self.emailList     
        BODY = emailMsg      
        message = ("MIME-Version: 1.0\r\nContent-type: text/html; \
        charset=utf-8\r\nFrom: %s\r\nTo: %s\r\nSubject: DHEC Rain Gauge Alert\r\n" % 
        (FROM, ", ".join(TO))) + BODY        
        # Send the mail
        try:   
          server = smtplib.SMTP(SERVER)
          server.sendmail(FROM, TO, message)
          server.quit()       
          self.logger.debug("Sending alert email.")
        except Exception, E:
          msg = self.procTraceback()
          self.logger.critical(msg)      
  
        
  """
  Function: writeKMLFile
  Purpose: Creates a KML file with the latest hour, 24, and 48 hour summaries.
  """
  def writeKMLFile(self):
    from pykml import kml

    if(self.configSettings.kmlFilePath != None):
      try:
        self.logger.debug("Creating DHEC rain gauge KML file: %s" % (self.configSettings.kmlFilePath))
        rainGaugeKML = kml.KML()
        doc = rainGaugeKML.createDocument("DHEC Rain Gauges")
        #DHEC doesn't reset the time on the rain gauges to deal with daylight savings, so if
        #we are in DST then we need to subtract one from the time to get the last hour.
        isdst = time.localtime()[-1]
        dstOffset = 0
        if(isdst):
          dstOffset = 1
        curTime = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())
        dbCursor = self.db.getRainGauges()
        if(dbCursor != None):
          for row in dbCursor:
            last1 = self.db.getLastNHoursSummary(curTime, row['short_name'], (1 + dstOffset))  #Get last hours summary
            if(last1 == - 1.0):
              last1 = 'Data unavailable'
            else:
              last1 = ('%4.2f inches') % (last1) 
            last24 = self.db.getLastNHoursSummary(curTime, row['short_name'], (24 + dstOffset)) #Get last 24 hours summary
            if(last24 == - 1.0):
              last24 = 'Data unavailable'
            else:
              last24 = ('%4.2f inches') % (last24) 
            last48 = self.db.getLastNHoursSummary(curTime, row['short_name'], (48 + dstOffset)) #Get last 48 hours summary
            if(last48 == - 1.0):
              last48 = 'Data unavailable'
            else:
              last48 = ('%4.2f inches') % (last48) 
            curTime = curTime.replace("T", " ")
            platformDesc = row['description']
            desc = "<table><tr>Location: %s</tr>\
                    <tr><ul>\
                    <li>Date/Time: %s</li>\
                    <li>Last Hour: %s</li>\
                    <li>Last 24 Hours: %s</li>\
                    <li>Last 48 Hours: %s</li></ul></table>"\
                    % (row['description'], curTime, last1, last24, last48)
            pm = rainGaugeKML.createPlacemark(row['short_name'], row['fixed_latitude'], row['fixed_longitude'], desc)
            doc.appendChild(pm)
        rainGaugeKML.root.appendChild(doc)  
        kmlFile = open(self.configSettings.kmlFilePath, "w")
        kmlFile.writelines(rainGaugeKML.writepretty())
        kmlFile.close()
      except Exception, e:
        msg = self.procTraceback()
        self.logger.critical(msg)      
        sys.exit(-1)
    else:
      self.logger.error("Cannot write KML file, no filepath provided in config file.")
  """
  Function: backupData
  Purpose: Rollover precipitation data in the live database older than 30 days into the backup database.
  """    
  def backupData(self):
    self.logger.info("Beginning data backup/rollover.")
    
    curYear = time.strftime('%Y', time.localtime())
    backupDB = None
    filePath = "%s%s/" % (self.configSettings.dbBackupFile, curYear)
    backupFilename = "%s%s-dhec.db" % (filePath, curYear)
    ################################################################################
    #Test to see if the database exists.
    if(not os.path.isfile(backupFilename)):    
      #Check to see if the directory exists
      if(not os.path.exists(filePath)):
        os.mkdir(filePath)
        self.logger.info("Directory: %s does not exist, creating." % (filePath))
      
      if(self.configSettings.dbBackupSQLSchemaFile == None):  
        self.logger.info("File: %s does not exist, cannot continue." % (backupFilename))
        return;
      #We've got a SQL file to create the schema with.
      else:
        backupDB = dhecDB(backupFilename, "dhec_logger")
        backupDB.DB.close()
        shellCmd = "sqlite3 \"%s\" < \"%s\""%(backupFilename,self.configSettings.dbBackupSQLSchemaFile)
        ret = os.system(shellCmd)
        self.logger.debug("Created database: %s with schema file: %s" %(backupFilename,self.configSettings.dbBackupSQLSchemaFile))
    else:
      backupDB = dhecDB(backupFilename, "dhec_logger")
      self.logger.info("Connecting to database: %s" % (backupFilename))

    sys.exit(-1)

    ################################################################################
      
    ################################################################################
    #On a platform by platform basis, get all data that is not in the current month.    
    dbCursor = self.db.getRainGauges()
    gaugeList = []
    for row in dbCursor:    
      gaugeList.append(row['platform_handle'])
    dbCursor.close()
    #Cutoff date, we want to keep last 30 days.      
    cutoffDate = time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time() - (30 * 24 * 60 * 60)))
    for platformHandle in gaugeList:
      #Get the m_types for the sensors we will roll over into the backup database.
      #(self, obsName, uom, platform, sOrder=1 ):
      mTypeList = '('
      precipMType = self.db.getMTypeFromObsName( "precipitation", "in", platformHandle, 1)
      mTypeList += "m_type_id=%d "%(precipMType)
      precipDailySum = self.db.getMTypeFromObsName( "precipitation_accumulated_daily", "in", platformHandle, 1)
      if(precipDailySum != None):
        mTypeList += "OR m_type_id=%d "%(precipDailySum)       
      windSpd = self.db.getMTypeFromObsName( "wind_speed", "mph", platformHandle, 1)
      if(windSpd != None):
        mTypeList += "OR m_type_id=%d "%(windSpd)
      windDir = self.db.getMTypeFromObsName( "wind_from_direction", "degrees_true", platformHandle, 1)
      if(windDir != None):
        mTypeList += "OR m_type_id=%d"%(windDir)
      
      if( len(mTypeList) <= 0 ):
        self.logger.error("No m_type_ids found for platform: %s" % (platformHandle))
        return
      else:
        mTypeList += ") AND "
      self.logger.info("Processing multi_obs table data for platform: %s" % (platformHandle))
      sql = "SELECT * FROM multi_obs\
             WHERE\
             m_date < '%s' AND\
             %s\
             platform_handle='%s' ORDER BY m_date ASC"\
             %(cutoffDate, mTypeList, platformHandle) 
      resultsCursor = self.db.executeQuery(sql)
      rowCnt = 0
      if(resultsCursor != None):
        for item in resultsCursor:
          obsName = ''
          uom = ''
          mVals = []
          mVals.append(float(item['m_value']))
          if( int(item['m_type_id']) == precipMType ):
            obsName = 'precipitation'
            uom = 'in'
            mVals.append(float(item['m_value_2']))
            mVals.append(float(item['m_value_3']))
          elif( int(item['m_type_id']) == precipDailySum ):
            obsName = 'precipitation_accumulated_daily'
            uom = 'in'
          elif( int(item['m_type_id']) == windSpd ):
            obsName = 'wind_speed'
            uom = 'mph'
          elif( int(item['m_type_id']) == windDir ): 
            obsName = 'wind_from_direction'
            uom = 'degrees_true'
          if(backupDB.addMeasurement(obsName, uom,
                                     platformHandle,
                                     item['m_date'],
                                     item['m_lat'], item['m_lon'],
                                     0,
                                     mVals,
                                     1,
                                     False) != True):
            self.logger.critical( "%s Function: %s Line: %s File: %s"\
                               %(backupDB.lastErrorMsg,backupDB.lastErrorFunc, backupDB.lastErrorLineNo, backupDB.lastErrorFile) )
            return
          
          rowCnt += 1               
        if(not backupDB.commit()):
          self.logger.error(backupDB.lastErrorMsg)
          sys.exit(- 1)
        self.logger.info("Successfully processed and committed: %d rows into backup." % (rowCnt))
        resultsCursor.close()
        
        #Now we delete the records from the source DB.
        self.logger.info("Deleting backed up records from source database.")
        sql = "DELETE FROM multi_obs WHERE m_date < '%s' and platform_handle='%s'" % (cutoffDate, platformHandle)
        resultsCursor = self.db.executeQuery(sql)
        if(resultsCursor != None):
          if(not self.db.commit()):
            self.logger.error(self.db.lastErrorMsg)
            sys.exit(- 1)
        else:
          self.logger.error(self.db.lastErrorMsg)
          sys.exit(- 1)
        resultsCursor.close()
        
    self.logger.info("Finished data backup/rollover.")

  def checkForPlatformAndSensor(self, orgInfo, platformInfo, sensorList, addUOMandSensor=False):
    try:
      id = self.db.platformExists(platformInfo['platform_handle'])
      #Error occured.
      if(id == None):
        self.logger.error("DB Error: %s Code: %s" % (self.db.self.lastErrorMsg, self.db.lastErrorCode))
      #Platform doesn't exist, let's add it.
      elif(id == - 1):
        info = {}
        #Check to see if the organization exists.
        orgID = self.db.organizationExists(orgInfo["short_name"]);
        if(orgID == - 1):
          #info['active'] = 1
          orgID = self.db.addOrganization(orgInfo)
          if(orgID != None):
            self.logger.info("Successfully added organization: %s to database\n" % (info['short_name']))
          else:
            self.logger.critical("Error adding organization: %s to database.\nError: %s" % (info['short_name'], self.db.lastErrorMsg))
            sys.exit(- 1)
  
        platformInfo['organization_id'] = orgID
        id = self.db.addPlatform(platformInfo)
        if(id != None):
          self.logger.info("Successfully added platform: %s to database\n" % (platformInfo['platform_handle']))
        else:
          self.logger.critical("Error adding platform: %s to database.\nError: %s" % (platformInfo['platform_handle'], self.db.lastErrorMsg))
          sys.exit(- 1)
      if(len(sensorList)):
        for sensorInfo in sensorList:
          obsID = self.db.sensorExists(sensorInfo['short_name'], sensorInfo['uom'], platformInfo['platform_handle'], 1)
          if(obsID == - 1):
            m_type = None
            if('m_type' in sensorInfo):
              m_type = int(sensorInfo['m_type'])
            obsID = self.db.addSensor(sensorInfo['short_name'], sensorInfo['uom'], platformInfo['platform_handle'], 1, 0, 1, m_type, addUOMandSensor)
            if(obsID != None):
              self.logger.info("Successfully added obs: %s on platform: %s to database\n" % ('precipitation', platformInfo['platform_handle']))
            else:
              self.logger.critical("Error adding obs: %s on platform: %s to database\nError: %s" % ('precipitation', platformInfo['platform_handle'], self.db.lastErrorMsg))
              sys.exit(- 1)
    except Exception, e:
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)

  def importBacteriaData(self, bacteriaFilename, addStationSummary=False):
    try:
      inDataFile = open(bacteriaFilename, "r")
      row = 0
      #Get the stations we want to process.
      dbCursor = self.db.getPlatforms("WHERE platform_handle LIKE '%monitorstation%'")
      if(dbCursor == None):
        self.logger.error("No stations returned, cannot continue.")
        sys.exit(-1)
      stationList = []
      for platform in dbCursor:
        stationList.append(platform['short_name'])
      
      self.logger.debug("Beginning import of file: %s" %(bacteriaFilename))      
      line = inDataFile.readline()
      while(len(line)):
        if( row == 0):
          header = line
        else:
          line = line.split(',')
          if(len(line) < 13):
            print("Not enough columns: %d in row: %d, skipping." %(len(line), row))
            continue
          colCnt = 0
          #THe column structure in the file is the following:
          #LIMS Number,Station,Inspection Date,Insp Time,Lab Number,Inspection Type,E Sign,ETCOC,Salinity,Rainfall,Tide,Wind/Curr,Weather

          #Strings in the file are all quoted, so we use the .lstrip() and .rstrip to get rid of the quotes
          lims = (line[0].lstrip("\"")).rstrip("\"")          
          station = (line[1].lstrip("\"")).rstrip("\"")
          if(station in stationList):
            date = (line[2].lstrip("\"")).rstrip("\"")
            #time is in military time, for example 12:00 is represented 1200.
            mtime = (line[3].lstrip("\"")).rstrip("\"")
            date = time.strptime(date, "%d-%b-%Y")
            date = time.strftime("%Y-%m-%d", date)
            
            lab = (line[4].lstrip("\"")).rstrip("\"")
            inspType = (line[5].lstrip("\"")).rstrip("\"")
            esign = (line[6].lstrip("\"")).rstrip("\"")
            etcoc = (line[7].lstrip("\"")).rstrip("\"")
            if(len(etcoc) == 0):
              etcoc = 'NULL'
            salinity = (line[8].lstrip("\"")).rstrip("\"")
            if(len(salinity) == 0):
              salinity = 'NULL'
            rain = (line[9].lstrip("\"")).rstrip("\"")
            if(len(rain) == 0):
              rain = 'NULL'
            tide = (line[10].lstrip("\"")).rstrip("\"")
            wind = (line[12].lstrip("\"")).rstrip("\n").rstrip("\"")
            wind = (wind.lstrip("'")).rstrip("'")
            if(len(wind) == 0):
              wind = 'NULL'
            weather = (line[12].lstrip("\"")).rstrip("\n").rstrip("\"")
            weather = (weather.lstrip("'")).rstrip("'")
            if(len(weather) == 0):
              weather = 'NULL'
            self.logger.debug("Saving station: %s Date/Time: %s %s" %(station, date, mtime))
            sql = "INSERT INTO dhec_beach\
                   (lims_number,station,insp_date,insp_time,lab_number,insp_type,e_sign,etcoc,salinity,rainfall,tide,wind_curr,weather)\
                   VALUES('%s','%s','%s','%s','%s','%s','%s',%s,%s,%s,'%s',%s,%s)"\
                   %(lims, station, date, mtime, lab, inspType, esign, etcoc, salinity, rain, tide, wind, weather)
            dbCursor = self.db.executeQuery(sql)
            if(dbCursor != None):
              self.db.commit()
              dbCursor.close()
            #Log error
            else:
              self.logger.critical("%s" %(self.db.getErrorInfo()))          
            if(addStationSummary):
              self.db.writeSummaryForStation(date, mtime, station, False)
        row += 1
        line = inDataFile.readline()
        
      self.logger.debug("Processed: %d lines from the file: %s" %(row,bacteriaFilename))
      if(addStationSummary):
        self.db.commit()
    except Exception, e:
      self.logger.critical("Error: ", exc_info=1)
      return(False)
    return(True)
################################################################################################################  

if __name__ == '__main__':
  try:
    import psyco
    psyco.full()
        
    parser = optparse.OptionParser()
    parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                      help="Configuration file." )
    parser.add_option("-t", "--TideFilePath", dest="tideFilePath",
                      help="Import a tide file from the given filepath." )
    parser.add_option("-b", "--BacteriaDataFile", dest="bacteriaDataFile",
                      help="Import a csv bacteria file from the given filepath." )
    parser.add_option("-s", "--AddStationSummaryEntry", dest="addStationSummaryEntry", action= 'store_true',
                      help="While importing a bacteria file, this flag specifies to create a station summary entry also." )
    (options, args) = parser.parse_args()
    if( options.xmlConfigFile == None ):
      parser.print_usage()
      parser.print_help()
      sys.exit(-1)

    dhecData = processDHECRainGauges(options.xmlConfigFile)
    
    if(options.tideFilePath != None and len(options.tideFilePath)):
      dhecData.db.importTideFile(options.tideFilePath)
    
    elif(options.bacteriaDataFile != None and len(options.bacteriaDataFile)):
      addSummary = False
      if(options.addStationSummaryEntry != None and options.addStationSummaryEntry):
        addSummary = True
      dhecData.importBacteriaData(options.bacteriaDataFile, addSummary)
      
      
  except Exception, E:
    import traceback
    print( traceback.print_exc() )
    
  
  
