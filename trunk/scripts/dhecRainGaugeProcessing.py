import os
import sys
import array
import struct
import csv
import time
import logging
import logging.handlers
from collections import defaultdict  
from lxml import etree
from ftplib import FTP
from pysqlite2 import dbapi2 as sqlite3
from xeniatools.xmlConfigFile import xmlConfigFile


################################################################################################################  
"""
  Class: rainGaugeData
  Purpose: Simple class that represents a processed row from a dhec rain gauge file.
"""
class rainGaugeData:
  def __init__(self):
    self.ID = -1
    self.dateTime = -1
    self.batteryVoltage = -1
    self.programCode = -1
    self.rainfall = -1
    self.windSpeed = -1
    self.windDir  = -1
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
      self.file = csv.reader(open( self.filePath, "rb" ))      
    except csv.Error, e:
      self.lastErrorMsg = ('file %s, line %d: %s' % (filename, self.file.line_num, e))
      self.logger.error( self.lastErrorMsg )
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
      if( len(row) == 0 ):
        self.logger.debug( 'Empty row on line: %d moving to next row' % ( self.file.line_num ) )
        return( dataRow )
      elif( len(row) < 7 ):
        self.logger.error( "Row: '%s' does not have enough columns to process on line: %d moving to next row" % ( row, self.file.line_num ) )
        return( dataRow )
      #1st entry is rain gauge ID
      if( len(row[0])):
          dataRow.ID = int(row[0])
      else:
        dataRow.ID = 0
        self.logger.error( "ID field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )
      #Array entries 1-3 are: Year, Julien day, time in military minutes. We convert these
      #into a datetime format. There are 2400 time entries, one of them is the days summary, the other
      #is another 10minute interval.   
      if( len(row[1]) and len(row[2]) and len(row[3]) ):   
        hour = int(row[3]) / 100    #Get the hours
        minute = int(row[3]) - (hour * 100) # Get the minutes
        #There are entries for the 2400 hour that are for the previous day, not the day that would start
        #at 0000. These rows are a 24 hour rainfall summary and one row is the final 10 minute sample for the day.
        if( hour == 24 ):
          hour = 23
          minute = 59
        datetime = "%d-%d %02d:%02d" % (int(row[1]),int(row[2]),hour,minute)
        datetime = time.strptime( datetime, '%Y-%j %H:%M')
        datetime = time.strftime( '%Y-%m-%dT%H:%M:00', datetime )      
        dataRow.dateTime = datetime
      else:
        self.logger.error( "Missing a date field on line: %d in row: '%s', moving to next row" % ( self.file.line_num, row ) )
        return( dataRow )
        
      if( len(row[4])):
          dataRow.batteryVoltage = float(row[4])
      #else:
      #  self.logger.debug( "Battery voltage field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )

      if( len(row[5])):
        dataRow.programCode = float(row[5])
      #else:
      #  self.logger.debug( "Program Code field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )
      if( len(row[6])):
        dataRow.rainfall = float(row[6])
      else:
        self.logger.error( "Rainfall field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )
      #print( 'Processing line: %d %s' % ( self.file.line_num, row ) )
      return(dataRow)
   
    except csv.Error, e:
      self.lastErrorMsg = ('File %s. Line %d: %s' % (self.filePath, self.file.line_num, e))
      self.logger.error( self.lastErrorMsg )
            
    return(dataRow)
################################################################################################################  
"""
Class: dhecDB
Purpose: Interface to the dhec beach advisory prediction database.
"""
class dhecDB:
  """
  Function: __init__
  Purpose: Initializes the database object. Connects to the database passed in on the dbName parameter.
  """
  def __init__(self, dbName, loggerName=None):
    self.logger = None
    if( loggerName != None ):
      self.logger = logging.getLogger(loggerName)
      self.logger.info("creating an instance of dhecDB")
    self.totalRowsProcd = 0
    self.rowErrorCnt = 0
    self.lastErrorMsg = None
    try:
      self.dbCon = sqlite3.connect( dbName )
      #This enables the ability to manipulate rows with the column name instead of an index.
      self.dbCon.row_factory = sqlite3.Row
    except sqlite3.Error, e:
      if( self.logger != None ):
        self.logger.critical( e.args[0] + ' Terminating script.' )
      else:
        print( e.args[0] )
      sys.exit(-1)
    except Exception, e:
      if( self.logger != None ):
        self.logger.critical( str(e) + ' Terminating script.' )
      else:
        print( str(e) )
      sys.exit(-1)

  def __del__(self):
    self.dbCon.close()
        
  def loadSpatiaLiteLib(self, spatiaLiteLibFile):
    self.dbCon.enable_load_extension(True)
    sql = 'SELECT load_extension("%s");' % (spatiaLiteLibFile)
    cursor = self.executeQuery(sql)
    if(cursor != None):
      return(True)    
    return(False)
  
  """
  
  """
  def commit(self):
    try:
      self.dbCon.commit()
      return(True)
    except sqlite3.Error, e:
      self.lastErrorMsg = e.args[0]
    except Exception, e:
      self.lastErrorMsg = str(e)
    return(False)
  
  """
  Function: executeQuery
  Purpose: Attempts to execute a SQL statement against the database. If successful returns the cursor.
  Parameters: 
    sqlQuery is the query to execute. 
  Return:
    A cursor if successful, otherwise None. If the query is an UPDATE or INSERT the cursor
    will not contain any records to read.
  """
  def executeQuery(self, sqlQuery):   
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sqlQuery )        
      return( dbCursor )
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sqlQuery        
    except Exception, E:
      self.lastErrorMsg = str(E)                     
    return(None)
  
  """
  Function: vacuumDB
  Purpose: Cleanup the database. 
  Parameters: None
  Return: True if successful, otherwise False.
  """    
  def vacuumDB(self):
    try:
      sql = "VACUUM;"
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )    
      return( True )    
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sqlQuery        
    except Exception, E:
      self.lastErrorMsg = str(E)                     
    return(False)
    
  """
  Function: writePrecip
  Purpose: Writes an entry into the precipitation table. Normally this is what we do when we
   are parsing the rain gauge data file to.
  Parameters: 
    datetime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    batt_voltage is the battery voltage of the rain gauge
    program_code is teh program code for the rain gauge
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def writePrecip( self, datetime,rain_gauge,batt_voltage,program_code,rainfall):
    sql = "INSERT INTO precipitation  \
          (date,rain_gauge,batt_voltage,program_code,rainfall ) \
          VALUES( '%s','%s',%3.2f,%.2f,%2.4f);" % (datetime,rain_gauge,batt_voltage,program_code,rainfall)
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      self.totalRowsProcd += 1
      return(True)
    
    #Duplicate data. 
    except sqlite3.IntegrityError, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
      
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):      
        self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
      sys.exit(-1)
      
    return(False)         

  """
  Function: write24HourSummary
  Purpose: Writes an entry into the precip_daily_summary table. This is the days summary for the rain gauge.
  Parameters: 
    datetime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def write24HourSummary( self, datetime,rain_gauge,rainfall ):
    sql = "INSERT INTO precip_daily_summary  \
          (date,rain_gauge,rainfall ) \
          VALUES( '%s','%s',%2.4f );" % (datetime,rain_gauge,rainfall)
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      self.totalRowsProcd += 1
      return(True)

    #Duplicate data. 
    except sqlite3.IntegrityError, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )

    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):      
        self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      sys.exit(-1)
    return(False)
 
  """
  Function: getInspectionDates
  Purpose: Queries the dhec_beach table and returns the dates of the inspections for the given
    station.
  Parameters: 
    station is the name of the station we are quering the dates for.
  Return: A list of the dates. If none were found, the list is empty. 
  """
  def getInspectionDates(self, station):
    dateList = []
    sql = "SELECT insp_date,insp_time FROM dhec_beach WHERE station = '%s' ORDER BY insp_date ASC" % station
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        time = int(row['insp_time']) 
        #insp_time is in a hhmm format, so we break it apart.
        hour = time / 100    #Get the hours
        minute = time - (hour * 100) # Get the minutes
        dateList.append( "%sT%02d:%02d:00" %( row['insp_date'],hour,minute ) )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )      
      sys.exit(-1)
    return(dateList)
  """
  Function: getMoonPhase
  Purpose: For the given day, return the moon phase(percentage of moon visible).
  Parameters:
    date is the day of interest
  Return:
    a floating point number representing the percentage of moon visible
  """
  def getMoonPhase(self, date):
    moonPhase = -1.0
    sql = "SELECT phase FROM moon_phase WHERE date = strftime( '%%Y-%%m-%%d','%s')" % (date)
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      val = dbCursor.fetchone()
      if( val['phase'] != None ):
        moonPhase = float(val['phase'])

    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )      
      sys.exit(-1)
      
    return(moonPhase)
  
  """
  Function: writeSummaryForStation
  Purpose:  Writes the entry for day into the summary table. 
  Parameters:
    datetime is the date for the summary
    station is the name of the station we are quering the dates for.
  Return: A list of the dates. If none were found, the list is empty. 
  """
  def writeSummaryForStation(self, datetime, station ):
    #import datetime
    
    sql = "SELECT  dhec_beach.station,dhec_beach.insp_type,dhec_beach.etcoc,dhec_beach.tide,dhec_beach.salinity,dhec_beach.weather,monitoring_stations.rain_gauge,precip_daily_summary.rainfall \
          FROM dhec_beach,monitoring_stations,precip_daily_summary \
          WHERE \
          dhec_beach.station = monitoring_stations.station AND \
          monitoring_stations.rain_gauge = precip_daily_summary.rain_gauge AND \
          dhec_beach.station = '%s' AND \
          dhec_beach.insp_date = strftime('%%Y-%%m-%%d', datetime('%s') ) and \
          precip_daily_summary.date = strftime('%%Y-%%m-%%dT23:59:00',datetime('%s'))" % ( station,datetime,datetime )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      beachData = dbCursor.fetchone()
      if( beachData != None ):
        rainGauge = beachData['rain_gauge']
        if( rainGauge != None ):
          #Query the rainfall totals over the given hours range. 
          #Get the last 24 hours summary
          sum24 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,24)
          #Get the last 48 hours summary
          sum48 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,48)
          #Get the last 72 hours summary
          sum72 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,72)
          #Get the last 96 hours summary
          sum96 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,96)
          #Get the last 120 hours summary
          sum120 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,120)
          #Get the last 144 hours summary
          sum144 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,144)
          #Get the last 168 hours summary
          sum168 = self.getLastNHoursSummaryFromPrecipSummary(datetime,rainGauge,168)
          #calculate the X day delay totals
          #1 day delay
          sum1daydelay = sum48 - sum24
          #2 day delay
          sum2daydelay = sum72 - sum48
          #3 day delay
          sum3daydelay = sum96 - sum72
          
          #Get the preceding dry days count, if there are any.
          dryCnt = self.getPrecedingDryDaysCount(datetime, rainGauge )
          
          #Get the 24 hour rainfall intensity
          rainfallIntensity = self.calcRainfallIntensity( rainGauge, datetime, 10 )
                  
          #Write the summary table
          etcoc = -1
          if( beachData['etcoc'] != None and beachData['etcoc'] != '' ):
            etcoc = int(beachData['etcoc'])
          salinity = -1
          if( beachData['salinity'] != None and beachData['salinity'] != ''):
            salinity = int(beachData['salinity'])
          tide = -1
          if( datetime == '2002-05-16T09:50:00' or datetime == '2002-05-19T08:49:00' ):
            i = 0
          if( beachData['tide'] != None and beachData['tide'] != '' ):
            tide = int(beachData['tide'])
          if( tide == -1 ):
            tide = self.getTideLevel( 8661070, datetime )
          weather = -1
          if( beachData['weather'] != None and beachData['weather'] != '' ):
            weather = beachData['weather']
          
          #rainfall = 0.0
          #if( beachData['rainfall'] != None and beachData != '' ):
          #  rainfall = beachData['rainfall']
          moon = self.getMoonPhase( datetime )  
          sql = "INSERT INTO station_summary \
                (date,station,rain_gauge,etcoc,salinity,tide,moon_phase,weather, \
                rain_summary_24,rain_summary_48,rain_summary_72,rain_summary_96,rain_summary_120,rain_summary_144,rain_summary_168, \
                rain_total_one_day_delay,rain_total_two_day_delay,rain_total_three_day_delay,\
                preceding_dry_day_count,inspection_type,rainfall_intensity_24 ) \
                VALUES('%s','%s','%s',%d,%d,%d,%.2f,%d, \
                        %.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f, \
                        %.2f,%.2f,%.2f,\
                        %d,'%s',%.2f )" % \
                ( datetime,station,rainGauge,etcoc,salinity,tide,moon,weather,
                  sum24,sum48,sum72,sum96,sum120,sum144,sum168,
                  sum1daydelay,sum2daydelay,sum3daydelay,
                  dryCnt,beachData['insp_type'],rainfallIntensity)
          dbCursor.execute( sql )
          #self.dbCon.commit()
          if( self.logger != None ):
            self.logger.info("Adding summary for station: %s date: %s." %(station,datetime))
          else:
            print("Adding summary for station: %s date: %s." %(station,datetime))
        return(True)
      else:
        if( self.logger != None ):
          self.logger.info("No data for station: %s date: %s. SQL: %s" %(station,datetime,sql))
        else:
          print("No data for station: %s date: %s. SQL: %s" %(station,datetime,sql))
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
        
    except Exception, e:
      if( self.logger != None ):
        self.logger.critical( str(e) + ' Terminating script.' )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
      sys.exit(-1)
    
    return( False )
  
  def getLastNHoursSummaryFromPrecipSummary( self, datetime, rain_gauge, prevHourCnt ):
    sql = "SELECT SUM(rainfall) \
           FROM precip_daily_summary \
           WHERE\
             rain_gauge = '%s' AND \
             date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') )" \
             % ( rain_gauge, datetime, prevHourCnt, datetime )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      sum = dbCursor.fetchone()[0]
      if( sum != None ):
        return( float(sum) )      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
    return( -1.0 )

  def getLastNHoursSummary( self, datetime, rain_gauge, prevHourCnt ):
    sql = "SELECT SUM(rainfall) \
           FROM precipitation \
           WHERE\
             rain_gauge = '%s' AND \
             date <= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') ) AND \
             date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) )" % ( rain_gauge, datetime, datetime, prevHourCnt )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      sum = dbCursor.fetchone()[0]
      if( sum != None ):
        return( float(sum) )      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
    return( -1.0 )

  def getLastNHoursSummaryEpoch( self, datetime, rain_gauge, prevHourCnt ):
    epochtime = int(time.mktime(time.strptime( datetime, '%Y-%m-%dT%H:%M:00' )))  
    endtime = epochtime - ( prevHourCnt * 60 * 60 )
    sql = "SELECT SUM(rainfall) \
           FROM precipitation \
           WHERE\
             rain_gauge = '%s' AND \
             epoch_time <= %d AND \
             epoch_time >= %d" % ( rain_gauge, epochtime, endtime )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      sum = dbCursor.fetchone()[0]
      if( sum != None ):
        return( float(sum) )      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
    return( -1.0 )
  """
  Function: getPrecedingDryDaysCount
  Purpose: For the given date, this function calculates how many days previous had no rainfall, if any.
  Parameters: 
    datetime is the datetime we are looking for
    rainGauge is the rain  gauge we are query the rainfall summary for.
  """
  def getPrecedingDryDaysCount(self, datetime, rainGauge ):
    iDryCnt = 0
    secondsInDay = 24 * 60 * 60
    sql = "Select A.date, A.ndx,  A.rainfall, \
            Case \
              When A.rainfall = 0  Then \
                IFNULL( (Select Max(B.ndx) From precip_daily_summary As B WHERE B.ndx < A.ndx AND B.rainfall=0   ), \
                (A.ndx) ) \
              End As grp \
          From precip_daily_summary As A WHERE rain_gauge = '%s' AND date <= strftime('%%Y-%%m-%%dT23:59:00', datetime('%s', '-2 day') ) ORDER BY datetime(date) DESC" % \
          ( rainGauge, datetime )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )    
      #We subtract off a day since when we are looking for the summaries we start on -1 day from the date provided.
      t1 = time.mktime(time.strptime( datetime, '%Y-%m-%dT%H:%M:00' )) - secondsInDay
      for row in dbCursor:
        if( row['grp'] != None ):
          t2 = time.mktime(time.strptime( row['date'], '%Y-%m-%dT%H:%M:00' ))         
          #We have to make sure the data points are 1 day apart, according the thesis if we are missing
          #a data point, we don't calculate the preceding dry days.
          if( ( t1 - t2 ) <= secondsInDay ):
            iDryCnt += 1
            t1 = t2
          else:
            iDryCnt = -1
            break
        else:
          break
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
      iDryCnt = -1
    except TypeError, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s" % (str(e)) )
      else:
        print( "ErrMsg: %s" % (str(e)) )      
      iDryCnt = -1
    return( iDryCnt )
  """
  Function: calcRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 10 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    rainGauge is the name of the rain gauge we are investigating.
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    minutes is the number of minutes we want to collect rainfall for.
  """
  def calcRainfallIntensity(self, rainGauge, date, minutes ):
    rainfallIntensity = -1
    try:
      #Get the entries where there was rainfall for the date, going forward the minutes number of minutes. 
      sql = "SELECT rainfall from precipitation \
              WHERE rain_gauge = '%s' AND \
              date >= strftime('%%Y-%%m-%%d', datetime('%s','-1 day') ) AND date < strftime('%%Y-%%m-%%d', datetime('%s') );" \
              % (rainGauge, date, date )
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      totalRainfall = 0
      numRainEntries = 0    
      hasData = False  
      for row in dbCursor:
        rainfall = float(row['rainfall'])
        hasData = True
        if(rainfall > 0.0 ):
          totalRainfall += rainfall
          numRainEntries += 1
      
      #We want to check to make sure we have data from our query, if we do, let's zero out rainfallIntesity.
      #Otherwise we want to leave it at -1 to denote we had no data for that time.
      if( hasData == True ):
        rainfallIntensity = 0.0
          
      if( numRainEntries ):  
        rainfallIntensity = totalRainfall / ( numRainEntries * 10 )
        
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )      
      
    return(rainfallIntensity)
  
  def getTideLevel( self, tideStationID, date ):
    tideLevel = -1
    #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
    #the 2000 for flood or 4000 for ebb.
    tideStages = [0,100,200,300]
    sql = "SELECT date,level,level_code FROM daily_tide_range \
            WHERE station_id = %d AND ( date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '-12 hours') AND \
            date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '12 hours') ) \
            ORDER BY date ASC" % (tideStationID,date,date) 
    try:
      epochT1 = time.mktime(time.strptime( date, '%Y-%m-%dT%H:%M:%S' ))
      prevTime = -1.0
      curTime = -1.0
      prevRow = []
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        curTime = time.mktime(time.strptime( row['date'], '%Y-%m-%dT%H:%M:%S' ))
        if( prevTime != -1.0 ):
          if( ( epochT1 >= prevTime ) and 
              ( epochT1 < curTime ) ):
              #Figure out if the tide is in Ebb or Flood
              tideState = -1
              prevLevel = float(prevRow['level'])
              curLevel = float(row['level'])
              if( prevLevel < curLevel ):
                tideState = 2000
              else:
                tideState = 4000
              #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
              #up into 4 pieces, then figure out where our query time falls.
              totalTime = curTime - prevTime
              qtrTime = totalTime / 4.0
              tidePos = -1
              
              for i in range(0,4):
                if( ( epochT1 >= prevTime ) and
                    ( epochT1 < ( prevTime + qtrTime ) ) ):
                   tidePost = i
                   tideLevel = tideState + tideStages[i]
                   return( tideLevel )
                   
                prevTime += qtrTime
        prevRow = row
        prevTime = curTime
                
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
    return(tideLevel)
  """
  Function: getPlatforms
  Purpose: Returns the platforms(rain gauges, monitoring stations) from the platforms table
  """
  def getPlatforms(self):
    try:
      sql = "SELECT * from platforms"
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      return( dbCursor )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):      
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
        
    return( None )
  """
  Function: getRainGauges
  Purpose: Returns the rain gauges from the platforms table
  """
  def getRainGauges(self, active=None):
    try:
      whereClause = '';
      if( active != None ):
        whereClause = " AND active=%d" %(active)
      sql = "SELECT * from platforms WHERE type = 1 %s" %( whereClause )
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      return( dbCursor )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if( self.logger != None ):
        self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      else:
        print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
    return( None )
  
  """
  Function: getLastXMRGDate
  Purpose: Returns the last date collected for radar data.
  Parameters:
  Return: String with the last date, or None if nothing found.
  """
  def getLastXMRGDate(self):
    try:
      sql = "SELECT max(collection_date) AS date FROM precipitation_radar"
      dbCursor = self.executeQuery(sql)
      if( dbCursor != None ):
        row = dbCursor.fetchone()
        if( row != None ):
          return( row['date'] )
    except Exception, E:
      self.logger.error( str(E) + ' Terminating script' )
      sys.exit(-1)
    return( None )
 
  """
  Function: cleanPrecipRadar
  Purpose: This function will remove all data older the olderThanDate from the precipitation_radar table.
  Parameters:
    olderThanDate is the comparison date to use.
  Return: 
    True if successful, otherwise False.
  """
  def cleanPrecipRadar(self, olderThanDate):
    sql = "DELETE FROM precipitation_radar WHERE collection_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s');" % ( olderThanDate )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      try:
        self.dbCon.commit()
        return(True)  
      except sqlite3.Error, e:
        self.rowErrorCnt += 1
        if( self.logger != None ):
          self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
        else:
          print( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
        
    return(False)  
  
################################################################################################################  
class dhecConfigSettings(xmlConfigFile):
  def __init__(self, xmlConfigFilename):
    try:
      #Call parents __init__
      xmlConfigFile.__init__(self, xmlConfigFilename)
      
      #Log file settings
      self.logFile = self.getEntry( '//logging/logDir' )
      self.maxBytes = self.getEntry( '//logging/maxBytes' )
      if( self.maxBytes == None ):
        self.maxBytes = 100000
      else:
        self.maxBytes = int(self.maxBytes)
        
      self.backupCount = self.getEntry( '//logging/backupCount' )
      if( self.backupCount == None ):
        self.backupCount = 5
      else:
        self.backupCount = int(self.backupCount)
      
      #DB settings
      self.dbSettings = self.getDatabaseSettings()
      self.dbBackupFile = self.getEntry('//environment/database/db/backup/filePath')      
      
      #Directory where to store rain gauge files.
      self.rainGaugeFileDir = self.getEntry('//rainGaugeProcessing/rainGaugeFileDir')
      
      #FTP address to pull rain gauge files from.
      self.rainGaugeFTPAddy = self.getEntry( '//rainGaugeProcessing/ftp/ip' )
      #FTP login settings.
      self.rainGaugeFTPUser = self.getEntry( '//rainGaugeProcessing/ftp/user' )
      self.rainGaugeFTPPwd = self.getEntry( '//rainGaugeProcessing/ftp/passwd' )
      self.rainGaugeFTPDir = self.getEntry( '//rainGaugeProcessing/ftp/fileDir' )
      
      #Flag that specifies if logfiles should be deleted after processing.
      self.delServerFiles = self.getEntry( '//rainGaugeProcessing/ftp/delServerFile' )
      if( self.delServerFiles != None ):
        self.delServerFiles = int( self.delServerFiles )
      else:
        self.delServerFiles = 0
      
      #File path for KML file creation.
      self.kmlFilePath = self.getEntry( '//rainGaugeProcessing/outputs/kml/filePath' )    
  
      self.spatiaLiteLib = self.getEntry( '//database/db/spatiaLiteLib' )
      self.baseURL = self.getEntry( '//xmrgData/baseURL' )
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      self.fileNameFilter = self.getEntry( '//xmrgData/fileNameFilter' )   
      self.xmrgDLDir = self.getEntry( '//xmrgData/downloadDir' )
    except Exception, e:
      print( 'ERROR: ' + str(e)  + ' Terminating script')
      sys.exit(-1)

    
  
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
    self.totalLinesProcd = 0        #Total number of lines processed from all files.
    self.totalLinesUnprocd = 0      #Total number of lines unable to be processed for some reason/
    self.totalTime = 0.0            #Total execution time.
    try:
      #xmlTree = etree.parse(xmlConfigFile)
      self.configSettings = dhecConfigSettings( xmlConfigFile )
      
      #Create our logging object.
      if(self.configSettings.logFile == None):
        print( 'ERROR: //logging/logDir not defined in config file. Terminating script' )
        sys.exit(-1)     

      self.logger = logging.getLogger("dhec_logger")
      self.logger.setLevel(logging.DEBUG)
      # create formatter and add it to the handlers
      formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(lineno)d,%(message)s")

      #Create the log rotation handler.
      handler = logging.handlers.RotatingFileHandler( self.configSettings.logFile, "a", self.configSettings.maxBytes, self.configSettings.backupCount )
      handler.setLevel(logging.DEBUG)
      handler.setFormatter(formatter)    
      self.logger.addHandler(handler)
      # add the handlers to the logger
      self.logger.info('Log file opened')
      
      if( self.configSettings.dbSettings['dbName'] == None ):
        self.logger.error( 'ERROR: //database/db/name not defined in config file. Terminating script' )
        sys.exit(-1)                      
      self.logger.debug( 'Database path: %s' % (self.configSettings.dbSettings['dbName']) )
      self.db = dhecDB(self.configSettings.dbSettings['dbName'])
            
      #Get a file list for the directory.
      if( self.configSettings.rainGaugeFileDir == None ):
        self.logger.error( 'ERROR: //rainGaugeProcessing/rainGaugeFileDir not defined in config file. Terminating script' )
        sys.exit(-1)      
      
      self.logger.debug( 'Directory for rain gauge data: %s' % self.configSettings.rainGaugeFileDir )
      
      #Check the settings for ftping the rain gauge data
      if( self.configSettings.rainGaugeFTPAddy == None ):       
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/ip not defined in config file. Terminating script' )
        sys.exit(-1)      
      if(  self.configSettings.rainGaugeFTPUser == None ): 
        self.logger.error( 'ERROR: ///rainGaugeProcessing/ftp/user not defined in config file. Terminating script' )
        sys.exit(-1)      
      if( self.configSettings.rainGaugeFTPPwd == None ):
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/passwd not defined in config file. Terminating script' )
        sys.exit(-1)      
      if( self.configSettings.rainGaugeFTPDir == None ):
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/fileDir not defined in config file. Terminating script' )
        sys.exit(-1)      
      if( self.configSettings.delServerFiles == None ):
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/delServerFile not defined in config file. Terminating script' )
        sys.exit(-1)      
        
      self.logger.debug( 'Raingauge FTP Info: IP: %s User: %s Pwd: %s Dir: %s Delete Server Files: %d' % 
                         (self.configSettings.rainGaugeFTPAddy, self.configSettings.rainGaugeFTPUser, self.configSettings.rainGaugeFTPPwd, self.configSettings.rainGaugeFTPDir, self.configSettings.delServerFiles))
      
      if( self.configSettings.kmlFilePath == None ):
        self.logger.error( 'ERROR: //rainGaugeProcessing/outputs/kml/filePath, cannot output KML file' )
        
      self.emailList = []
      emailList = self.configSettings.getEntry( '//rainGaugeProcessing/alert/emailList' )
      if( emailList != None ):
        #List of email addresses to send the alert to.
        emailList = emailList.split(',')
        for email in emailList:
          self.emailList.append( email ) 
        val = self.configSettings.getEntry( '//rainGaugeProcessing/alert/lagTimeAlert' )
        if( val != None ):
          #This is the number of hours we can miss updates for the rain gauge data before we send an email alert.
          #Convert it into seconds.
          self.lagAlertTime = ( float(val) * 3600.0 )
        else:
          self.logger.error( "ERROR: //rainGaugeProcessing/alert/lagTimeAlert missing, cannot send email alert if data is missing"  )      
      else:
        self.logger.debug( "//rainGaugeProcessing/alert/emailList missing, cannot send email alert if data is missing"  )
                              
    except OSError, e:
      print( 'ERROR: ' + str(e) + ' Terminating script' )      
      sys.exit(-1)
    except Exception, e:
      print( 'ERROR: ' + str(e)  + ' Terminating script')
      sys.exit(-1)
  """
  Function: __del__
  Purpose: Destructor. Used to make sure the logger gets completely shutdown. 
  """
  def __del__(self):
    #Cleanup the logger.
    if( self.logger != None ):
      for handler in self.logger.handlers:
        self.logger.removeHandler( handler )
        handler.close()
  """
  Function: setFileList
  Purpose: Allows us to override the fileList of csv files to process. 
  Parameters:
    fileList is a list of csv files to process.
  """
  def setFileList(self, fileList ):
    self.fileList = fileList  
    
  """
  Function: deleteRainGaugeDataFiles
  Purpose: Deletes the rain gauge files in the rain gauge dir.
  Parameters:
  """
  def deleteRainGaugeDataFiles(self):
    self.fileList = os.listdir( self.configSettings.rainGaugeFileDir )      
    try:
      for file in self.fileList:
        fullPath = self.configSettings.rainGaugeFileDir + file
        
        #Make sure we are trying to delete a file and not a directory.
        if( os.path.isfile(fullPath) != True ):
          self.logger.debug( "%s is not a file, skipping" % (fullPath) )
          continue  
        else:
          os.remove( fullPath )
          self.logger.debug( "Deleted rain gauge file: %s" % (fullPath) )
          
    except Exception,e:
      self.logger.error(str(e) + ' Terminating script.')
      sys.exit(-1)
          
        
  """
  Function: ftpRainGaugeData
  Purpose: FTPs the rain gauge csv files onto our local machine for processing. 
  Parameters:
  """
  def ftpRainGaugeData(self):
    try:      
      ftp = FTP(self.configSettings.rainGaugeFTPAddy)
      ftp.login( self.configSettings.rainGaugeFTPUser, self.configSettings.rainGaugeFTPPwd )
      ftp.cwd(self.configSettings.rainGaugeFTPDir)
      #Get a list of the files in the dir
      fileList = ftp.nlst()
      for file in fileList:
        if( file.find( '.csv' ) > 0 ):
          Filename = self.configSettings.rainGaugeFileDir + file         
          outFile = open( Filename, 'wt')
          
          startTime = 0;
          if( sys.platform == 'win32'):
            startTime = time.clock()
          else:
            startTime = time.time()            
          #Download the file into the local file.
          ftp.retrlines("RETR " + file, lambda s, w=outFile.write: w(s+"\n"))

          if( sys.platform == 'win32'):
            endTime = time.clock()
          else:
            endTime = time.time()
          
          self.logger.debug( "FTPd file: %s to %s in %.2f ms" % (file,Filename,(endTime-startTime)*1000.0 ))          
          outFile.close()
          if( self.configSettings.delServerFiles ):
            ftp.delete( file )
          
        else:
          self.logger.debug( "File: %s is not a csv file" % (file) )
        
      return(True)
      
    except Exception,e:
      self.logger.error(str(e) )
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
      self.logger.info( "------------------------------------------------------------" )
      self.fileList = os.listdir( self.configSettings.rainGaugeFileDir )      
      for file in self.fileList:
        self.linesSkipped = 0
        self.dbRowsNotInserted = 0
        startTime = 0.0
        if( sys.platform == 'win32'):
          startTime = time.clock()
        else:
          startTime = time.time()
        try:
          fullPath = self.configSettings.rainGaugeFileDir + file
          
          self.logger.info( "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" )
          #Make sure we are trying to process a file and not a directory.
          if( os.path.isfile(fullPath) != True ):
            self.logger.debug( "%s is not a file, skipping" % (fullPath) )
            continue
            
          self.logger.info( "Begin processing file: %s" % fullPath )
          rainGaugeFile = readRainGaugeData()
          rainGaugeFile.openFile( fullPath )
          dataRow = rainGaugeFile.processLine()
          #Get the row id and the summary id.
          rainGaugeId = file.split('.')
          #id = self.rainGaugeInfo[rainGaugeId[0]]  
          #updateID = id['updateid']
          #summaryID = id['summaryid']
          self.writeSummaryTable = False
          while( dataRow != None ):
            if( dataRow.ID > 0 ):
              #The idea behind this is that there are 2 ID types in the data. One with a signature of xx1 is a normal
              #10 minute interval sample, one with an xx2 signature is the 24 hour summary. So if the first bit is
              #set, my assumption is that it's a 10 minute sample.
              updateType = dataRow.ID & 1
              if( updateType == 1):
                if( self.db.writePrecip( dataRow.dateTime, rainGaugeId[0].lower(), dataRow.batteryVoltage, dataRow.programCode, dataRow.rainfall ) == False ):
                  self.logger.error( 'Failed to write the precipitation data into the database. File Line: %d' % rainGaugeFile.file.line_num )
                  self.dbRowsNotInserted += 1                           
              elif( updateType == 0 ):
                if( self.db.write24HourSummary( dataRow.dateTime, rainGaugeId[0].lower(), dataRow.rainfall ) == False ):
                  self.logger.error( 'Failed to write the summary precipitation data into the database. File Line: %d' % rainGaugeFile.file.line_num )
                  self.dbRowsNotInserted += 1                           
              else:
                  self.logger.error( 'File Line: %d ID: %d is not valid' % ( rainGaugeFile.file.line_num, dataRow.ID ) )
                  self.linesSkipped += 1                           
            else:
              self.logger.error( 'No record processed from line: %d' % rainGaugeFile.file.line_num )
              self.linesSkipped += 1                           
                                      
            dataRow = rainGaugeFile.processLine()
         
        except StopIteration,e:
          if( self.linesSkipped ):
            self.logger.error( 'Unable to process: %d lines out of %d lines' % (self.linesSkipped, rainGaugeFile.file.line_num) )
            self.totalLinesUnprocd += self.linesSkipped
          else:
            self.logger.info( 'Total lines processed: %d' % rainGaugeFile.file.line_num )
          self.logger.info( 'EOF file: %s.' % file )
          self.totalLinesProcd += rainGaugeFile.file.line_num
          
        fileProcdCnt += 1
        try:
          #self.db.writeJunkTest( '00-00-00T00:00:00', 'TEST',-1,-1,-1)
          #Commit all the entries into the database for this file.
          self.logger.debug( 'Committing SQL inserts' )
          self.db.dbCon.commit()
          if( self.dbRowsNotInserted ):
            self.logger.error('Unable to insert: %d rows into the database.' % self.dbRowsNotInserted )
          endTime = 0.0
          if( sys.platform == 'win32'):
            endTime = time.clock()
          else:
            endTime = time.time()
          self.logger.debug( 'Total time to process file: %f msec' % ( ( endTime - startTime ) * 1000.0 ) )
          self.logger.info( "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" )
         
          self.totalTime += ( endTime - startTime ) * 1000.0
        except sqlite3.Error, e:
          self.logger.critical(e.args[0] + ' Terminating execution')
          sys.exit(-1)
    except Exception,e:
      self.logger.critical(str(e) + ' Terminating execution')
      sys.exit(-1)
    
    #Log various statistics out.
    self.logger.info( "###############Final Statistics#########################" )
    self.logger.info( "Finished processing file list. Processed: %d of %d files." % (fileProcdCnt,len( self.fileList )) )
    if( self.totalLinesUnprocd ):
      self.logger.error( "Unable to process: %d of %d lines." % (self.totalLinesUnprocd,self.totalLinesProcd ) )      
    else:
      self.logger.debug( "Total Lines Processed: %d" % (self.totalLinesProcd ) )
    if( self.db.rowErrorCnt ):
      self.logger.error( "Unable to insert: %d of %d rows into the database" % (self.db.rowErrorCnt,self.db.totalRowsProcd))
    else:
      self.logger.debug( "Inserted %d rows into the database" % (self.db.totalRowsProcd))
      
    self.logger.debug( "Total Processing Time: %f msecs" % (self.totalTime ) )
    self.logger.info( "###############Final Statistics#########################" )
    self.logger.info( "------------------------------------------------------------" )
    
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """  
  def vacuumDB(self):
    db = dhecDB(self.configSettings.dbSettings['dbName'], 'dhec_logger')
    if( db.vacuumDB() != None ):
      self.logger.debug("Vacuumed database.")
      return( True )
    else:
      self.logger.error( "Database vacuum failed; %s" %( db.lastErrorMsg ) )
      db.lastErrorMsg = ""
    return( False )
      
    
  
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
    if( len( self.emailList )):
      sendAlertEmail = True
    dbCursor = self.db.getRainGauges()
    if( dbCursor != None ):
      emailMsg = ''
      rainGaugeList = ''
      #To get the current epoch time in our time zone, we use the timezone value + the dstOffset.
      #timezone does not take daylight savings into account, so we manually do that.
      isdst = time.localtime()[-1]
      dstOffset = 0
      if( isdst ):
        dstOffset = 3600
      curEpochTime = ( time.time() )                 
      for row in dbCursor:
        sql = "SELECT max(date) AS date from precipitation WHERE rain_gauge = '%s'" %( row['name'] )
        dateCursor = self.db.dbCon.cursor()
        dateCursor.execute( sql )
        dateRow = dateCursor.fetchone() 
        if( dateRow != None ):          
          dataEpochTime = time.mktime( time.strptime(dateRow['date'], '%Y-%m-%dT%H:%M:%S') )
          dif = curEpochTime - dataEpochTime
          if( dif >  self.lagAlertTime ):
            if( sendAlertEmail ):
              rainGaugeList += "<li>Rain Gauge: %s last entry date: %s is older than %f hour</li>" % ( row['name'], dateRow['date'], ( self.lagAlertTime / 3600.0 ) )   
            self.logger.error( "Rain Gauge: %s last entry date: %s is older than %4.1f hours" % ( row['name'], dateRow['date'], ( self.lagAlertTime / 3600.0 ) ) )            
        dateCursor.close()
      dbCursor.close()
      if( len(rainGaugeList) ):
        import smtplib
        from email.MIMEMultipart import MIMEMultipart
        from email.MIMEText import MIMEText   
        emailMsg = "<ul>%s</ul>" %( rainGaugeList )
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
          self.logger.debug( "Sending alert email." )
        except Exception, E:
          self.logger.error( ( str(E) ) )
  
        
  """
  Function: writeKMLFile
  Purpose: Creates a KML file with the latest hour, 24, and 48 hour summaries.
  """
  def writeKMLFile(self):
    from pykml import kml

    if( self.configSettings.kmlFilePath != None ):
      try:
        self.logger.debug( "Creating DHEC rain gauge KML file: %s" %( self.configSettings.kmlFilePath ) )
        rainGaugeKML = kml.KML()
        doc = rainGaugeKML.createDocument( "DHEC Rain Gauges" )
        #DHEC doesn't reset the time on the rain gauges to deal with daylight savings, so if
        #we are in DST then we need to subtract one from the time to get the last hour.
        isdst = time.localtime()[-1]
        dstOffset = 0
        if( isdst ):
          dstOffset = 1
        curTime = time.strftime( '%Y-%m-%dT%H:%M:%S', time.localtime() )
        dbCursor = self.db.getRainGauges()
        if( dbCursor != None ):
          for row in dbCursor:
            last1 = self.db.getLastNHoursSummary( curTime, row['name'], ( 1 + dstOffset ) )  #Get last hours summary
            if( last1 == -1.0 ):
              last1 = 'Data unavailable'
            else:
              last1 = ( '%4.2f inches' ) % (last1) 
            last24 = self.db.getLastNHoursSummary( curTime, row['name'], ( 24 + dstOffset ) ) #Get last 24 hours summary
            if( last24 == -1.0 ):
              last24 = 'Data unavailable'
            else:
              last24 = ( '%4.2f inches' ) % (last24) 
            last48 = self.db.getLastNHoursSummary( curTime, row['name'], ( 48 + dstOffset ) ) #Get last 48 hours summary
            if( last48 == -1.0 ):
              last48 = 'Data unavailable'
            else:
              last48 = ( '%4.2f inches' ) % (last48) 
            curTime = curTime.replace( "T", " " )
            desc = "<table><tr>Location: %s</tr>\
                    <tr><ul>\
                    <li>Date/Time: %s</li>\
                    <li>Last Hour: %s</li>\
                    <li>Last 24 Hours: %s</li>\
                    <li>Last 48 Hours: %s</li></ul></table>"\
                    % ( row['description'], curTime, last1, last24, last48 )
            pm = rainGaugeKML.createPlacemark( row['name'], row['latitude'], row['longitude'], desc )
            doc.appendChild( pm )
        rainGaugeKML.root.appendChild( doc )  
        kmlFile = open( self.configSettings.kmlFilePath, "w" )
        kmlFile.writelines( rainGaugeKML.writepretty() )
        kmlFile.close()
      except Exception,e:
        self.logger.critical(str(e) + ' Terminating execution')
        sys.exit(-1)
    else:
      self.logger.error( "Cannot write KML file, no filepath provided in config file." )
  """
  Function: backupData
  Purpose: Rollover precipitation data in the live database older than 30 days into the backup database.
  """    
  def backupData(self):
    self.logger.info("-----------------------------------------------------------------------")  
    self.logger.info( "Beginning data backup/rollover." )
    
    curYear = time.strftime('%Y',time.localtime())
    backupDB = None
    filePath = "%s%s/" % (self.configSettings.dbBackupFile, curYear )
    backupFilename = "%s%s-dhec.db" % ( filePath, curYear )
    ################################################################################
    #Test to see if the database exists.
    if( not os.path.isfile(backupFilename) ):    
      #Check to see if the directory exists
      if( not os.path.exists( filePath ) ):
        os.mkdir(filePath)
        self.logger.info( "Directory: %s does not exist, creating." %(filePath))
        
      self.logger.info( "File: %s does not exist, creating." %(backupFilename))
        
      backupDB = dhecDB(backupFilename,"dhec_logger")
      #Now create the tables for precipitation and precip_daily_summary.
      sql = "CREATE TABLE \"precipitation\" (\"Ndx\" INTEGER PRIMARY KEY  NOT NULL ,"\
            "\"date\" DATETIME NOT NULL ,"\
            "\"rain_gauge\" TEXT NOT NULL ,"\
            "\"batt_voltage\" FLOAT,"\
            "\"program_code\" FLOAT,"\
            "\"rainfall\" FLOAT NOT NULL )"
      dbCursor = backupDB.executeQuery(sql)
      if(dbCursor == None ):
        self.logger.error( "Failed to create table: precipitation." )
        sys.exit(-1)
      self.logger.info( "Created table precipitation." );
      #Now Create the index
      sql = "CREATE UNIQUE INDEX \"idx_precipitation\" ON \"precipitation\" (\"date\" ASC, \"rain_gauge\" ASC)"
      dbCursor = backupDB.executeQuery(sql)
      if(dbCursor == None ):
        self.logger.error( "Failed to create index: idx_precipitation." )
        sys.exit(-1)
      self.logger.info( "Created index idx_precipitation." );
        
      sql = "CREATE TABLE \"precip_daily_summary\""\
          "(\"Ndx\" INTEGER PRIMARY KEY  NOT NULL ,"\
           "\"date\" DATETIME NOT NULL ,"\
           "\"rain_gauge\" TEXT NOT NULL ,"\
           "\"rainfall\" FLOAT NOT NULL )"
      dbCursor = backupDB.executeQuery(sql)
      if(dbCursor == None ):
        self.logger.error( "Failed to create table: precip_daily_summary." )
        sys.exit(-1)
      self.logger.info( "Created table precip_daily_summary." );
        
      #Now Create the index
      sql = "CREATE UNIQUE INDEX \"idx_precip_daily_summary\" ON \"precip_daily_summary\" (\"date\" ASC, \"rain_gauge\" ASC)"
      dbCursor = backupDB.executeQuery(sql)
      if(dbCursor == None ):
        self.logger.error( "Failed to create index: idx_precip_daily_summary." )
        sys.exit(-1)
      self.logger.info( "Created index idx_precip_daily_summary." );      
    else:
      backupDB = dhecDB(backupFilename,"dhec_logger")
      self.logger.info( "Connecting to database: %s" % (backupFilename) )
    ################################################################################
      
    ################################################################################
    #On a platform by platform basis, get all data that is not in the current month.    
    dbCursor = self.db.getRainGauges()
    #Cutoff date, we want to keep last 30 days.  
    
    cutoffDate = time.strftime( "%Y-%m-%dT00:00:00", time.localtime(time.time()-(30 * 24 * 60 * 60)))
    gaugeList = []
    for row in dbCursor:    
      gaugeList.append(row['name'])
    dbCursor.close()
    for rainGauge in gaugeList:
      self.logger.info( "Processing precipitation table data for rain gauge: %s" % (rainGauge))
      sql = "SELECT * FROM precipitation WHERE date < '%s' AND rain_gauge='%s'" %(cutoffDate,rainGauge) 
      resultsCursor = self.db.executeQuery(sql)
      rowCnt = 0
      if(resultsCursor != None):
        for item in resultsCursor:
          backupDB.writePrecip( item['date'], 
                                item['rain_gauge'], 
                                item['batt_voltage'], 
                                item['program_code'], 
                                item['rainfall'])   
          rowCnt += 1               
        if( not backupDB.commit() ):
          self.logger.error( backupDB.lastErrorMsg )
          sys.exit(-1)
        self.logger.info( "Successfully processed and committed: %d rows into backup." % (rowCnt) )
        resultsCursor.close()
        
        #Now we delete the records from the source DB.
        self.logger.info( "Deleting backed up records from source database.")
        sql = "DELETE FROM precipitation WHERE date < '%s' and rain_gauge='%s'" % (cutoffDate,rainGauge)
        resultsCursor = self.db.executeQuery(sql)
        if(resultsCursor != None):
          if(not self.db.commit()):
            self.logger.error(self.db.lastErrorMsg)
            sys.exit(-1)
        else:
          self.logger.error(self.db.lastErrorMsg)
          sys.exit(-1)
        resultsCursor.close()
        
      self.logger.info( "Processing precip_daily_summary table data for rain gauge: %s" % (rainGauge))
      rowCnt = 0
      sql = "SELECT * FROM precip_daily_summary WHERE date < '%s' AND rain_gauge='%s'" %(cutoffDate,rainGauge) 
      resultsCursor = self.db.executeQuery(sql)
      if(resultsCursor != None):
        for item in resultsCursor:
          backupDB.write24HourSummary( item['date'], 
                                        item['rain_gauge'], 
                                        item['rainfall'])     
          rowCnt += 1               
               
        if( not backupDB.commit() ):
          self.logger.error( backupDB.lastErrorMsg )
          sys.exit(-1)
        self.logger.info( "Successfully processed and committed: %d rows into backup." % (rowCnt) )
        resultsCursor.close()
        
        #Now we delete the records from the source DB.
        self.logger.info( "Deleting backed up records from source database.")
        sql = "DELETE FROM precip_daily_summary WHERE date < '%s' and rain_gauge='%s'" % (cutoffDate,rainGauge)
        resultsCursor = self.db.executeQuery(sql)
        if(resultsCursor != None):
          if(not self.db.commit()):
            self.logger.debug(self.db.lastErrorMsg)
            sys.exit(-1)
          resultsCursor.close()
        else:
          self.logger.error(self.db.lastErrorMsg)
          sys.exit(-1)
      else:
        self.logger.error( self.db.lastErrorMsg )
        sys.exit(-1)
    self.logger.info( "Finished data backup/rollover." )
    self.logger.info("-----------------------------------------------------------------------")  
      
################################################################################################################  
      
if __name__ == '__main__':
  if( len(sys.argv) < 2 ):
    print( "Usage: xmrgFile.py xmlconfigfile")
    sys.exit(-1)    


  dhecData = processDHECRainGauges(sys.argv[1])

  #dhecData.processFiles()
  #Create KML file for obs.
  
  
