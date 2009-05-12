import os
import sys
import array
import struct
import csv
import time
import logging
#import logging.handlers
from collections import defaultdict  
from lxml import etree
from ftplib import FTP
from pysqlite2 import dbapi2 as sqlite3


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
"""
Class: dhecDB
Purpose: Interface to the dhec beach advisory prediction database.
"""
class dhecDB:
  """
  Function: __init__
  Purpose: Initializes the database object. Connects to the database passed in on the dbName parameter.
  """
  def __init__(self, dbName):
    self.logger = logging.getLogger("dhec_logger.dhecDB")
    self.logger.info("creating an instance of dhecDB")
    self.totalRowsProcd = 0
    self.rowErrorCnt = 0
    try:
      self.dbCon = sqlite3.connect( dbName )
      #This enables the ability to manipulate rows with the column name instead of an index.
      self.dbCon.row_factory = sqlite3.Row
    except sqlite3.Error, e:
      self.logger.critical( e.args[0] + ' Terminating script.' )
      sys.exit(-1)
    except Exception, e:
      self.logger.critical( str(e) + ' Terminating script.' )
      sys.exit(-1)
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
      self.logger.error( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
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
      self.logger.error( "ErrMsg: %s SQL: \"%s\"" % (e.args[0], sql) )
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      sys.exit(-1)
    return(False)
  """
  Function: getRainGaugeNames
  Purpose: Queries the monitoring_stations table and returns the distinct rain gauges.  
  Parameters: None 
  Return: A list of the rain gauges. If none were found, the list is empty. 
  """
  def getRainGaugeNames(self):
    gaugeList = []
    sql = "SELECT DISTINCT(rain_gauge) from monitoring_stations"    
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        gaugeList.append( row[0] )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      sys.exit(-1)
    return(gaugeList)

  """
  Function: getStationNames
  Purpose: Queries the monitoring_stations table and returns the monitoring stations  
  Parameters: None 
  Return: A list of the stations. If none were found, the list is empty. 
  """
  def getStationNames(self):
    stationList = []
    sql = "SELECT station from monitoring_stations"    
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        stationList.append( row[0] )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      sys.exit(-1)
    return(stationList)
  
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
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
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
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
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
          self.logger.info("Adding summary for station: %s date: %s." %(station,datetime))
        return(True)
      else:
        self.logger.info("No data for station: %s date: %s. SQL: %s" %(station,datetime,sql))
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
    except Exception, e:
      self.logger.critical( str(e) + ' Terminating script.' )
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      iDryCnt = -1
    except TypeError, e:
      self.rowErrorCnt += 1
      self.logger.error( "ErrMsg: %s" % (str(e)) )
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      
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
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
    return(tideLevel)
  """
  Function: getPlatforms
  Purpose: Returns the platforms(rain gauges, monitoring stations) from the platforms table
  """
  def getPlatforms(self):
    try:
      #whereClause = "WHERE active = 1"
      #if( !active ):
      #  where = "WHERE active = 0"
      sql = "SELECT * from platforms"
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      return( dbCursor )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
    return( None )
  
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
      xmlTree = etree.parse(xmlConfigFile)

      #Create our logging object.
      val = xmlTree.xpath( '//logging/logDir' )
      if(len(val)):
        logFile = val[0].text
      else:
        print( 'ERROR: //logging/logDir not defined in config file. Terminating script' )
        sys.exit(-1)     

      self.logger = logging.getLogger("dhec_logger")
      self.logger.setLevel(logging.DEBUG)
      # create file handler which logs even debug messages
      logFh = logging.FileHandler(logFile)
      logFh.setLevel(logging.DEBUG)
      # create formatter and add it to the handlers
      formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(lineno)d,%(message)s")
      logFh.setFormatter(formatter)

      #Create the log rotation handler.
      #handler = logging.handlers.RotatingFileHandler( logFile, maxBytes=500, backupCount=5)      
      #self.logger.addHandler(handler)

      # add the handlers to the logger
      self.logger.addHandler(logFh)
      self.logger.info('Log file opened')

      val = xmlTree.xpath( '//database/db/name' )
      if(len(val)):
        dbPath = val[0].text
      else:
        self.logger.error( 'ERROR: //database/db/name not defined in config file. Terminating script' )
        sys.ext(-1)      
        
      self.logger.debug( 'Database path: %s' % dbPath )
      self.db = dhecDB(dbPath) 
      #Get a file list for the directory.
      val = xmlTree.xpath( '//rainGaugeProcessing/rainGaugeFileDir' )
      if(len(val)):
        self.fileDirectory = val[0].text 
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/rainGaugeFileDir not defined in config file. Terminating script' )
        sys.ext(-1)      
      
      self.logger.debug( 'Directory for rain gauge data: %s' % self.fileDirectory )
      
      #Get the settings for ftping the rain gauge data
      val = xmlTree.xpath( '//rainGaugeProcessing/ftp/ip' )
      if( len(val) ):
        self.rainGaugeFTPAddy = val[0].text       
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/ip not defined in config file. Terminating script' )
        sys.ext(-1)      
      val = xmlTree.xpath( '//rainGaugeProcessing/ftp/user' )
      if( len(val) ):
        self.rainGaugeFTPUser = val[0].text 
      else:
        self.logger.error( 'ERROR: ///rainGaugeProcessing/ftp/user not defined in config file. Terminating script' )
        sys.ext(-1)      
      val = xmlTree.xpath( '//rainGaugeProcessing/ftp/passwd' )
      if( len(val) ):
        self.rainGaugeFTPPwd = val[0].text
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/passwd not defined in config file. Terminating script' )
        sys.ext(-1)      
      val = xmlTree.xpath( '//rainGaugeProcessing/ftp/fileDir' )
      if(len(val)):
        self.rainGaugeFTPDir = val[0].text
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/fileDir not defined in config file. Terminating script' )
        sys.ext(-1)      
      val = xmlTree.xpath( '//rainGaugeProcessing/ftp/delServerFile' )
      if(len(val)):
        self.delServerFiles = int(val[0].text)
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/ftp/delServerFile not defined in config file. Terminating script' )
        sys.ext(-1)      
        
      self.logger.debug( 'Raingauge FTP Info: IP: %s User: %s Pwd: %s Dir: %s Delete Server Files: %d' % 
                         (self.rainGaugeFTPAddy, self.rainGaugeFTPUser, self.rainGaugeFTPPwd, self.rainGaugeFTPDir, self.delServerFiles))
      
      val = xmlTree.xpath( '//rainGaugeProcessing/outputs/kml/filePath' )
      if(len(val)):
        self.kmlFilePath = val[0].text
      else:
        self.logger.error( 'ERROR: //rainGaugeProcessing/outputs/kml/filePath, cannot output KML file' )
      #The data files have an ID as the first column with the format of xx1 or xx2. xx1 is the 10 minute
      #data where the xx2 is the 
      #self.rainGaugeInfo = defaultdict(dict)
      #rainGauge = xmlTree.xpath( '//environment/rainGaugeProcessing/rainGaugeList')
      #for child in rainGauge[0].getchildren():
      #  name = child.xpath( 'name' )[0].text 
      #  updateID = int(child.xpath( 'fileID' )[0].text) 
      #  summaryID = int(child.xpath( 'file24hrSumId' )[0].text)
      #  self.rainGaugeInfo[name]['updateid'] = updateID
      #  self.rainGaugeInfo[name]['summaryid'] = summaryID
      
    except OSError, e:
      print( 'ERROR: ' + str(e) + ' Terminating script' )      
    except Exception, e:
      print( 'ERROR: ' + str(e)  + ' Terminating script')
      
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
    self.fileList = os.listdir( self.fileDirectory )      
    try:
      for file in self.fileList:
        fullPath = self.fileDirectory + file
        
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
      ftp = FTP(self.rainGaugeFTPAddy)
      ftp.login( self.rainGaugeFTPUser, self.rainGaugeFTPPwd )
      ftp.cwd(self.rainGaugeFTPDir)
      #Get a list of the files in the dir
      fileList = ftp.nlst()
      for file in fileList:
        if( file.find( '.csv' ) > 0 ):
          Filename = self.fileDirectory + file         
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
          if( self.delServerFiles ):
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
      self.fileList = os.listdir( self.fileDirectory )      
      for file in self.fileList:
        self.linesSkipped = 0
        self.dbRowsNotInserted = 0
        startTime = 0.0
        if( sys.platform == 'win32'):
          startTime = time.clock()
        else:
          startTime = time.time()
        try:
          fullPath = self.fileDirectory + file
          
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
  Function: writeKMLFile
  Purpose: Creates a KML file with the latest hour, 24, and 48 hour summaries.
  """
  def writeKMLFile(self):
    from pykml import kml

    if( len( self.kmlFilePath ) ):
      try:
        rainGaugeKML = kml.KML()
        doc = rainGaugeKML.createDocument( "DHEC Rain Gauges" )
        #DHEC doesn't reset the time on the rain gauges to deal with daylight savings, so if
        #we are in DST then we need to subtract one from the time to get the last hour.
        isdst = time.localtime()[-1]
        dstOffset = 0
        if( isdst ):
          dstOffset = 1
        curTime = time.strftime( '%Y-%m-%dT%H:%M:%S', time.localtime() )
        dbCursor = self.db.getPlatforms()
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
        kmlFile = open( self.kmlFilePath, "w" )
        kmlFile.writelines( rainGaugeKML.writepretty() )
        kmlFile.close()
      except Exception,e:
        self.logger.critical(str(e) + ' Terminating execution')
        sys.exit(-1)
    else:
      self.logger.error( "Cannot write KML file, no filepath provided in config file." )
      
if __name__ == '__main__':
  if( len(sys.argv) < 2 ):
    print( "Usage: xmrgFile.py xmlconfigfile")
    sys.exit(-1)    

  #datetime = time.strptime( "2004-07-31", '%Y-%m-%d')
  #date = time.strftime( '%j', datetime )

  dhecData = processDHECRainGauges(sys.argv[1])
  #dhecData.processFiles()
  #Create KML file for obs.
  
  sys.exit(0)  
  
  summaryFile = open( "C:\\Documents and Settings\\dramage\\workspace\\SVNSandbox\\wqportlet\\trunk\\data\\raingauge\\PrecipStats.csv", "wa")
  #dateList = ['2001','2002','2003','2004','2005','2006','2007','2008','2009']  
  dateList = ['2001']
  gaugeList = dhecData.db.getRainGaugeNames()
  for rainGauge in gaugeList:
    summaryFile.write( "Rain Gauge: %s\n" % (rainGauge) )
    for date in dateList:      
      summaryFile.write( "Year: %s\n" % (date) )
      startDate = ("%s-01-01") % date
      endDate = ("%s-12-31") % date
      sql = "SELECT * FROM precipitation WHERE \
              rain_gauge = '%s' AND \
              date >= datetime('%s') AND date <= ('%s')" % ( rainGauge,startDate,endDate)
      try:
        dbCursor = dhecData.db.dbCon.cursor()
        dbCursor.execute( sql )
        updateInterval = 10 * 60 #Each update should be every 10 minutes, we want to convert to seconds for epoch time compares.
        missingInterval = 0        
        t2 = -1
        firstdate = ''
        lastdate = ''
        prevDate = ''
        #test = time.mktime(time.strptime( '2001-09-01T23:59:00', '%Y-%m-%dT%H:%M:00' ))
        for row in dbCursor:
          lastdate = row['date']
          t1 = time.mktime(time.strptime( lastdate, '%Y-%m-%dT%H:%M:00' ))
          if(t2 != -1):
            # Give a padding of 5 minutes(300secs) to see if the update is falling in the right place.
            if( ( ( t1 - t2 ) > ( updateInterval + 300 ) ) ):
              #if( test == t1 or test == t2 ):
              #  i = 0
              missingInterval = ( (t1 - t2) / updateInterval ) - 1
              summaryFile.write( "%s missing %d intervals\n" % (prevDate,missingInterval))
          else:
            firstdate = row['date']
          prevDate = row['date']
          t2 = t1

        summaryFile.write( "Starting date: %s Ending date: %s\n" %( firstdate,lastdate))
      except sqlite3.Error, e:
        self.logger.critical('%s SQL: %s Terminating execution' % (e.args[0], sql))
        sys.exit(-1)
      except Exception,e:
        self.logger.critical(str(e) + ' Terminating execution')
        sys.exit(-1)



#      dhecData.db.writeSummaryForStation( date, station )
  #dhecData.processFiles()
#  summaryFile = open( "C:\\Documents and Settings\\dramage\\workspace\\SVNSandbox\\wqportlet\\trunk\\data\\raingauge\\summary.csv", "wa")
#  summaryFile.write( "Rain Gauge,Summary (inches), Days over 1/2in,Date Range\n" )
#  dateList = [["2006-03-01","2006-09-30"],["2007-03-01","2007-09-30"], ["2008-03-01","2008-09-30"]]
  
#  gaugeList = dhecData.db.getRainGaugeNames()
#  for datePair in dateList:  
#    for gauge in gaugeList:
#      sql = "SELECT SUM(rainfall) FROM precip_daily_summary WHERE  \
#              rain_gauge = '%s' AND \
#              date >= strftime('%%Y-%%m-%%dT00:00:00', datetime('%s')) AND \
#              date <= strftime('%%Y-%%m-%%dT23:59:00', datetime('%s'))" % (gauge,datePair[0],datePair[1])
#      try:
#        dbCursor = dhecData.db.dbCon.cursor()
#        dbCursor.execute( sql )
#        sum = 0
#        row = dbCursor.fetchone()
#        if(row[0] != None):
#          sum = float(row[0])
#
#        sql = "SELECT COUNT(date) FROM precip_daily_summary WHERE  \
#              rain_gauge = '%s' AND \
#              rainfall > 0.5 AND \
#              date >= strftime('%%Y-%%m-%%dT00:00:00', datetime('%s')) AND \
#              date <= strftime('%%Y-%%m-%%dT23:59:00', datetime('%s'))" % (gauge,datePair[0],datePair[1])
#        dbCursor.execute( sql )
#        cntoverhalfinch = 0
#        row = dbCursor.fetchone()
#        if(row[0] != None):
#          cntoverhalfinch = int(row[0])
#        summaryFile.write( "%s,%f,%d,%s to %s\n" % ( gauge,sum,cntoverhalfinch,datePair[0],datePair[1]))
#      except sqlite3.Error, e:
#        self.logger.critical('%s SQL: %s Terminating execution' % (e.args[0], sql))
#        sys.exit(-1)
#      except Exception,e:
#        self.logger.critical(str(e) + ' Terminating execution')
#        sys.exit(-1)
      
        
  #xmrg = xmrgFile()
  #inputFile = "C:\\Temp\\xmrg0506199516z\\xmrg0506199516z"
#  inputFile = "C:\\Temp\\xmrg0129200918z\\xmrg0129200918z"
#  xmrg.openFile( inputFile, 0 )
#  xmrg.writeASCIIGrid( 'polarstereo', 'C:\\Temp\\xmrg0129200918z\\xmrg0129200918z.asc')
  #xmrg.writeASCIIGrid( 'hrap', 'C:\\Temp\\xmrg0506199516z\\xmrg0506199516z.asc')
  
#  try:
#    srcGridFile = gdal.Open('C:\\Temp\\xmrg0129200918z\\xmrg0129200918z.asc', GA_ReadOnly)
#    geotransform = srcGridFile.GetGeoTransform()
#    band = srcGridFile.GetRasterBand(1)
#    print 'Size is ',srcGridFile.RasterXSize,'x',srcGridFile.RasterYSize,'x',srcGridFile.RasterCount
#    print 'Projection is ',srcGridFile.GetProjection()
#    print 'Origin = (',geotransform[0], ',',geotransform[3],')'
#    print 'Pixel Size = (',geotransform[1], ',',geotransform[5],')'
#    print 'Converting band number',1,'with type',gdal.GetDataTypeName(band.DataType)    
       
#  except Exception, E:
#    print( 'ERROR: ' + str(E) ) 
  
  