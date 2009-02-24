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

try:
  from osgeo import gdal
  from osgeo.gdalconst import *
  gdal.TermProgress = gdal.TermProgress_nocb
except ImportError:
  import gdal
  from gdalconst import *  
try:
  import numpy as Numeric
  Numeric.arrayrange = Numeric.arange
except ImportError:
  import Numeric

"""
  Class: xmrgFile
  Purpose: This class processes a NOAA XMRG binary file.
"""
class xmrgFile:
  """
    Function: init
    Purpose: Initalizes the class.
    Parameters: None
    Return: None
  """
  def __init__(self):
    self.fileName = ''
    self.lastErrorMsg = ''
    self.srcFileOpen = 0
    
  def openFile(self, filePath, useLog):
    self.fileName = filePath
    retVal = False
    try:
      self.xmrgFile = open( self.fileName, mode = 'rb' )
      retVal = True
    except Exception, E:
      self.lastErrorMsg = str(E)
      print( 'ERROR: ' + str(E)) 
   
    return(retVal)
    
  def readFileHeader( self ):
    try:
      #Determine if byte swapping is needed.
      #From the XMRG doc:
      #FORTRAN unformatted records have a 4 byte integer at the beginning and
      #end of each record that is equal to the number of 4 byte words
      #contained in the record.  When reading xmrg files through C using the
      #fread function, the user must account for these extra bytes at the
      #beginning and end of each  record.
      
      #Original header is as follows
      #4 byte integer for num of 4 byte words in record
      #int representing HRAP-X coord of southwest corner of grid(XOR)
      #int representing HRAP-Y coord of southwest corner of grid(YOR)
      #int representing HRAP grid boxes in X direction (MAXX)
      #int representing HRAP grid boxes in Y direction (MAXY)
      header = array.array('I')
      #read 6 bytes since first int is the header, next 4 ints are the grid data, last int is the tail. 
      header.fromfile( self.xmrgFile, 6)
      self.swapBytes= 0
      #Determine if byte swapping is needed
      if( header[0] != 16 ):
        self.swapBytes = 1
        header.byteswap()
      
      self.XOR = header[1]    #X Origin of the HRAP grid     
      self.YOR = header[2]    #Y origin of the HRAP grid
      self.MAXX = header[3]   #Number of columns in the data 
      self.MAXY = header[4]   #Number of rows in the data 
      
      #reset the array
      header = array.array('I')
      #Read the fotran header for the next block of data. Need to determine which header type we'll be reading
      header.fromfile( self.xmrgFile, 1 )
      if( self.swapBytes ):
        header.byteswap()
        
      self.fileNfoHdrData = '' 
      byteCnt = header[0]  
      unpackFmt = ''
      hasDataNfoHeader = True
      if( self.swapBytes ):
        unpackFmt += '>'
        
      #Header for files written 1999 to present.
      if( byteCnt == 66 ):
        #The info header has the following layout
        #Operating system: char[2]
        #user id: char[8]
        #saved date: char[10]
        #saved time: char[10]
        #process flag: char[20]
        #valid date: char[10]
        #valid time: char[10]
        #max value: int
        #version number: float
        unpackFmt += '=2s8s10s10s8s10s10sif'
        buf = self.xmrgFile.read(66)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        self.srcFileOpen = 1
      #Files written June 1997 to 1999  
      elif( byteCnt == 38 ):
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(38)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        self.srcFileOpen = 1
        
      #Files written June 1997 to 1999. I assume there was some bug for this since the source
      #code also was writing out an error message.  
      elif( byteCnt == 37 ):
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(37)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        self.srcFileOpen = 1
        
      #Files written up to June 1997, no 2nd header.  
      elif( byteCnt == ( self.MAXX * 2 ) ):
        print( "Reading pre-1997 format" )        
        self.srcFileOpen = 1
        #File does not have 2nd header, so we need to reset the file point to the point before we
        #did the read for the 2nd header tag.
        self.xmrgFile.seek( 24, os.SEEK_SET )
        hasDataNfoHeader = False
      
      #Invalid byte count.
      else:
        self.lastErrorMsg = 'Header is unknown format, cannot continue.'
        return( False )
      
      #If the file we are reading was not a pre June 1997, we read the tail int, 
      #should be equal to byteCnt
      if( hasDataNfoHeader ): 
        header = array.array('I')
        header.fromfile( self.xmrgFile, 1 )
        if( self.swapBytes ):
          header.byteswap()        
        if( header[0] != byteCnt ):
          self.lastErrorMsg = 'ERROR: tail byte cnt does not equal head.'
          return( False )
          
      if( self.srcFileOpen ):
        return( True )

    except Exception, E:
      self.lastErrorMsg = str(E)
    
    return( False )      
  
  def writeASCIIGrid(self, format, outputFile, northmostrowfirst=True, units='inches'):
    if( self.readFileHeader() ):
      try:
        gridFile = open( outputFile, "wt")        
        ####################################################################
        #Write the grid header
        xllCorner = -1
        yllCorner = -1
        cellsize = -1
        nodata = -1
        
        dataConvert = 100.0
        #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
        #inches , need to divide by 2540.
        if( units == 'inches' ):
          dataConvert = dataConvert * 25.4
          
        #Write the grid in an HRAP projection?
        if(format == 'hrap'):
          xllCorner = self.XOR
          yllCorner = self.YOR
          cellsize = 1 
                   
        #Write the grid in a polar stereo  projection?
        elif( format == 'polarstereo'):
          xllCorner = ( self.XOR * 4762.5 ) - ( 401.0 * 4762.5 )
          yllCorner = ( self.YOR * 4762.5 ) - ( 1601.0 * 4762.5 )
          cellsize = 4762.5
          nodata = -9999.0
        
        gridFile.write( 'ncols %d\n' % self.MAXX )  
        gridFile.write( 'nrows %d\n' % self.MAXY )  
        gridFile.write( 'xllcorner %d\n' % xllCorner )
        gridFile.write( 'yllcorner %d\n' % yllCorner )
        gridFile.write( 'cellsize %f\n' % cellsize )
        if( nodata != -1 ):
          gridFile.write( 'nodata_value %d\n' % nodata )
        ####################################################################
        #Write data
        #Used if we want to make the northmost row the first in the grid file. We have to invert
        #the rows as they are in the source file since it is southmost first there.
        rowList = []
        if( northmostrowfirst ):
          rowList = [ [0 for y in range(self.MAXX)] for x in range(self.MAXY) ]
        for row in range( self.MAXY ):
          #Read off the record header
          dataArray= array.array('I')
          dataArray.fromfile( self.xmrgFile, 1 )
          if( self.swapBytes ):
            dataArray.byteswap();
          #Verify the header for this row of data matches what the header specified.
          #We do MAXX * 2 since each value is a short.
          if( dataArray[0] != (self.MAXX*2) ):
            self.lastErrorMsg = 'Header tag Byte count: %d for row: %d does not match header: %d.' %( dataArray[0], row, self.MAXX )
            print( 'ERROR: ' + self.lastErrorMsg )
            return( False )
          
          #Read a columns worth of data out
          dataArray= array.array('h')
          dataArray.fromfile( self.xmrgFile, self.MAXX )
          
          #Need to byte swap?
          if( self.swapBytes ):
            dataArray.byteswap();
          col = 0
          for val in dataArray:            
            if( val < 0 ):
              val = -9999.0
            else:
              val /= dataConvert
            #If we are writing the file so the southmost row is first, go head and write the row
            #to the file. Otherwise to have northmost first, we have to invert the rows.
            if( northmostrowfirst == False ):
              gridFile.write( '%f ' % val )
            else:
              rowList[row][col] = val
            col+=1
          #Add the return at the end of the row.
          if( northmostrowfirst == False ):
            gridFile.write( "\n" )
          
          #Now read trailing tag
          dataArray= array.array('I')
          dataArray.fromfile( self.xmrgFile, 1 )
          if( self.swapBytes ):
            dataArray.byteswap();
          #Verify the header for this row of data matches what the header specified.
          #We do MAXX * 2 since each value is a short.
          if( dataArray[0] != (self.MAXX*2) ):
            self.lastErrorMsg = 'Trailing tag Byte count: %d for row: %d does not match header: %d.' %( dataArray[0], row, self.MAXX )
            return( False )
      
        if( northmostrowfirst ):
          #Flip the order of the array so northmost ends up as element 0.
          rowList.reverse()
          for x in range(self.MAXY):
            for y in range(self.MAXX):
              val = rowList[x][y]
              #If we are writing the file so the southmost row is first, go head and write the row
              #to the file. Otherwise to have northmost first, we have to invert the rows.
              gridFile.write( '%f ' % val )
            gridFile.write( "\n" )
          
        print( 'Processed %d rows.' % ( row + 1 ) )
        gridFile.flush()
        gridFile.close()            
        
        return( True )
      except Exception, E:
        self.lastErrorMsg = str(E) 

    return( False )

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
      elif( len(row) < 6 ):
        self.logger.error( "Row: '%s' is missing data on line: %d moving to next row" % ( row, self.file.line_num ) )
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
        datetime = "%d-%d %d:%d" % (int(row[1]),int(row[2]),hour,minute)
        datetime = time.strptime( datetime, '%Y-%j %H:%M')
        datetime = time.strftime( '%Y-%m-%dT%H:%M:00', datetime )      
        dataRow.dateTime = datetime
      else:
        self.logger.error( "Missing a date field on line: %d in row: '%s', moving to next row" % ( self.file.line_num, row ) )
        return( dataRow )
        
      if( len(row[4])):
          dataRow.batteryVoltage = float(row[4])
      else:
        self.logger.debug( "Battery voltage field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )

      if( len(row[5])):
        dataRow.programCode = float(row[5])
      else:
        self.logger.debug( "Program Code field empty on line: %d in row: '%s'" % ( self.file.line_num, row ) )
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
from pysqlite2 import dbapi2 as sqlite3
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
  def writePrecip( self, datetime,rain_gauge,batt_voltage,program_code,rainfall ):
    sql = "INSERT INTO precipitation  \
          (date,rain_gauge,batt_voltage,program_code,rainfall ) \
          VALUES( '%s','%s',%3.2f,%.2f,%2.4f );" % (datetime,rain_gauge,batt_voltage,program_code,rainfall)
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
    sql = "SELECT insp_date FROM dhec_beach WHERE station = '%s' ORDER BY datetime(insp_date) ASC" % station
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        dateList.append( row[0] )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.critical( "ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql) )
      sys.exit(-1)
    return(dateList)
  
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
    
    sql = "SELECT  dhec_beach.station,dhec_beach.etcoc,dhec_beach.tide,dhec_beach.salinity,dhec_beach.weather,monitoring_stations.rain_gauge,precip_daily_summary.rainfall \
          FROM dhec_beach,monitoring_stations,precip_daily_summary \
          WHERE \
          dhec_beach.station = monitoring_stations.station AND \
          monitoring_stations.rain_gauge = precip_daily_summary.rain_gauge AND \
          dhec_beach.station = '%s' AND \
          datetime(dhec_beach.insp_date) = datetime('%s') and \
          strftime('%%Y-%%m-%%d', datetime(precip_daily_summary.date)) = strftime('%%Y-%%m-%%d',datetime('%s'))" % ( station,datetime,datetime )
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
          #2 day delay
          sum2daydelay = sum48 - sum24
          #3 day delay
          sum3daydelay = sum72 - sum48
          
          #Get the preceding dry days count, if there are any.
          dryCnt = self.getPrecedingDryDaysCount('2009-02-01', 'nmb1' )
          
          #Write the summary table
          etcoc = -1
          if( beachData['etcoc'] != None and beachData['etcoc'] != '' ):
            etcoc = beachData['etcoc']
          salinity = -1
          if( beachData['salinity'] != None and beachData['salinity'] != ''):
            salinity = beachData['salinity']
          tide = -1
          if( beachData['tide'] != None and beachData['tide'] != '' ):
            tide = beachData['tide']
          rainfall = 0.0
          if( beachData['rainfall'] != None and beachData != '' ):
            rainfall = beachData['rainfall']
          sql = "INSERT INTO station_summary \
                (date,station,rain_gauge,etcoc,salinity,rainfall,tide,rain_summary_24,rain_summary_48,rain_summary_72,weather) \
                VALUES('%s','%s','%s',%s,%s,%.3f,%s,%.3f,%.3f,%.3f,'%s')" % \
                (datetime,station,rainGauge,etcoc,salinity,rainfall,tide,sum24,sum48,sum72,beachData['weather'])
          dbCursor.execute( sql )
          self.dbCon.commit()
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
             datetime(date) <= datetime('%s') AND \
             datetime(date) >= datetime( '%s', '-%d hours' )" % ( rain_gauge, datetime, datetime, prevHourCnt )
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
             datetime(date) <= datetime('%s') AND \
             datetime(date) >= datetime( '%s', '-%d hours' )" % ( rain_gauge, datetime, datetime, prevHourCnt )
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
  
  def getPrecedingDryDaysCount(self, datetime, rainGauge ):
    iDryCnt = 0
    sql = "Select A.date, A.ndx,  A.rainfall, \
            Case \
              When A.rainfall = 0  Then \
                IFNULL( (Select Max(B.ndx) From precip_daily_summary As B WHERE B.ndx < A.ndx AND B.rainfall=0   ), \
                (A.ndx) ) \
              End As grp \
          From precip_daily_summary As A WHERE rain_gauge = '%s' AND strftime('%%Y-%%m-%%d', datetime(date) ) <= strftime('%%Y-%%m-%%d', datetime('%s', '-1 day') ) ORDER BY datetime(date) DESC" % \
          ( rainGauge, datetime )
    try:
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      for row in dbCursor:
        if( row['grp'] != None ):
          iDryCnt += 1
        else:
          break
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
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
    date is the begin date/time of where we want to start getting the rainfall data.
    minutes is the number of minutes we want to collect rainfall for.
  """
  def calcRainfallIntensity(self, rainGauge, date, minutes ):
    rainfallIntensity = -1
    try:
      #Get the entries where there was rainfall for the date, going forward the minutes number of minutes. 
      sql = "SELECT rainfall from precipitation \
              WHERE rain_gauge = '%s' AND \
              datetime(date) >= datetime( '%s' ) AND datetime(date) < datetime( '%s', '%d minutes' ) AND \
              rainfall > 0;" \
              % (rainGauge, date, date, minutes )
      dbCursor = self.dbCon.cursor()
      dbCursor.execute( sql )
      totalRainfall = 0
      numRainEntries = 0
      rainfallIntensity = 0
      for row in dbCursor:
        totalRainfall += row['rainfall']
        numRainEntries += 1
        
      rainfallIntensity = totalRainfall / ( numRainEntries * 10 )
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      self.logger.error( "ErrMsg: %s SQL: '%s'" % (e.args[0], sql) )
      
    return(rainfallIntensity)
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
      self.ftpRainGaugeData()
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
     
if __name__ == '__main__':
  if( len(sys.argv) < 2 ):
    print( "Usage: xmrgFile.py xmlconfigfile")
    sys.exit(-1)    


  dhecData = processDHECRainGauges(sys.argv[1])
  #dhecData.processFiles()  
#  stationList = dhecData.db.getStationNames()
#  for station in stationList:
#    dateList = dhecData.db.getInspectionDates(station)
#    for date in dateList:
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
  
  