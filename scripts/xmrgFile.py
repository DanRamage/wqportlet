import os
import sys
import array
import struct
import csv
import time
import logging
import logging.handlers
from collections import defaultdict  
import optparse
import math
import gzip
import traceback
from numpy import zeros
from lxml import etree
sys.path.insert(0, "C:\\Documents and Settings\\dramage\\workspace\\BeachAdvisory") 
from dhecRainGaugeProcessing import dhecDB
from dhecRainGaugeProcessing import dhecConfigSettings
from xeniatools import getRemoteFiles

#try:
#  from osgeo import gdal
#  from osgeo import ogr
# from osgeo.gdalconst import *
#  gdal.TermProgress = gdal.TermProgress_nocb
#except ImportError:
#  import gdal
#  from gdalconst import *  
#try:
#  import numpy as Numeric
#  Numeric.arrayrange = Numeric.arange
#except ImportError:
#  import Numeric


class hrapCoord(object):
  def __init__(self, column=None, row=None):
    self.column = column
    self.row    = row
class LatLong(object):
  def __init__(self,lat=None,long=None):
    self.latitude = lat
    self.longitude= long
    
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
  def __init__(self, loggerName=None):
    self.loggerName = loggerName
    if( loggerName != None ):
      self.logger = logging.getLogger(loggerName)
      self.logger.debug("creating an instance of xmrgFile")
    
    self.fileName = ''
    self.lastErrorMsg = ''
    self.srcFileOpen = 0
    
    self.earthRadius = 6371.2;
    self.startLong   = 105.0;
    self.startLat    = 60.0;
    self.xmesh       = 4.7625;
    self.meshdegs    = (self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh
  
  def Reset(self):
    self.fileName = ''
    self.lastErrorMsg = ''
    self.srcFileOpen = 0
    self.xmrgFile.close()
    
  def openFile(self, filePath, useLog):
    self.fileName = filePath
    retVal = False
    try:
      #Is the file compressed? If so, we want to uncompress it to a file for use.
      #The reason for not working with the GzipFile object directly is it is not compatible
      #with the array.fromfile() functionality.
      if( self.fileName.rfind('gz') ):
        #SPlit the filename from the extension.
        parts = self.fileName.split('.')
        self.fileName = parts[0] 
        self.xmrgFile = open( self.fileName, mode = 'wb' )
        zipFile = gzip.GzipFile( filePath, 'rb' )
        contents = zipFile.read()
        self.xmrgFile.writelines(contents)
        self.xmrgFile.close()

      self.xmrgFile = open( self.fileName, mode = 'rb' )
      retVal = True
    except Exception, E:
      self.lastErrorMsg = str(E)
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
      
      if( loggerName != None ):
        self.logger.error( str(E) )
      else:
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
        if( loggerName != None ):
          self.logger.info( "Reading pre-1997 format" )
        else:
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
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
      
      if( self.logger != None ):
        self.logger.error(self.lastErrorMsg)
      else:
        print(self.lastErrorMsg)
    
    return( False )      
  
  def readRecordTag(self):
    dataArray= array.array('I')
    dataArray.fromfile( self.xmrgFile, 1 )
    if( self.swapBytes ):
      dataArray.byteswap();
    #Verify the header for this row of data matches what the header specified.
    #We do MAXX * 2 since each value is a short.
    if( dataArray[0] != (self.MAXX*2) ):
      self.lastErrorMsg = 'Trailing tag Byte count: %d for row: %d does not match header: %d.' %( dataArray[0], row, self.MAXX )
      return( None )
    return(dataArray)
  
  def readRow(self):
    #Read off the record header
    tag = self.readRecordTag()
    if( tag == None ):
      return(None)
    
    #Read a columns worth of data out
    dataArray= array.array('h')
    dataArray.fromfile( self.xmrgFile, self.MAXX )
    #Need to byte swap?
    if( self.swapBytes ):
      dataArray.byteswap();    

    #Read off the record footer.
    tag = self.readRecordTag()
    if( tag == None ):
      return(None)

    return( dataArray )
  
  def readAllRows(self):
    self.grid = zeros([self.MAXY,self.MAXX],int)
    for row in range( self.MAXY ):    
      dataArray= self.readRow()
      if( dataArray == None ):
        return(False)
      col = 0                    
      for val in dataArray:            
        self.grid[row][col] = val
        col+=1
              
    return(True)
    
  def inBBOX(self, testLatLong, minLatLong, maxLatLong):
    inBBOX = False
    if( ( testLatLong.latitude >= minLatLong.latitude and testLatLong.longitude >= minLatLong.longitude ) and
        ( testLatLong.latitude < maxLatLong.latitude and testLatLong.longitude < maxLatLong.longitude ) ):
      inBBOX = True
    return( inBBOX )
  
  def writeLatLonDB(self, dbFile, minLatLong=None, maxLatLong=None,newCellSize=None,interpolate=False):
    import os.path

    db = dhecDB(dbFile, self.loggerName)     
    if( self.readFileHeader() ):     
      self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(self.XOR,self.YOR,self.MAXX,self.MAXY))
      try:
        if( self.readAllRows() ):
          
          #This is the database insert datetime.           
          datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
          #Parse the filename to get the data time.
          (directory,filetime) = os.path.split( self.fileName )
          (filetime,ext) = os.path.splitext( filetime )
          #Let's get rid of the xmrg verbage so we have the time remaining.
          #The format for the time on these files is MMDDYYY sometimes a trailing z for
          #the UTC time zone. 24 hour files don't have the z, or an hour           
          filetime = filetime.replace('24hrxmrg', '')
          filetime = filetime.replace('xmrg', '')
          dateformat = "%m%d%Y%Hz" 
          if( filetime.rfind( 'z' ) == -1 ):
            dateformat = "%m%d%Y"  
          #The XMRG time is UTC, however we want to store all our times as localtimes.
          isdst = time.localtime()[-1]
          offset = 0
          if (isdst):            
            offset = 4 * 3600
          else:
            offset = 5 * 3600
          #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
          #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
          #I want instead of brining in the calender package which has the conversion.
          secs = time.mktime(time.strptime( filetime, dateformat ))
          secs -= offset
          filetime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(secs) )
          #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
          #inches , need to divide by 2540.
          dataConvert = 100.0 
          dataConvert = dataConvert * 25.4

          #If we are using a bounding box, let's get the row/col in hrap coords.
          llHrap = None
          urHrap = None
          startCol = 0
          startRow = 0
          if( minLatLong != None and maxLatLong != None ):
            llHrap = self.latLongToHRAP(minLatLong,True,True)
            urHrap = self.latLongToHRAP(maxLatLong,True,True)
            startCol = llHrap.column
            startRow = llHrap.row
          recsAdded = 0
          if( newCellSize == None ):
            for row in range(startRow,self.MAXY):
              for col in range(startCol,self.MAXX):
                val = self.grid[row][col]
                #If there is no precipitation value, or the value is erroneous 
                if( val <= 0 ):
                  #val = -9999.0
                  continue
                else:
                  val /= dataConvert
                  
                hrap = hrapCoord( self.XOR + col, self.YOR + row )
                latlon = self.hrapCoordToLatLong( hrap )                                
                latlon.longitude *= -1
                insertSQL = False
                if( minLatLong != None and maxLatLong != None ):
                  if( self.inBBOX( latlon, minLatLong, maxLatLong ) ):
                    insertSQL = True
                else:
                  insertSQL = True
                if( insertSQL ):
                  wkt = "POINT(%f %f)" %(latlon.longitude, latlon.latitude)
                  sql = "INSERT INTO precipitation_radar \
                        (wkt_geometry,insert_date,collection_date,latitude,longitude,precipitation) \
                        VALUES('%s','%s','%s',%f,%f,%f);" \
                        %( wkt,datetime,filetime,latlon.latitude,latlon.longitude,val)
                  cursor = db.executeQuery( sql )
                  #Problem with the query, since we are working with transactions, we have to rollback.
                  if( cursor == None ):
                    self.logger.error( db.lastErrorMsg )
                    db.lastErrorMsg = None
                    db.dbCon.rollback()
                  recsAdded += 1
          #We are resizing the grid.
          else:
            endProcessing = False           
            spacing = (1.0 / newCellSize)
            #If we are using a bounding box, let's start going through the grid at the bbox's 
            #row and column instead of running through the whole grid to get to that point
            for row in range(startRow,self.MAXY):
              if( row < self.MAXY - 1):
                for col in range(startCol,self.MAXX):
                  if( col < self.MAXX - 1 ):
                    for i in range( newCellSize ):
                      for j in range( newCellSize ):
                        x = spacing * i 
                        y = spacing * j     
                        #Are we interpolating the data?
                        if( interpolate ):  
                          z0 = self.grid[row][col]
                          z1 = self.grid[row][col+1]
                          z2 = self.grid[row+1][col]
                          z3 = self.grid[row+1][col+1]
                          val = 0
                          #If all the data points are 0, no need to run the interpolation.
                          if( z0 != 0 and z1 != 0 and z3 != 0 ):
                            val = self.biLinearInterpolatePoint( x,y, z0, z1, z2, z3 )
                        else:
                          val = self.grid[row][col] 
                        #If there is no precipitation value, or the value is erroneous 
                        if( val <= 0 ):
                          #val = -9999.0
                          continue
                        else:                          
                          val /= dataConvert          
  
                        hrap = hrapCoord( self.XOR + col + x, self.YOR + row + y )              
                        latlon = self.hrapCoordToLatLong( hrap )              
                        latlon.longitude *= -1
                        insertSQL = False   
                        #If we are using a bounding box, let's see if the coordinate is inside it.               
                        if( minLatLong != None and maxLatLong != None ):
                          if( self.inBBOX( latlon, minLatLong, maxLatLong ) ):
                            insertSQL = True
                        else:
                          insertSQL = True
                        if( insertSQL ):
                          wkt = "POINT(%f %f)" %(latlon.longitude, latlon.latitude)
                          sql = "INSERT INTO precipitation_radar \
                                (wkt_geometry,insert_date,collection_date,latitude,longitude,precipitation) \
                                VALUES('%s','%s','%s',%f,%f,%f);" \
                                %( wkt,datetime,filetime,latlon.latitude,latlon.longitude,val)
                          cursor = db.executeQuery( sql )
                          #Problem with the query, since we are working with transactions, we have to rollback.
                          if( cursor == None ):
                            self.logger.error( db.lastErrorMsg )
                            db.lastErrorMsg = None
                            db.dbCon.rollback()
                          recsAdded += 1
                            
                        if( urHrap != None ):
                          #If we are out of the bounding box area, we can stop the loop.
                          if( ( col > urHrap.column ) and ( row > urHrap.row  ) ):
                            endProcessing = True
                            break                
                      if( endProcessing ):
                        break                                      
                  if( endProcessing ):
                    break
              if( endProcessing ):
                break      
          #Commit the inserts.    
          db.dbCon.commit()
          if( self.logger != None ):
            self.logger.info( "Processed: %d rows. Added: %d records to database." %((row + 1),recsAdded))
          else:
            print( 'Processed %d rows. Added: %d records to database.' % (row + 1),recsAdded )
  
      except Exception, E:
        self.lastErrorMsg = str(E)
        info = sys.exc_info()        
        excNfo = traceback.extract_tb(info[2],1)
        items = excNfo[0]
        self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
        if(self.logger != None):
          self.logger.error(self.lastErrorMsg)
        else:
          print(self.lastErrorMsg)
        
        return(False)
      return(True)
  def writeLatLonCSVFile(self,outputFile,units='inches', minLatLong=None, maxLatLong=None,newCellSize=None,interpolate=False):
    if( self.readFileHeader() ):
      try:
        if( self.readAllRows() ):
          gridFile = open( outputFile, "wt")  
          dataConvert = 1
          #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
          #inches , need to divide by 2540.
          if( units == 'inches' ):
            dataConvert = 100.0 
            dataConvert = dataConvert * 25.4
            gridFile.write( 'Latitude,Longitude,Precipitation(inches)\n')      
          else:
            gridFile.write( 'Latitude,Longitude,Precipitation(hundreths of mm)\n')      
          #If we are using a bounding box, let's get the row/col in hrap coords.
          llHrap = None
          urHrap = None
          startCol = 0
          startRow = 0
          if( minLatLong != None and maxLatLong != None ):
            llHrap = self.latLongToHRAP(minLatLong,True)
            urHrap = self.latLongToHRAP(maxLatLong,True)
            startCol = llHrap.column
            startRow = llHrap.row
      
          if( newCellSize == None ):
            for row in range(startCol,self.MAXY):
              for col in range(startRow,self.MAXX):
                val = self.grid[row][col]
                if( val < 0 ):
                  val = -9999.0
                else:
                  val /= dataConvert
                  hrap = hrapCoord( self.XOR + col, self.YOR + row )
                  latlon = self.hrapCoordToLatLong( hrap )              
                  latlon.longitude *= -1                  
                  if( self.inBBOX( latlon, minLatLong, maxLatLong ) ):
                    gridFile.write( '%f,%f,%f\n' % (latlon.latitude, latlon.longitude, val ) )                
                                            
          #We are resizing the grid.
          else:
            endProcessing = False           
            spacing = (1.0 / newCellSize)
            #If we are using a bounding box, let's start going through the grid at the bbox's 
            #row and column instead of running through the whole grid to get to that point
            for row in range(startRow,self.MAXY):
              if( row < self.MAXY - 1):
                for col in range(startCol,self.MAXX):
                  if( col < self.MAXX - 1 ):
                    for i in range( newCellSize ):
                      for j in range( newCellSize ):
                        x = spacing * i 
                        y = spacing * j     
                        #Are we interpolating the data?
                        if( interpolate ):  
                          z0 = self.grid[row][col]
                          z1 = self.grid[row][col+1]
                          z2 = self.grid[row+1][col]
                          z3 = self.grid[row+1][col+1]
                          val = 0
                          #If all the data points are 0, no need to run the interpolation.
                          if( z0 != 0 and z1 != 0 and z3 != 0 ):
                            val = self.biLinearInterpolatePoint( x,y, z0, z1, z2, z3 )
                        else:
                          val = self.grid[row][col]                           
                        val /= dataConvert          
  
                        hrap = hrapCoord( self.XOR + col + x, self.YOR + row + y )              
                        latlon = self.hrapCoordToLatLong( hrap )              
                        latlon.longitude *= -1                  
                        if( self.inBBOX( latlon, minLatLong, maxLatLong ) ):
                          gridFile.write( '%f,%f,%f\n' % (latlon.latitude, latlon.longitude, val ) )
                        
                        #print( "row: %d(%f) col: %d(%f) x: %f y: %f z0: %f z1: %f z2: %f z3: %f Interp: %f" \
                        #      %(row,latlon.latitude,col,latlon.longitude,x,y,z0,z1,z2,z3,val) )\
                              
                        if( urHrap != None ):
                          #If we are out of the bounding box area, we can stop the loop.
                          if( ( col > urHrap.column ) and ( row > urHrap.row  ) ):
                            endProcessing = True
                            break                
                      if( endProcessing ):
                        break                                      
                  if( endProcessing ):
                    break
              if( endProcessing ):
                break           
          if(loggerName != None):
            self.logger.info( "Processed %d rows." % ((row + 1)))
          else:
            print( "Processed %d rows." % ((row + 1)))
  
      except Exception, E:
        self.lastErrorMsg = str(E)
        info = sys.exc_info()        
        excNfo = traceback.extract_tb(info[2],1)
        items = excNfo[0]
        self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
        
        if(loggerName != None):
          self.logger.error(self.lastErrorMsg)
        else:
          print( str(E) )
           
  def hrapCoordToLatLong(self, hrapPoint ):
    latLong     = LatLong()
        
    x = hrapPoint.column - 401.0;
    y = hrapPoint.row - 1601.0;
    rr = x * x + y * y
    #gi = ((self.earthRadius * (1.0 + math.sin(self.tlat))) / self.xmesh)
    #gi *= gi
    #gi = ((self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh)
    gi = self.meshdegs * self.meshdegs
    #latLong.latitude = math.asin((gi - rr) / (gi + rr)) * self.raddeg
    latLong.latitude = math.degrees(math.asin((gi - rr) / (gi + rr)))
    
    #ang = math.atan2(y,x) * self.raddeg
    ang = math.degrees(math.atan2(y,x))
    
    if(ang < 0.0):
      ang += 360.0;
    latLong.longitude = 270.0 + self.startLong - ang;
    
    if(latLong.longitude < 0.0):
      latLong.longitude += 360.0;
    elif(latLong.longitude > 360.0):
      latLong.longitude -= 360.0;
    
    return( latLong )

  def latLongToHRAP(self, latLong, roundToNearest=False, adjustToOrigin=False):
    flat = math.radians( latLong.latitude )
    flon = math.radians( abs(latLong.longitude) + 180.0 - self.startLong )
    r = self.meshdegs * math.cos(flat)/(1.0 + math.sin(flat))
    x = r * math.sin(flon)
    y = r * math.cos(flon)
    hrap = hrapCoord( x + 401.0, y + 1601.0 )
    
    #Bounds checking
    if( hrap.column > ( self.XOR + self.MAXX ) ):
      hrap.column = self.XOR + self.MAXX
    if( hrap.row > (self.YOR + self.MAXY) ):
      hrap.row = self.YOR + self.MAXY
    if( roundToNearest ):
      hrap.column = int( hrap.column - 0.5 ) 
      hrap.row = int( hrap.row - 0.5 )
    if( adjustToOrigin ):
      hrap.column -= self.XOR 
      hrap.row -= self.YOR
    
    return(hrap)
  
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
          #rowList = [ [0 for y in range(self.MAXX)] for x in range(self.MAXY) ]
          rowList = zeros([self.MAXY,self.MAXX],int)
        for row in range( self.MAXY ):

          #Read off the record header
          dataArray = self.readRecordTag()
          
          #Read a columns worth of data out
          dataArray=self.readRow()
          
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
          dataArray = self.readRecordTag()
      
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
        info = sys.exc_info()        
        excNfo = traceback.extract_tb(info[2],1)
        items = excNfo[0]
        self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
        
        if(loggerName != None):
          self.logger.error(self.lastErrorMsg)
        

    return( False )   
    
  def biLinearInterpolatePoint(self, x, y, z0, z1, z2, z3):
    z = None
    # z3------z2
    # |       |
    # |       |
    # z0------z1
    #b1 + b2x + b3y + b4xy 
    #b1 = z0
    #b2 = z1-z0
    #b3 = z3-z0
    #b4 = z0-z1-z3+z2
    
    b1 = z0
    b2 = z1 - z0
    b3 = z3 - z0
    b4 = z0 - z1 - z3 + z2
    z = b1 + (b2*x) + (b3*y) + (b4*x*y)

    return(z)

class processXMRGData(object):
  
  def __init__(self, xmlConfigFile):
    try:
      self.loggerName = 'dhec_logger'
  
      self.configSettings = dhecConfigSettings( xmlConfigFile )
      
      #Create our logging object.
      if(self.configSettings.logFile == None):
        print( 'ERROR: //logging/logDir not defined in config file. Terminating script' )
        sys.exit(-1)     
  
      self.logger = logging.getLogger(self.loggerName)
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
                 
      if(self.configSettings.spatiaLiteLib == None ):
        self.logger.error( 'ERROR: //database/db/spatiaLiteLib not defined in config file. Terminating script' )
        sys.exit(-1)           
      if(self.configSettings.baseURL == None):
        self.logger.error( "//xmrgData/baseURL not defined, cannot continue." )
        sys.exit(-1)
      if(self.configSettings.xmrgDLDir == None):
        self.configSettings.xmrgDLDir  = './';         
        self.logger.debug( "//xmrgData/downloadDir not provided, using './'." )
        
    except Exception, E:
      self.lastErrorMsg = str(E)
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
      if(self.logger != None):
        self.logger.error(self.lastErrorMsg)
      else:
        print( self.lastErrorMsg )
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
    
  def buildXMRGFilename(self, desiredDateTime):
    desiredTime=time.strptime(desiredDateTime, "%Y-%m-%dT%H:00:00")
    #Internally we stores date/times as localtime, however the xmrg remote files are stamped as UTC
    #times so we have to convert.
    desiredTime=time.mktime(desiredTime)
    desiredTime=time.gmtime(desiredTime)

    #Hourly filename format is: xmrgMMDDYYYYHHz.gz WHERE MM is month, DD is day, YYYY is year
    #HH is UTC hour     
    hour = time.strftime( "%H", desiredTime)
    date = time.strftime( "%m%d%Y", desiredTime)
    fileName = 'xmrg%s%sz.gz' % ( date,hour )
    return(fileName)
    
  def getXMRGFile(self, fileName):
    return(self.remoteFileDL.getFile( fileName, None, False))
      
  def getLatestHourXMRGData(self,writeToDB=True,writeCSVFile=False,writeASCIIGrid=False,backFillGaps=True):
    
    try: 
      self.remoteFileDL = getRemoteFiles.remoteFileDownload( self.configSettings.baseURL, self.configSettings.xmrgDLDir, 'b', False, None, True)
      
      #Clean out any data older than 24 hours.
      db = dhecDB(self.configSettings.dbSettings['dbName'], self.loggerName)
      time24HoursAgo = time.time() - ( 24 * 60 * 60 ) #Current time minus 24 hours worth of seconds.
      currentDateTime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime(time24HoursAgo))
      db.cleanPrecipRadar(currentDateTime)
      
      dateList=[]
      #The latest completed hour will be current hour - 1.
      hr = time.time()-3600
      latestHour = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(hr))
      #add it to our list to process
      dateList.append(latestHour)
      
      #Are we going to try to backfill any gaps in the data?
      if(backFillGaps):
        #Generate what the last 24 hour date/times should be.
        baseTime = time.time()-3600
        for x in range(23):
          datetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(baseTime - ((x+1) * 3600)))          
          dateList.append(datetime)
        sql = "SELECT DISTINCT(collection_date) as date FROM precipitation_radar;"
        dbCursor = db.executeQuery(sql)
        if(dbCursor != None):
          #Now we'll loop through and pull any date from the datebase that matches a date in our list
          #from the list. This gives us our gaps.
          for row in dbCursor:
            dbDate = row['date'] 
            for x in range(len(dateList)):
              if(dbDate == dateList[x]):
                dateList.pop(x)
                break          
      db.dbCon.close()
      
      for date in dateList:
        fileName = self.buildXMRGFilename(date)
        #Now we try to download the file.
        fileName = self.getXMRGFile(fileName)
        if( fileName != None ):
          self.logger.info( "Processing XMRG File: %s" %(fileName))
          xmrg = xmrgFile( self.loggerName )
          xmrg.openFile( fileName, 0 )
              
          minLL = LatLong(31.53,-83.3)
          maxLL = LatLong(36.39, -74.1)
          if( minLL != None and maxLL != None ):
            self.logger.debug( "Using BBOX. LL-Latitude %f LL-Longitude: %f UR-Latitude: %f UR-Longitude: %f" %(minLL.latitude,minLL.longitude,maxLL.latitude,maxLL.longitude))
          if( writeToDB ):
            xmrg.writeLatLonDB( self.configSettings.dbSettings['dbName'], minLL, maxLL )
            #Test the spatialite lib.
            #db = dhecDB(self.dbPath, self.loggerName)
            #db.loadSpatiaLiteLib( self.spatiaLiteLib )     
            #sql = 'SELECT latitude,longitude,precipitation FROM precipitation_radar \
            #        WHERE Contains( GeomFromText( \'POLYGON((-79.238743 33.592447, -78.994153 33.747115, -78.882832 33.554469, -79.104116 33.454099, -79.238743 33.592447))\'), GeomFromText(wkt_geometry) )'
            #cursor = db.executeQuery( sql )
            #if( cursor != None ):
            #  outFile = open( "C:\\Documents and Settings\\dramage\\workspace\\SVNSandbox\\wqportlet\\trunk\\data\\spatialitetext.csv", "wt")  
            #  for row in cursor:
            #    outFile.write( '%f,%f,%f\n' %( row['latitude'],row['longitude'],row['precipitation']) )
            #  outFile.close()
              
          if( writeCSVFile ):
            destFile = options.xmrgFile
            if( ndx != -1 ):
              destFile = parts[0] + '-latlon.csv'
            else:
              destFile += '-latlon.csv'        
            xmrg.Reset()
            xmrg.openFile( options.xmrgFile, 0 )
            xmrg.writeLatLonCSVFile( destFile, True, 'inches', minLL, maxLL )
            
          if( writeASCIIGrid ):
            destFile = options.xmrgFile
            if( ndx != -1 ):
              destFile = parts[0] + '-grid.txt'
            else:
              destFile += '-grid.txt'        
            xmrg.Reset()
            xmrg.openFile( options.xmrgFile, 0 )
        else:
          self.logger.error( "Unable to download file: %s" %(fileName))
          
    except Exception, E:
      self.lastErrorMsg = str(E) 
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])      
      self.logger.error( self.lastErrorMsg )
    return(None)
  
if __name__ == '__main__':   
  try:
    parser = optparse.OptionParser()
    parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                      help="Configuration file." )
    (options, args) = parser.parse_args()
    if( options.xmlConfigFile == None ):
      parser.print_usage()
      parser.print_help()
      sys.exit(-1)
    xmrgData = processXMRGData(options.xmlConfigFile)
    xmrgData.getLatestHourXMRGData(True)
  except Exception, E:
    info = sys.exc_info()        
    excNfo = traceback.extract_tb(info[2],1)
    items = excNfo[0]    
    print( str(E) +" File: %s Line: %d Function: %s" % (items[0],items[1],items[2]) )
  
       
  
  #try:
    #srcGridFile = gdal.Open(gridDestFile, GA_Update)
    #geotransform = srcGridFile.GetGeoTransform()
    #retVal = srcGridFile.SetProjection( "Polar_Stereographic" )
    #srcGridFile.FlushCache()
    #band = srcGridFile.GetRasterBand(1)
    #print 'Size is ',srcGridFile.RasterXSize,'x',srcGridFile.RasterYSize,'x',srcGridFile.RasterCount
    #print 'Projection is ',srcGridFile.GetProjectionRef()
    #print 'Origin = (',geotransform[0], ',',geotransform[3],')'
    #print 'Pixel Size = (',geotransform[1], ',',geotransform[5],')'
    #print 'Converting band number',1,'with type',gdal.GetDataTypeName(band.DataType)    
    
      
    #outFile = driver.Create( destFile + '2', srcGridFile.RasterXSize, srcGridFile.RasterYSize, srcGridFile.RasterCount, gdal.GDT_Byte )

  #except Exception, E:
  #  print( 'ERROR: ' + str(E) ) 
  
