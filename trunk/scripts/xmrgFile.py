import os
import sys
import array
import struct
import csv
import time
import logging
#import logging.handlers
from collections import defaultdict  
import optparse
import math
from numpy import zeros

try:
  from osgeo import gdal
  from osgeo import ogr
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
  def __init__(self):
    self.fileName = ''
    self.lastErrorMsg = ''
    self.srcFileOpen = 0
    
    self.earthRadius = 6371.2;
    self.startLong   = 105.0;
    self.startLat    = 60.0;
    #self.raddeg      = 180 / math.pi;
    #self.deg2rad     = math.pi / 180
    self.xmesh       = 4.7625;
    #self.tlat        = 60.0 / self.raddeg;
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
  
  def writeLatLonCSVFile(self,outputFile,northmostrowfirst=True,units='inches', newCellSize = None, minLatLong=None, maxLatLong=None):
    if( self.readFileHeader() ):
      try:
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
        #If we are using a bounding box to clip out a region of interest, instead of working with lats/longs
        #convert them to an HRAP coord. Saves the expense of converting every point from the file from HRAP to
        #a lat/long.
        llHrap = None
        urHrap = None
        if( minLatLong != None and maxLatLong != None ):
          llHrap = self.latLongToHRAP(minLatLong)
          #Adjust the row/column to the grid origin.
          llHrap.column = int( llHrap.column - 0.5 ) - self.XOR 
          llHrap.row = int( llHrap.row - 0.5 ) - self.YOR
          urHrap = self.latLongToHRAP(maxLatLong)
          urHrap.column = int( urHrap.column - 0.5 ) - self.XOR
          urHrap.row = int( urHrap.row - 0.5 ) - self.YOR
          
        if( newCellSize ):           
          grid = zeros([self.MAXY,self.MAXX],int)

        for row in range( self.MAXY ):
        
          #Read a row.
          dataArray= self.readRow()
          if( dataArray == None ):
            return(False)
          col = 0                       
          for val in dataArray:            
            if( newCellSize ):
              grid[row][col] = val
            else:
              if( val < 0 ):
                val = -9999.0
              else:
                val /= dataConvert
  
              hrap = hrapCoord( self.XOR + col, self.YOR + row )              
              latlon = self.hrapCoordToLatLong( hrap )              
              latlon.longitude *= -1
              writeLine = True
              if( minLatLong != None and maxLatLong != None ):
                if( ( latlon.latitude >= minLatLong.latitude and latlon.longitude >= minLatLong.longitude ) and
                    ( latlon.latitude < maxLatLong.latitude and latlon.longitude < maxLatLong.longitude ) ):
              #if( llHrap != None ):                
              #  if( ( ( col >= llHrap.column ) and ( row >= llHrap.row ) ) and
              #      ( ( col < urHrap.column ) and ( row < urHrap.row  ) ) ):
                  #Calc lat/long coords from HRAP.
                  #hrap = hrapCoord( self.XOR + col, self.YOR + row )              
                  #latlon = self.hrapCoordToLatLong( hrap )              
                  #latlon.longitude *= -1
                  writeLine = True
                else:
                  writeLine = False
                  
              if( writeLine ):                      
                gridFile.write( '%f,%f,%f,%d,%d\n' % (latlon.latitude, latlon.longitude, val,(self.YOR + row),(self.XOR + col)) )                
           
            col+=1
          
        if( newCellSize ):
          endProcessing = False           
          spacing = (1.0 / newCellSize)
          #If we are using a bounding box, let's start going through the grid at the bbox's 
          #row and column instead of running through the whole grid to get to that point
          startCol = 0
          startRow = 0
          if( llHrap != None ):
            startCol = llHrap.column
            startRow = llHrap.row
          for row in range(startRow,self.MAXY):
            if( row < self.MAXY - 1):
              for col in range(startCol,self.MAXX):
                if( col < self.MAXX - 1 ):
                  for i in range( newCellSize ):
                    for j in range( newCellSize ):
                      x = spacing * i 
                      y = spacing * j     
                      z0 = grid[row][col]
                      z1 = grid[row][col+1]
                      z2 = grid[row+1][col]
                      z3 = grid[row+1][col+1]
                      val = 0
                      if( z0 != 0 and z1 != 0 and z3 != 0 ):
                        val = self.biLinearInterpolatePoint( x,y, z0, z1, z2, z3 )                            
                      val /= dataConvert          

                      hrap = hrapCoord( self.XOR + col + x, self.YOR + row + y )              
                      latlon = self.hrapCoordToLatLong( hrap )              
                      latlon.longitude *= -1
                      
                      writeLine = True
                      if( minLatLong != None and maxLatLong != None ):
                        if( ( latlon.latitude >= minLatLong.latitude and latlon.longitude >= minLatLong.longitude ) and
                            ( latlon.latitude < maxLatLong.latitude and latlon.longitude < maxLatLong.longitude ) ):
                      #if( llHrap != None ):                
                      #  if( ( ( col >= llHrap.column ) and ( row >= llHrap.row ) ) and
                      #      ( ( col < urHrap.column ) and ( row < urHrap.row  ) ) ):
                          #Calc lat/long coords from HRAP.
                      #    hrap = hrapCoord( self.XOR + col + x, self.YOR + row + y )              
                      #    latlon = self.hrapCoordToLatLong( hrap )              
                      #    latlon.longitude *= -1
                          writeLine = True
                        else:
                          writeLine = False
                          
                      if( writeLine ):                      
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

           
        print( 'Processed %d rows.' % ( row + 1 ) )
  
      except Exception, E:
        self.lastErrorMsg = str(E)
        print( str(E) ) 
    
    return( False )
  
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

  def latLongToHRAP(self, latLong):
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
  
if __name__ == '__main__':   
  parser = optparse.OptionParser()
  parser.add_option("-f", "--XMRGFile", dest="xmrgFile",
                    help="XMRG Data file to process." )
  (options, args) = parser.parse_args()
     
       
  xmrg = xmrgFile()
  xmrg.openFile( options.xmrgFile, 0 )
  ndx = options.xmrgFile.rfind( '.' )
  gridDestFile = options.xmrgFile
  asciiDestFile = options.xmrgFile
  if( ndx != -1 ):
    parts = options.xmrgFile.rsplit('.', 1 )
    gridDestFile = parts[0] + '-grid.asc'
    asciiDestFile = parts[0] + '-latlon.csv'
  else:
    gridDestFile += '-grid.asc'
    asciiDestFile += '-latlon.csv'
    
  #xmrg.writeASCIIGrid( 'hrap', gridDestFile)
  #xmrg.Reset()
  #xmrg.openFile( options.xmrgFile, 0 )
  minLL = LatLong(33.354303,-79.803447)
  maxLL = LatLong(33.984628,-78.09772)
  #xmrg.writeLatLonCSVFile( asciiDestFile, True, 'inches', None, minLL, maxLL )
  #xmrg.Reset()
  #xmrg.openFile( options.xmrgFile, 0 )
  xmrg.writeLatLonCSVFile( asciiDestFile, True, 'inches', 47, minLL, maxLL )
  
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
  
