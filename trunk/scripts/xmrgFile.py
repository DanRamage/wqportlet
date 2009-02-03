import os
import array
import struct
import csv
import time
from lxml import etree

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
      self.lastErrorMsg = ('file %s, line %d: %s' % (filename, reader.line_num, e))
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
      #1st entry is rain gauge ID
      dataRow.ID = row[0]
      #Array entries 1-3 are: Year, Julien day, time in military minutes. We convert these
      #into a datetime format. There are 2400 time entries, one of them is the days summary, the other
      #is another 10minute interval.      
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
      
      dataRow.ID = row[0]
      dataRow.dateTime = datetime
      dataRow.batteryVoltage = row[4]
      dataRow.programCode = row[5]
      dataRow.rainfall = float(row[6])
      print( 'Processing line: %d %s' % ( self.file.line_num, row ) )
      return(dataRow)
      #No more rows to iterate.
   
    except csv.Error, e:
      self.lastErrorMsg = ('File %s. Line %d: %s' % (self.filePath, self.file.line_num, e))
            
    return(dataRow)

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
    try:
      xmlTree = etree.parse(xmlConfigFile)
      dbPath = xmlTree.xpath( '//database/db/name' )[0].text 
      #self.dbCon = dbConnection
      #Get a file list for the directory.
      self.fileDirectory = xmlTree.xpath( '//rainGaugeProcessing/rainGaugeFileDir' )[0].text 
      self.fileList = os.listdir( self.fileDirectory )

      self.rainGaugeInfo = {}
      rainGauge = xmlTree.xpath( '//environment/rainGaugeProcessing/rainGaugeList')
      for child in rainGauge[0].getchildren():
        name = child.xpath( 'name' )[0].text 
        id = child.xpath( 'fileID' )[0].text 
        summaryid = child.xpath( 'file24hrSumId' )[0].text
        self.rainGaugeInfo[name] = [('id', id),('summaryid', summaryid)] 
      
    except OSError, e:
      print( str(e) )
    except Exception, e:
      print( str(e) )
  """
  Function: setFileList
  Purpose: Allows us to override the fileList of csv files to process. 
  Parameters:
    fileList is a list of csv files to process.
  """
  def setFileList(self, fileList ):
    self.fileList = fileList  
    
  """
  Function: processFiles
  Purpose: Loops through the fileList and processes the dhec data files. The data is then stored
    into the database.
  Parameters:None
  Return:    
  """
  def processFiles(self):    
    for file in self.fileList:
      try:     
        fullPath = self.fileDirectory + '\\' + file
        print( "Begin processing file: %s" % fullPath )
        rainGaugeFile = readRainGaugeData()
        rainGaugeFile.openFile( fullPath )
        dataRow = rainGaugeFile.processLine()
        #Get the row id and the summary id.
        rainGaugeId = file.split('.')
        id = self.rainGaugeInfo[rainGaugeId[0]]    
        while( dataRow != None ):
          if( dataRow != None ):
            i = 0
          else:
            break                       
          dataRow = rainGaugeFile.processLine()    
      except StopIteration,e:
        print( 'EOF file: %s' % file )

if __name__ == '__main__':
  
  dhecData = processDHECRainGauges("C:\\Documents and Settings\\dramage\workspace\\SVNSandbox\\wqportlet\\trunk\\scripts\\config.xml")
  dhecData.processFiles()
  
  xmrg = xmrgFile()
  #inputFile = "C:\\Temp\\xmrg0506199516z\\xmrg0506199516z"
  inputFile = "C:\\Temp\\xmrg0129200918z\\xmrg0129200918z"
  xmrg.openFile( inputFile, 0 )
  xmrg.writeASCIIGrid( 'polarstereo', 'C:\\Temp\\xmrg0129200918z\\xmrg0129200918z.asc')
  #xmrg.writeASCIIGrid( 'hrap', 'C:\\Temp\\xmrg0506199516z\\xmrg0506199516z.asc')
  
  try:
    srcGridFile = gdal.Open('C:\\Temp\\xmrg0129200918z\\xmrg0129200918z.asc', GA_ReadOnly)
    geotransform = srcGridFile.GetGeoTransform()
    band = srcGridFile.GetRasterBand(1)
    print 'Size is ',srcGridFile.RasterXSize,'x',srcGridFile.RasterYSize,'x',srcGridFile.RasterCount
    print 'Projection is ',srcGridFile.GetProjection()
    print 'Origin = (',geotransform[0], ',',geotransform[3],')'
    print 'Pixel Size = (',geotransform[1], ',',geotransform[5],')'
    print 'Converting band number',1,'with type',gdal.GetDataTypeName(band.DataType)    
       
  except Exception, E:
    print( 'ERROR: ' + str(E) ) 
  
  