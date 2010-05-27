import os
import os.path
import sys
import array
import struct
import csv
import time
import re
import logging
import logging.handlers
#from collections import defaultdict  
import optparse
import math
import gzip
from numpy import zeros
from pysqlite2 import dbapi2 as sqlite3      



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
    self.headerRead = False
    
    self.earthRadius = 6371.2;
    self.startLong   = 105.0;
    self.startLat    = 60.0;
    self.xmesh       = 4.7625;
    self.meshdegs    = (self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh
  
  """
  Function: Reset
  Purpose: Prepares the xmrgFile object for reuse. Resets various variables and closes the currently open file object.
  Parameters: None
  Return: None
  """
  def Reset(self):
    self.fileName = ''
    self.lastErrorMsg = ''
    self.xmrgFile.close()
  
  """
  Function: openFile
  Purpose: Attempts to open the file given in the filePath string. If the file is compressed using gzip, this will uncompress
    the file as well.
  Parameters:
    filePath is a string with the full path to the file to open.
  Return:
    True if successful, otherwise False.
  """
  def openFile(self, filePath):
    self.fileName = filePath
    self.compressedFilepath = ''
    retVal = False
    try:
      #Is the file compressed? If so, we want to uncompress it to a file for use.
      #The reason for not working with the GzipFile object directly is it is not compatible
      #with the array.fromfile() functionality.
      if( self.fileName.rfind('gz') != -1):
        self.compressedFilepath = self.fileName
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
      import traceback      
      self.lastErrorMsg = traceback.format_exc()
      if( loggerName != None ):
        self.logger.error(self.lastErrorMsg)
      else:
        print(self.lastErrorMsg) 
   
    return(retVal)
  
  """
 Function: cleanUp
 Purpose: Called to delete the XMRG file that was just worked with. Can delete the uncompressed file and/or 
  the source compressed file. 
 Parameters:
   deleteFile if True, will delete the unzipped binary file.
   deleteCompressedFile if True, will delete the compressed file the working file was extracted from.
  """
  def cleanUp(self,deleteFile,deleteCompressedFile):
    self.xmrgFile.close()
    if(deleteFile):
      os.remove(self.fileName)
    if(deleteCompressedFile and len(self.compressedFilepath)):
      os.remove(self.compressedFilepath)
    return
    
  """
  Function: readFileHeader
  Purpose: For the open file, reads the header. Call this function first before attempting to use readRow or readAllRows.
    If you don't the file pointer will not be at the correct position.
  Parameters: None
  Returns: True if successful, otherwise False.
  """
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
      srcFileOpen = False  
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
        #buf = array.array('B')
        #buf.fromfile(self.xmrgFile,66)
        #if( self.swapBytes ):
        #  buf.byteswap()
          
        buf = self.xmrgFile.read(66)
        
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
      #Files written June 1997 to 1999  
      elif( byteCnt == 38 ):
        if( self.swapBytes ):
          unpackFmt += '>'
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(38)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
        
      #Files written June 1997 to 1999. I assume there was some bug for this since the source
      #code also was writing out an error message.  
      elif( byteCnt == 37 ):
        if( self.swapBytes ):
          unpackFmt += '>'
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(37)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
        
      #Files written up to June 1997, no 2nd header.  
      elif( byteCnt == ( self.MAXX * 2 ) ):
        if( self.swapBytes ):
          unpackFmt += '>'
        if( loggerName != None ):
          self.logger.info( "Reading pre-1997 format" )
        else:
          print( "Reading pre-1997 format" )        
        srcFileOpen = True
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
          
      if( srcFileOpen ):
        self.headerRead = True
        return( True )

    except Exception, E:
      import traceback      
      self.lastErrorMsg = traceback.format_exc()
      
      if( self.logger != None ):
        self.logger.error(self.lastErrorMsg)
      else:
        print(self.lastErrorMsg)
    
    return( False )      
  
  """
  Function: readRecordTag
  Purpose: Reads the tag that surrounds each record in the file.
  Parameters: None
  Return: An integer dataArray with the tag data if read, otherwise None.
  """
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
  
  """
  Function: readRow
  Purpose: Reads a single row from the file.
  Parameters: None'
  Returns: If successful a dataArray containing the row values, otherwise None.
  """
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
  
  """
  Function: readAllRows
  Purpose: Reads all the rows in the file and stores them in a dataArray object. Data is stored in self.grid.
  Parameters: None
  Returns: True if succesful otherwise False.
  
  """
  def readAllRows(self):
    #Create a integer numeric array(from numpy). Dimensions are MAXY and MAXX.
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
  
  """
  Function: inBBOX
  Purpose: Tests to see if the testLatLong is in the bounding box given by minLatLong and maxLatLong.
  Parameters:
    testLatLong is the lat/long pair we are testing.
    minLatLong is a latLong object representing the bottom left corner.
    maxLatLong is a latLong object representing the upper right corner.
  Returns:
    True if the testLatLong is in the bounding box, otherwise False.
  """  
  def inBBOX(self, testLatLong, minLatLong, maxLatLong):
    inBBOX = False
    if( ( testLatLong.latitude >= minLatLong.latitude and testLatLong.longitude >= minLatLong.longitude ) and
        ( testLatLong.latitude < maxLatLong.latitude and testLatLong.longitude < maxLatLong.longitude ) ):
      inBBOX = True
    return( inBBOX )
  

  """
  Function: hrapCoordToLatLong
  Purpose: Converts the HRAP grid point given in hrapPoint into a latitude and longitude.
  Parameters:  
    hrapPoint is an hrapPoint object that defines the row,col point we are converting.
  Returns:
    A LatLong() object with the converted data.
  """
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

  """
  Function: latLongToHRAP
  Purpose: Converts a latitude and longitude into an HRAP grid point.
  Parameters:  
    latLong is an latLong object that defines the point we are converting.
    roundToNearest specifies if we want to round the hrap point to the nearest integer value.
    adjustToOrigin specifies if we want to adjust the hrap point to the origin of the file.
  Returns:
    A LatLong() object with the converted data.
  """
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
  
  """
  Function: getCollectionDateFromFilename
  Purpose: Given the filename, this will return a datetime string in the format of YYYY-MM-DDTHH:MM:SS.
  Parameters:
    fileName is the xmrg filename to parse the datetime from.
  Return:
    A string representing the date and time in the form: YYYY-MM-DDTHH:MM:SS
  """
  def getCollectionDateFromFilename(self, fileName):
    #Parse the filename to get the data time.
    (directory,filetime) = os.path.split( fileName )
    (filetime,ext) = os.path.splitext( filetime )
    #Let's get rid of the xmrg verbage so we have the time remaining.
    #The format for the time on these files is MMDDYYY sometimes a trailing z or for some historical
    #files, the format is xmrg_MMDDYYYY_HRz_SE. The SE could be different for different regions, SE is southeast.     
    #24 hour files don't have the z, or an hour
    
    dateformat = "%m%d%Y%H" 
    #Regexp to see if we have one of the older filename formats like xmrg_MMDDYYYY_HRz_SE
    fileParts = re.findall("xmrg_\d{8}_\d{1,2}", filetime)
    if(len(fileParts)):
      #Now let's manipulate the string to match the dateformat var above.
      filetime = re.sub("xmrg_", "", fileParts[0])
      filetime = re.sub("_","", filetime)
    else:
      if(filetime.find('24hrxmrg') != -1):
        dateformat = "%m%d%Y"
      filetime = filetime.replace('24hrxmrg', '')
      filetime = filetime.replace('xmrg', '')
      filetime = filetime.replace('z', '')
    #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
    #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
    #I want instead of brining in the calender package which has the conversion.
    secs = time.mktime(time.strptime( filetime, dateformat ))
    #secs -= offset
    filetime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(secs) )
    
    return(filetime)
  
class xmrgDB(object):
  def __init__(self):
    self.db = None
    self.lastErrorMsg = ''
  """
  Function: connect
  Purpose: Connects to the sqlite database file passed in dbFilepath.
  Parameters:
    dbFilepath is the fully qualified path to the sqlite database.
  Returns: True if we successfully connected to the database, otherwise False.
  If an exception occured, the stack trace is written into self.lastErrorMsg.
  
  """  
  def connect(self, dbFilepath, spatiaLiteLibFile=''):
    try:
      self.db = sqlite3.connect( dbFilepath )
      #This enables the ability to manipulate rows with the column name instead of an index.
      self.db.row_factory = sqlite3.Row
      #If the path to the spatialite package was provided, attempt to load the extension.
      if(len(spatiaLiteLibFile)):
        self.db.enable_load_extension(True)
        sql = 'SELECT load_extension("%s");' % (spatiaLiteLibFile)
        cursor = self.executeQuery(sql)
        if(cursor != None):
          return(True)
        else:
          self.lastErrorMsg = "Failed to load SpatiaLite library: %s. Cannot continue." %(spatiaLiteLibFile)    
    except Exception, E:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))     
    return(False)
  
  """
  Function: executeQuery
  Purpose: Executes the sql statement passed in.
  Parameters: 
    sqlQuery is a string containing the query to execute.
  Return: 
    If successfull, a cursor is returned, otherwise None is returned.
  """
  def executeQuery(self, sqlQuery):   
    try:
      dbCursor = self.db.cursor()
      dbCursor.execute( sqlQuery )        
      return( dbCursor )
    except sqlite3.Error, e:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))     
    except Exception, E:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))     
    return(None)

  
  """
  Function: cleanPrecipRadar
  Purpose: This function will remove all data older the olderThanDate from the precipitation_radar table.
  Parameters:
    olderThanDate is the comparison date to use.
  Return: 
    True if successful, otherwise False.
  """
  def cleanUp(self, olderThanDate):
    sql = "DELETE FROM precipitation_radar WHERE collection_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s');" % (olderThanDate)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.db.commit()
        return(True)  
      except sqlite3.Error, e:
        import traceback        
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        
        self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                        exceptionValue,
                                        exceptionTraceback)))     
    return(False)  

  def buildPolygonString(self, polygonPtList):
    if(len(polygonPtList)):
      points = ''
      for point in polygonPtList:
        point = point.lstrip()
        point = point.rstrip()
        point = point.split(' ')
        if(len(points)):
          points += ',' 
        buf = ('%s %s' % (point[0], point[1]))
        points += buf
      return('POLYGON((%s))' % (points))
    return('')
    
  """
  Function: getRadarDataForBoundary
  Purpose: For the given rain gauge(boundaryName), this function will return the radar data that is in that POLYGON.
  Parameters:
    boundaryPolygon is a list of x,y tuples which forms the polygon we use to determine the intersection with the 
      radar polygons.
    strtTime is the datetime to begin the search
    endTime is the datetime to end the search.
  Return:
    Database cursor with the results if query is successful, otherwise None.
  """
  def getRadarDataForBoundary(self, boundaryPolygon,strtTime,endTime):
    polyString = self.buildPolygonString(boundaryPolygon)
    sql = "SELECT latitude,longitude,precipitation FROM precipitation_radar \
            WHERE\
            (collection_date >= '%s' AND collection_date <= '%s') AND\
            Intersects( Geom, \
                        GeomFromText('%s'))"\
            %(strtTime,endTime,polyString)
    return(self.executeQuery(sql))
  
  """
  Function: calculateWeightedAvg
  Purpose: For a given station(rain gauge) this function queries the radar data, gets the grids that fall
   into the watershed of interest and calculates the weighted average.
  Parameters:
    watershedName is the watershed we want to calculate the average for. For ease of use, I use the rain gauge name to 
       name the watersheds.
    startTime is the starting time in YYYY-MM-DDTHH:MM:SS format.
    endTime is the starting time in YYYY-MM-DDTHH:MM:SS format.
  """
  def calculateWeightedAvg(self, boundaryPolygon, startTime, endTime):
    weighted_avg = -9999
    polyString = self.buildPolygonString(boundaryPolygon)
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.      
    sql = "SELECT * FROM(\
           SELECT (Area(Intersection(radar.geom,GeomFromText('%s')))/Area(GeomFromText('%s'))) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar \
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                Intersects(radar.geom, GeomFromText('%s')))"\
                %(polyString, polyString, startTime, endTime, polyString)
    dbCursor = self.executeQuery(sql)        
    if(dbCursor != None):
      total = 0.0
      date = ''
      cnt = 0
      for row in dbCursor:
        percent = row['percent']
        precip = row['precipitation']
        
        total += (percent * precip)
        cnt += 1
      dbCursor.close()
      if(cnt > 0):
        weighted_avg = total
    else:
      weighted_avg = None
    return(weighted_avg)
    
if __name__ == '__main__':   
  try:
    parser = optparse.OptionParser()
    parser.add_option("-d", "--DatabaseFile", dest="databaseFile",
                      help="Full path to the database used to store the imported file." )
    parser.add_option("-s", "--SpatialiteLib", dest="spatialiteLib",
                      help="Full path to the spatialite library. For windows this will be a DLL, for Linux a shared object file." )
    parser.add_option("-f", "--XMRGFile", dest="xmrgFile",
                      help="The XMRG file to process." )
    parser.add_option("-b", "--BBOX", dest="bbox",
                      help="The bounding box to use to select the area of interest from the source XMRG file and store into the database.\
                            If not provided, the entire XMRG file is imported." )
    parser.add_option("-0", "--StoreDryPrecipCells", dest="storeDryPrecipCells", action= 'store_true',
                      help="If set, when importing the XMRG file, cells that had precipitation of 0 will be stored in the database.")
    parser.add_option("-p", "--Polygon", dest="polygon",
                      help="Polygon of interest to use for querying against the radar data." )
    
    (options, args) = parser.parse_args()
    #if( options.xmlConfigFile == None ):
    #  parser.print_usage()
    #  parser.print_help()
    #  sys.exit(-1)
      
    db = xmrgDB()
    if(db.connect(options.databaseFile, options.spatialiteLib) != True):
      print("Unable to connect to database: %s, cannot continue" %(options.databaseFile))


    #Each long/lat pair is seperated by a comma, so let's braek up the input into the pairs.
    bboxParts = options.bbox.split(',')
    minLatLong = LatLong()
    maxLatLong = LatLong()
    #Each long/lat pair is seperated by a space.
    pairs = bboxParts[0].split(' ')
    minLatLong.longitude = float(pairs[0]) 
    minLatLong.latitude = float(pairs[1])
    pairs = bboxParts[1].split(' ')
    maxLatLong.longitude = float(pairs[0]) 
    maxLatLong.latitude = float(pairs[1])
          
    #Open the XMRG file and process the contents, storting the data into the database.
    dataFile = xmrgFile()
    dataFile.openFile(options.xmrgFile)
    if( dataFile.readFileHeader() ):     
      print( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(dataFile.XOR,dataFile.YOR,dataFile.MAXX,dataFile.MAXY))
      if( dataFile.readAllRows() ):
        #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
        #inches , need to divide by 2540.
        dataConvert = 100.0 
        dataConvert = 25.4 * dataConvert 

        #This is the database insert datetime.           
        datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
        #Parse the filename to get the data time.
        (directory,filetime) = os.path.split( dataFile.fileName )
        (filetime,ext) = os.path.splitext( filetime )
        filetime = dataFile.getCollectionDateFromFilename(filetime)


        #Flag to specifiy if any non 0 values were found. No need processing the weighted averages 
        #below if nothing found.
        rainDataFound=False 
        #If we are using a bounding box, let's get the row/col in hrap coords.
        llHrap = None
        urHrap = None
        startCol = 0
        startRow = 0
        #If we are using a bounding box to clip out the input data we are interested in, convert those
        #lat/longs into the HRAP grid to set where we start our import.
        if( minLatLong != None and maxLatLong != None ):
          llHrap = dataFile.latLongToHRAP(minLatLong,True,True)
          urHrap = dataFile.latLongToHRAP(maxLatLong,True,True)
          startCol = llHrap.column
          startRow = llHrap.row
        recsAdded = 0
        for row in range(startRow,dataFile.MAXY):
          for col in range(startCol,dataFile.MAXX):
            val = dataFile.grid[row][col]
            #If there is no precipitation value, or the value is erroneous 
            if( val <= 0 ):
              if(options.storeDryPrecipCells):
                val = 0
              else:
                continue
            else:
              val /= dataConvert
              
            hrap = hrapCoord( dataFile.XOR + col, dataFile.YOR + row )
            latlon = dataFile.hrapCoordToLatLong( hrap )                                
            latlon.longitude *= -1
            saveToDB = False
            if( minLatLong != None and maxLatLong != None ):
              if( dataFile.inBBOX( latlon, minLatLong, maxLatLong ) ):
                saveToDB = True
            else:
              saveToDB = True
            if(saveToDB):
              #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
              #that has each point in the grid for a given point.                  
              hrapNewPt = hrapCoord( dataFile.XOR + col, dataFile.YOR + row + 1)
              latlonUL = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonUL.longitude *= -1
              hrapNewPt = hrapCoord( dataFile.XOR + col + 1, dataFile.YOR + row)
              latlonBR = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonBR.longitude *= -1
              hrapNewPt = hrapCoord( dataFile.XOR + col + 1, dataFile.YOR + row + 1)
              latlonUR = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonUR.longitude *= -1
              wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))"\
                    %(latlon.longitude, latlon.latitude,
                      latlonUL.longitude, latlonUL.latitude, 
                      latlonUR.longitude, latlonUR.latitude, 
                      latlonBR.longitude, latlonBR.latitude, 
                      latlon.longitude, latlon.latitude, 
                      )
              sql = "INSERT INTO precipitation_radar \
                    (insert_date,collection_date,latitude,longitude,precipitation,geom) \
                    VALUES('%s','%s',%f,%f,%f,GeomFromText('%s',4326));" \
                    %( datetime,filetime,latlon.latitude,latlon.longitude,val,wkt)
              cursor = db.executeQuery( sql )
              #Problem with the query, since we are working with transactions, we have to rollback.
              if( cursor != None ):
                recsAdded += 1
              else:
                print(db.lastErrorMsg)
                db.lastErrorMsg = None
                db.db.rollback()
        #Commit the inserts.    
        db.db.commit()
        print('Added: %d records to database.' % (recsAdded))

        #Now let's take the polygon of interest, find out all the radar cells that intersect it, then 
        #calculate a weighted average.
        polygonPtList = options.polygon.split(',')
        radarCursor = db.getRadarDataForBoundary(polygonPtList, filetime, filetime)
        if(radarCursor != None):
          for row in radarCursor:
            print( "Longitude: %s Latitude: %s PrecipValue: %s" % (row['latitude'],row['longitude'],row['precipitation']))
        weightedAvg = db.calculateWeightedAvg(polygonPtList, filetime, filetime)
        print("Weighted Average: %f" %(weightedAvg))
  except Exception, E:
    import traceback
    print( traceback.print_exc() )
        