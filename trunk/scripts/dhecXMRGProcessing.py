"""
Revisions:
Date: 2011-07-28
Function: dhecXMRGProcessing.vacuumDB
Changes: Added function to vacuum database. Previously we were using the rain gauge processing class, however since
we've moved away from using rain gauges, I want to retire that code base.

Date: 2011-07-25
Function: dhecXMRGProcessing.__init__
Changes: Added member variable processingStartTime to denote the processing start time. Use if for the row_entry_date column  
  when adding records to the database.
Function: calculateWeightedAverage
Changes: Use the processingStartTime member to set the rowEntryDate parameter in the addMeasurement db function call. 

Date: 2011-06-23
Function: getLatestHourXMRGData
Changes: Implemented datetime object use to clean up the date calculations.
Pass the database connection into the writeLatLonDb function instead of having it create the object every loop.
"""
import sys
import os
import traceback
import optparse
import time
import re
if(sys.platform == "win32"):
  sys.path.insert(0, "C:\Documents and Settings\dramage\workspace\BeachAdvisory")
#from xmrgFile import xmrgFile,processXMRGData,hrapCoord,LatLong
from xmrgFile import xmrgFile,hrapCoord,LatLong
from processXMRGFile import processXMRGData
from dhecDB import dhecDB
from dhecRainGaugeProcessing import processDHECRainGauges
from xeniatools import getRemoteFiles


class baseExportClass(object):   
  def openOutputFile(self,filename,fileDir):
    return(False)
  def writeRecord(self):
    return(False)

class dhecXMRGProcessing(processXMRGData):
  def __init__(self,xmlConfigFile):
    try: 
      #Call base class init.
      processXMRGData.__init__(self,xmlConfigFile)
      bbox = self.configSettings.getEntry('//xmrgData/processingSettings/bbox')
      self.minLL = None
      self.maxLL = None
      if(bbox != None):
        latLongs = bbox.split(';')
        self.minLL = LatLong()
        self.maxLL = LatLong()
        latlon = latLongs[0].split(',')
        self.minLL.latitude = float( latlon[0] )
        self.minLL.longitude = float( latlon[1] )
        latlon = latLongs[1].split(',')
        self.maxLL.latitude = float( latlon[0] )
        self.maxLL.longitude = float( latlon[1] )
  
      #Delete data that is older than the LastNDays
      self.xmrgKeepLastNDays = self.configSettings.getEntry('//xmrgData/processingSettings/keepLastNDays')
      if(self.xmrgKeepLastNDays != None):
        self.xmrgKeepLastNDays = int(self.xmrgKeepLastNDays)
      #Try to fill in any holes in the data going back N days.
      self.backfillLastNDays = self.configSettings.getEntry('//xmrgData/processingSettings/backfillLastNDays')
      if(self.backfillLastNDays != None):
        self.backfillLastNDays = int(self.backfillLastNDays)

      self.writePrecipToDB = self.configSettings.getEntry('//xmrgData/processingSettings/writeToDB')
      if(self.writePrecipToDB != None):
        self.writePrecipToDB = int(self.writePrecipToDB)
      else:
        self.writePrecipToDB = 1
      self.writePrecipToShapefile = self.configSettings.getEntry('//xmrgData/processingSettings/writeToShapefile')
      if(self.writePrecipToShapefile != None):
        self.writePrecipToShapefile = int(self.writePrecipToShapefile)        
      else:        
        self.writePrecipToShapefile = 1
      #If we are going to write shapefiles, get the output directory.
      if(self.writePrecipToShapefile):
        self.shapefileDir = self.configSettings.getEntry('//xmrgData/processingSettings/shapeFileDir')
        if(len(self.shapefileDir) == 0):
          self.writePrecipToShapefile = 0
          if(self.logger != None):
            self.logger.error("No shapefile directory provided, will not write shapefiles.")
                
      self.saveAllPrecipVals = self.configSettings.getEntry('//xmrgData/processingSettings/saveAllPrecipVals')
      if(self.saveAllPrecipVals != None):
        self.saveAllPrecipVals = int(self.saveAllPrecipVals)
      else:
        self.saveAllPrecipVals = 0
      self.createPolygonsFromGrid = self.configSettings.getEntry('//xmrgData/processingSettings/createPolygonsFromGrid')
      if(self.createPolygonsFromGrid != None):
        self.createPolygonsFromGrid = int(self.createPolygonsFromGrid)
      else:
        self.createPolygonsFromGrid = 1
      
      #Flag to specify if we want to delete the compressed XMRG file when we are done processing.
      #We might not be working off a compressed source file, so this flag only applies to a compressed file.
      self.deleteCompressedSourceFile = self.configSettings.getEntry('//xmrgData/processingSettings/deleteCompressedSourceFile')
      if(self.deleteCompressedSourceFile != None):
        self.deleteCompressedSourceFile = int(self.deleteCompressedSourceFile)
      else:
        self.deleteCompressedSourceFile = 0

      #Flag to specify if we want to delete the XMRG file when we are done processing.
      self.deleteSourceFile = self.configSettings.getEntry('//xmrgData/processingSettings/deleteSourceFile')
      if(self.deleteSourceFile != None):
        self.deleteSourceFile = int(self.deleteSourceFile)
      else:
        self.deleteSourceFile = 0

      #Directory to import XMRG files from 
      self.importDirectory = self.configSettings.getEntry('//xmrgData/processingSettings/importDirectory')
      if(self.importDirectory != None):
        self.importDirectory = self.importDirectory
      else:
        self.importDirectory = ""
        
      #Flag to specify if we want to calculate the weighted averages for the watersheds as we write the radar data
      #into the precipitation_radar table.
      self.calcWeightedAvg = self.configSettings.getEntry('//xmrgData/processingSettings/calculateWeightedAverage')
      if(self.calcWeightedAvg != None):
        self.calcWeightedAvg = int(self.calcWeightedAvg)
      else:
        self.calcWeightedAvg = 0
      
      #2011-07-25 DWR
      #Added a processing start time to use for the row_entry_date value when we add new records to the database.
      self.processingStartTime = time.strftime('%Y-%d-%m %H:%M:%S', time.localtime())
    except Exception, E:
      self.lastErrorMsg = str(E) 
      if(self.logger != None):
        self.logger.exception(E)
      else:
        print(traceback.print_exc())

  def importFiles(self, importDirectory=None):
    try:
      if(importDirectory == None):
        importDirectory = self.importDirectory
      
      db = dhecDB(self.configSettings.dbSettings['dbName'], None, self.logger)
      if(self.logger != None):
        self.logger.debug("Loading spatialite: %s" %(self.configSettings.spatiaLiteLib))
      if(db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib) == False):
        if(self.logger != None):
          self.logger.debug("Error loading: %s Error: %s" %(self.configSettings.spatiaLiteLib,db.lastErrorMsg))
      
      startMonth = 'Apr'
      endMonth   = 'Oct'
      #Get a list of the files in the import dir.
      fileList = os.listdir(importDirectory)
      #If we want to skip certain months, let's pull those files out of the list.
      monthList = {'Jan': 1, 'Feb': 2, 'Mar': 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12 }
      startMonth = monthList[startMonth]
      endMonth = monthList[endMonth]
              
      for fileName in fileList:    
        fileTime = self.getCollectionDateFromFilename(fileName)
        #Get the month from the time.
        month = time.strptime(fileTime, "%Y-%m-%dT%H:%M:%S")
        month = int(time.strftime("%m", month))
        #If the file is outside the month range we are interested in, go on to the next file. 
        if(month < startMonth or month > endMonth):
          continue
        fullPath = "%s/%s" %(importDirectory,fileName)  
        #Make sure we are trying to import a file and not a directory.
        if(os.path.isfile(fullPath) != True):
          self.logger.debug("%s is not a file, skipping" % (fullPath))
          continue          
        if( self.processXMRGFile(fullPath, db)):
          self.logger.debug("Successfully processed: %s" %(fileName))
        else:
          self.logger.error("Unable to process: %s" %(fileName))
      db.DB.close()                
    except Exception, E:
      self.lastErrorMsg = str(E) 
      if(self.logger != None):
        self.logger.exception(E)
      else:
        print(traceback.print_exc())
      
  def getLatestHourXMRGData(self):   
    import datetime
    from datetime import tzinfo
    from pytz import timezone

    try: 
      self.remoteFileDL = getRemoteFiles.remoteFileDownload( self.configSettings.baseURL, self.configSettings.xmrgDLDir, 'b', False, None, True, self.logger)

      #Clean out any data older than xmrgKeepLastNDays.
      db = dhecDB(self.configSettings.dbSettings['dbName'], None, self.logger)
      if(self.logger != None):
        self.logger.debug("Loading spatialite: %s" %(self.configSettings.spatiaLiteLib))
      if(db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib) == False):
        if(self.logger != None):
          self.logger.debug("Error loading: %s Error: %s" %(self.configSettings.spatiaLiteLib,db.lastErrorMsg))
            
      
      #DWR 2011-06-23
      #Since we process on the previous day, make the starting time the beginning of the current day.
      #Use the datetime object to improve the code readability.
      nowTime = datetime.datetime.now(timezone('UTC'))
      curDay = nowTime
      curDay = curDay.replace(hour=0, minute=0, second=0,microsecond=0)
      
      #Clean up data older than xmrgKeepLastNDays
      olderThan = curDay - datetime.timedelta(days=self.xmrgKeepLastNDays)
      db.cleanPrecipRadar(olderThan.strftime("%Y-%m-%dT%H:%M:%S"))
      #Current time minus N days worth of seconds.
      #timeNHoursAgo = time.time() - ( self.xmrgKeepLastNDays * 24 * 60 * 60 ) 
      #currentDateTime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime(timeNHoursAgo))
      #db.cleanPrecipRadar(currentDateTime)
            
      dateList=[]
      #The latest completed hour will be current hour - 1.
      #hr = time.time()-3600
      #latestHour = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(hr))
      latestHour = nowTime - datetime.timedelta(hours=1)
      latestHour = latestHour.strftime("%Y-%m-%dT%H:00:00")
      #add it to our list to process
      dateList.append(latestHour)
      
      #Are we going to try to backfill any gaps in the data?
      if(self.backfillLastNDays):
        #baseTime = time.time()-3600
        baseTime = nowTime - datetime.timedelta(hours=1)
        #Now let's build a list of the last N hours of data we should have to see if we have any holes
        #to fill.
        lastNHours = self.backfillLastNDays * 24
        for x in range(lastNHours):
          hr = x + 1
          dateTime = baseTime - datetime.timedelta(hours=hr)
          dateList.append(dateTime.strftime("%Y-%m-%dT%H:00:00"))
          #DWR 2011-06-23
          #This is incorrect, was building the list with local time where data in the database is in UTC.
          #datetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(baseTime - ((x+1) * 3600)))          
          #dateList.append(datetime)
        sql = "SELECT DISTINCT(collection_date) as date FROM precipitation_radar ORDER BY collection_date DESC;"
        dbCursor = db.executeQuery(sql)
        if(dbCursor != None):
          #Now we'll loop through and pull any date from the database that matches a date in our list
          #from the list. This gives us our gaps.
          for row in dbCursor:
            dbDate = row['date'] 
            for x in range(len(dateList)):
              if(dbDate == dateList[x]):
                dateList.pop(x)
                break          
          dbCursor.close()
                    
      if(self.logger != None):
        self.logger.debug( "Date/times missing in XMRG database: %s" % (dateList))
      for date in dateList:
        xmrgFilename = self.buildXMRGFilename(date,False)
        if(self.logger != None):
          self.logger.info( "Date: %s XMRG Filename: %s" %(date,xmrgFilename))
        #Now we try to download the file.
        fileName = self.getXMRGFile(xmrgFilename)
        if( fileName != None ):
          if(self.logger != None):
            self.logger.info( "Processing XMRG File: %s" %(fileName))
          self.processXMRGFile(fileName, db)
        else:
          self.logger.error( "Date: %s Unable to download file: %s" %(date, xmrgFilename))
          
    except Exception, E:
      self.lastErrorMsg = str(E) 
      if(self.logger != None):
        self.logger.exception(E)
      else:
        print(traceback.print_exc())
    
    db.DB.close()

    return(None)

  def processXMRGFile(self,fileName, db=None):
    if( self.minLL != None and 
        self.maxLL != None ):
      self.logger.debug( "Using BBOX. LL-Latitude %f LL-Longitude: %f UR-Latitude: %f UR-Longitude: %f"\
                          %(self.minLL.latitude,self.minLL.longitude,self.maxLL.latitude,self.maxLL.longitude))
    if(self.writePrecipToDB):
      if(self.writeLatLonDB( fileName, self.configSettings.dbSettings['dbName'], self.minLL, self.maxLL, db ) == False):
        return(False)
    if(self.writePrecipToShapefile):
      if(self.writeShapefile( fileName, self.minLL, self.maxLL ) == False):
        return(False)
    return(True)
  
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
  
  def writeShapefile(self, fileName, minLatLong=None, maxLatLong=None):
    import osgeo.ogr
    import osgeo.osr
  
    xmrg = xmrgFile( self.configSettings.loggerName )
    xmrg.openFile( fileName )
    if( xmrg.readFileHeader() ):     
      self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
      try:        
        if( xmrg.readAllRows() ):
          
          spatialReference = osgeo.osr.SpatialReference()
          spatialReference.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
          driver = osgeo.ogr.GetDriverByName('ESRI Shapefile')

          #This is the database insert datetime.           
          datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
          #Parse the filename to get the data time.
          (directory,filetime) = os.path.split( xmrg.fileName )
          (filetime,ext) = os.path.splitext( filetime )
          #Check to see if any of the shapefile files exist, if so delete them otherwise
          #we can't create the shapefile.
          shapeFilename = '%s/%s.shp' %(self.shapefileDir,filetime)
          if(os.path.exists(shapeFilename)):
            os.remove(shapeFilename)
          shapeData = driver.CreateDataSource(shapeFilename)
          if(shapeData == None):
            self.logger.error("Unable to create shapefile: %s" %(shapeData))
            return

          layer = shapeData.CreateLayer("xmrg", spatialReference, osgeo.ogr.wkbPolygon)
          # create a field for the precipitation value.
          fieldDefn = osgeo.ogr.FieldDefn('FID', osgeo.ogr.OFTInteger)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)
          # create a field for the precipitation value.
          fieldDefn = osgeo.ogr.FieldDefn('precip', osgeo.ogr.OFTReal)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)

          fieldDefn = osgeo.ogr.FieldDefn('latitude', osgeo.ogr.OFTReal)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)
          fieldDefn = osgeo.ogr.FieldDefn('longitude', osgeo.ogr.OFTReal)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)

          fieldDefn = osgeo.ogr.FieldDefn('HRAPX', osgeo.ogr.OFTInteger)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)
          fieldDefn = osgeo.ogr.FieldDefn('HRAPY', osgeo.ogr.OFTInteger)
          # add the field to the shapefile
          layer.CreateField(fieldDefn)

          layerDefinition = layer.GetLayerDefn()
          """
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
          """          
          filetime = self.getCollectionDateFromFilename(filetime)

          #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
          #inches , need to divide by 2540.
          dataConvert = 100.0 
          dataConvert = 25.4 * dataConvert 

          #If we are using a bounding box, let's get the row/col in hrap coords.
          llHrap = None
          urHrap = None
          startCol = 0
          startRow = 0
          if( minLatLong != None and maxLatLong != None ):
            llHrap = xmrg.latLongToHRAP(minLatLong,True,True)
            urHrap = xmrg.latLongToHRAP(maxLatLong,True,True)
            startCol = llHrap.column
            startRow = llHrap.row
          recsAdded = 0
          featureId = 0
          for row in range(startRow,xmrg.MAXY):
            for col in range(startCol,xmrg.MAXX):
              val = xmrg.grid[row][col]
              #If there is no precipitation value, or the value is erroneous 
              if( val <= 0 ):
                if(self.saveAllPrecipVals):
                  val = 0
                else:
                  continue                                    
              else:
                val /= dataConvert
                
              hrap = hrapCoord( xmrg.XOR + col, xmrg.YOR + row )
              latlon = xmrg.hrapCoordToLatLong( hrap )                                
              latlon.longitude *= -1
              insertSQL = False
              if( minLatLong != None and maxLatLong != None ):
                if( xmrg.inBBOX( latlon, minLatLong, maxLatLong ) ):
                  insertSQL = True
              else:
                insertSQL = True
              if( insertSQL ):
                #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                #that has each point in the grid for a given point.                  
                hrapNewPt = hrapCoord( xmrg.XOR + col, xmrg.YOR + row + 1)
                latlonUL = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUL.longitude *= -1
                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row)
                latlonBR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonBR.longitude *= -1
                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row + 1)
                latlonUR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUR.longitude *= -1
                     
                # Create ring for polygon
                ring = osgeo.ogr.Geometry(osgeo.ogr.wkbLinearRing)
                polygon = osgeo.ogr.Geometry(osgeo.ogr.wkbPolygon)
                #polygon.ImportFromWkt(wkt)
                ring.AddPoint(latlon.longitude, latlon.latitude)
                ring.AddPoint(latlonUL.longitude, latlonUL.latitude)
                ring.AddPoint(latlonUR.longitude, latlonUR.latitude)
                ring.AddPoint(latlonBR.longitude, latlonBR.latitude)
                ring.AddPoint(latlonBR.longitude, latlonBR.latitude)
                polygon.AddGeometry(ring)

                # Create feature
                feature = osgeo.ogr.Feature(layerDefinition)
                feature.SetGeometry(polygon)
                #feature.SetFID(featureId)
                feature.SetField('FID', featureId)
                feature.SetField('precip', val)
                feature.SetField('latitude', latlon.latitude)
                feature.SetField('longitude', latlon.longitude)
                feature.SetField('HRAPX', hrap.column)
                feature.SetField('HRAPY', hrap.row)

                # Save feature
                layer.CreateFeature(feature)

                featureId += 1                            
      except Exception, E:
        self.lastErrorMsg = str(E)
        if(self.logger != None):
          self.logger.exception(E)
        else:
          print(traceback.print_exc())
        return(False)
      
      xmrg.cleanUp(self.deleteSourceFile,self.deleteCompressedSourceFile)
      return(True)

  def writeLatLonDB(self, fileName, dbFile, minLatLong=None, maxLatLong=None,db=None):

    if(self.logger != None):
      self.logger.debug("writeLatLonDB File: %s BBOX: %f,%f %f,%f"\
                        %(fileName,minLatLong.latitude,minLatLong.longitude,maxLatLong.latitude,maxLatLong.longitude))
    #Database connection not supplied, so create it.
    if(db == None):
      db = dhecDB(dbFile, self.configSettings.loggerName)     
      if(self.logger != None):
        self.logger.debug("Loading spatialite: %s" %(self.configSettings.spatiaLiteLib))
      if(db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib) == False):
        if(self.logger != None):
          self.logger.debug("Error loading: %s Error: %s" %(self.configSettings.spatiaLiteLib,db.lastErrorMsg))

    xmrg = xmrgFile( self.configSettings.loggerName )
    xmrg.openFile( fileName )
    
    if( xmrg.readFileHeader() ):     
      self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
      try:
        if( xmrg.readAllRows() ):
          
          #This is the database insert datetime.           
          datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
          #Parse the filename to get the data time.
          (directory,filetime) = os.path.split( xmrg.fileName )
          (filetime,ext) = os.path.splitext( filetime )
          filetime = self.getCollectionDateFromFilename(filetime)
          #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
          #inches , need to divide by 2540.
          dataConvert = 100.0 
          dataConvert = 25.4 * dataConvert 

          #Flag to specifiy if any non 0 values were found. No need processing the weighted averages 
          #below if nothing found.
          rainDataFound=False 
          #If we are using a bounding box, let's get the row/col in hrap coords.
          llHrap = None
          urHrap = None
          startCol = 0
          startRow = 0
          if( minLatLong != None and maxLatLong != None ):
            llHrap = xmrg.latLongToHRAP(minLatLong,True,True)
            urHrap = xmrg.latLongToHRAP(maxLatLong,True,True)
            startCol = llHrap.column
            startRow = llHrap.row
          recsAdded = 0
          for row in range(startRow,xmrg.MAXY):
            for col in range(startCol,xmrg.MAXX):
              val = xmrg.grid[row][col]
              #If there is no precipitation value, or the value is erroneous 
              if( val <= 0 ):
                if(self.saveAllPrecipVals):
                  val = 0
                else:
                  continue
              else:
                val /= dataConvert
                
              hrap = hrapCoord( xmrg.XOR + col, xmrg.YOR + row )
              latlon = xmrg.hrapCoordToLatLong( hrap )                                
              latlon.longitude *= -1
              insertSQL = False
              if( minLatLong != None and maxLatLong != None ):
                if( xmrg.inBBOX( latlon, minLatLong, maxLatLong ) ):
                  insertSQL = True
              else:
                insertSQL = True
              if( insertSQL ):
                rainDataFound = True
                #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                #that has each point in the grid for a given point.                  
                hrapNewPt = hrapCoord( xmrg.XOR + col, xmrg.YOR + row + 1)
                latlonUL = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUL.longitude *= -1
                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row)
                latlonBR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonBR.longitude *= -1
                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row + 1)
                latlonUR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUR.longitude *= -1
                wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))"\
                      %(latlon.longitude, latlon.latitude,
                        latlonUL.longitude, latlonUL.latitude, 
                        latlonUR.longitude, latlonUR.latitude, 
                        latlonBR.longitude, latlonBR.latitude, 
                        latlon.longitude, latlon.latitude, 
                        )
                #wkt = "POINT(%f %f)" %(latlon.longitude, latlon.latitude)
                sql = "INSERT INTO precipitation_radar \
                      (insert_date,collection_date,latitude,longitude,precipitation,geom) \
                      VALUES('%s','%s',%f,%f,%f,GeomFromText('%s',4326));" \
                      %( datetime,filetime,latlon.latitude,latlon.longitude,val,wkt)
                cursor = db.executeQuery( sql )
                #Problem with the query, since we are working with transactions, we have to rollback.
                if( cursor == None ):
                  self.logger.error( db.lastErrorMsg )
                  db.lastErrorMsg = None
                  db.DB.rollback()
                recsAdded += 1
          #Commit the inserts.    
          db.commit()
          if( self.logger != None ):
            self.logger.info( "Processed: %d rows. Added: %d records to database." %((row + 1),recsAdded))
          else:
            print( 'Processed %d rows. Added: %d records to database.' % (row + 1),recsAdded )
          #NOw calc the weighted averages for the watersheds and add the measurements to the multi-obs table
          if(rainDataFound and self.calcWeightedAvg):
            self.calculateWeightedAverages(filetime,filetime,db,True)
          else:
            i = 0
      except Exception, E:
        self.lastErrorMsg = str(E)
        if(self.logger != None):
          self.logger.exception(E)
        else:
          print(traceback.print_exc())
        return(False)
      
      xmrg.cleanUp(self.deleteSourceFile,self.deleteCompressedSourceFile)
      return(True)
        
  def calculateWeightedAverages(self,startDate,endDate,dbConnection,addSensor=False, rainGaugeList=None):
    try:
      if(self.logger != None):
        self.logger.debug("calculateWeightedAverages StartDate: %s EndDate: %s" %(startDate,endDate))
      #If we didn't get a list of the rain gauges, create one.
      if(rainGaugeList == None):
        #Get all the raingauges.
        where = "WHERE platform_handle LIKE '%raingauge%'"
        rainGauges = dbConnection.getPlatforms(where)
        rainGaugeList = []
        #We create a list of the gauges. The reason I don't loop through the cursor is if we 
        #have the addSensor set to True and we add a sensor and commit the changes to the database,
        #our open cursor is no longer valid.
        for rainGauge in rainGauges:
          rainGaugeList.append(rainGauge['short_name'])
      recsAdded = False
      for rainGauge in rainGaugeList:  
        platformHandle = "nws.%s.radar" %(rainGauge)
        #Get the info about the rain gauge. We need the lat/long for adding the measurement into the 
        #multi_obs table.
        platformCursor = dbConnection.getPlatformInfo(platformHandle)
        if(platformCursor != None):
          nfo = platformCursor.fetchone()
          if(nfo != None):
            #Calculate the weighted averages and add into multi obs table.
            avg = dbConnection.calculateWeightedAvg(rainGauge, startDate, endDate)            
            if(avg != None):
              if(avg > 0.0 or self.saveAllPrecipVals):                    
                if(avg != -9999):
                  mVals = []
                  mVals.append(avg)
                  
                  if(addSensor):
                    dbConnection.addSensor('precipitation_radar_weighted_average', 'in', platformHandle, 1, 0, 1, None, False)        
                  #Add the avg into the multi obs table. Since we are going to deal with the hourly data for the radar and use
                  #weighted averages, instead of keeping lots of radar data in the radar table, we calc the avg and 
                  #store it as an obs in the multi-obs table.
                  if(dbConnection.addMeasurement('precipitation_radar_weighted_average', 'in',
                                                     platformHandle,
                                                     startDate,
                                                     nfo['fixed_latitude'], nfo['fixed_longitude'],
                                                     0,
                                                     mVals,
                                                     1,
                                                     False,
                                                     self.processingStartTime) != True):
                    if(self.logger != None):
                      self.logger.error( "%s"\
                                         %(dbConnection.getErrorInfo()) )
                    dbConnection.clearErrorInfo()
                    return(False)
                  else:
                    if(self.logger != None):
                      self.logger.debug( "Platform: %s added weighted avg: %f." %(platformHandle,avg) )                                 
                  recsAdded = True
                else:
                  if(self.logger != None):
                    self.logger.debug( "Platform: %s weighted avg: %f is not valid, not adding to database." %(platformHandle,avg) )
              else:
                if(self.logger != None):
                  self.logger.debug( "Platform: %s configuration parameter not set to add precip values of 0.0." %(platformHandle))                                                         
            else:
              if(self.logger != None):
                self.logger.error( "Weighted AVG error: %s" %(dbConnection.getErrorInfo()) )
                dbConnection.clearErrorInfo()
                  
        else:
          if(self.logger != None):
            self.logger.error( "Platform: %s not found. Cannot add measurement." %(platformHandle) )
            
      if(recsAdded):
        dbConnection.commit()
                  
      return(True)
    except Exception, E:
      self.lastErrorMsg = str(E)
      if(self.logger != None):
        self.logger.exception(E)
      else:
        print(self.lastErrorMsg)        
      return(False)

          
  def createWatershedSummaries(self):
    if(self.logger != None):
      self.logger.debug("Entering createWatershedSummaries.")
      
    db = dhecDB(self.configSettings.dbSettings['dbName'], None, self.logger)
    if(self.logger != None):
      self.logger.debug("Loading spatialite: %s" %(self.configSettings.spatiaLiteLib))
    if(db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib) == False):
      if(self.logger != None):
        self.logger.debug("Error loading: %s Error: %s" %(self.configSettings.spatiaLiteLib,db.lastErrorMsg))
      
    #Get the unique dates for the radar data.
    sql = "SELECT DISTINCT(collection_date) as date FROM precipitation_radar ORDER BY collection_date ASC;"
    rainDatesCursor = db.executeQuery(sql)            
    dates = []
    for dateRow in rainDatesCursor:         
      date = dateRow['date']
      dates.append(date)
    #Get all the raingauges.
    where = "WHERE platform_handle LIKE '%raingauge%'"
    rainGauges = db.getPlatforms(where)
    rainGaugeList = []
    #We create a list of the gauges. The reason I don't loop through the cursor is if we 
    #have the addSensor set to True and we add a sensor and commit the changes to the database,
    #our open cursor is no longer valid.
    for rainGauge in rainGauges:
      rainGaugeList.append(rainGauge['short_name'])
      
    rainDatesCursor.close()
    #Get the geom for the watershed boundary.
    for date in dates:       
      self.calculateWeightedAverages(date,date,db,True,rainGaugeList)   
    db.DB.close()
    
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """  
  def vacuumDB(self):
    from stat import *
    
    retVal = False
    if(self.logger != None):
      stats = os.stat(self.configSettings.dbSettings['dbName'])
      self.logger.debug("Begin database vacuum. File size: %d" % (stats[ST_SIZE]))      
    db = dhecDB(self.configSettings.dbSettings['dbName'], None, self.logger)
    if(db.vacuumDB() != None):
      if(self.logger != None):
        stats = os.stat(self.configSettings.dbSettings['dbName'])
        self.logger.debug("Database vacuum completed. File size: %d" % (stats[ST_SIZE]))      
      retVal = True
    else:
      self.logger.error("Database vacuum failed: %s" % (db.lastErrorMsg))
      db.lastErrorMsg = ""
    db.DB.close()
    return(retVal)
  
    
if __name__ == '__main__':   
  try:
    import psyco
    psyco.full()
        
    parser = optparse.OptionParser()
    parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                      help="Configuration file." )
    parser.add_option("-l", "--GetLatest", dest="getLatest", action= 'store_true',
                      help="Get latest XMRG files." )
    parser.add_option("-i", "--ImportXMRGDir", dest="importXMRGDir", action= 'store_true',
                      help="Directory with XMRG files to import." )
    parser.add_option("-s", "--CreateWatershedRadarSummaries", dest="createRadarSummaries", action= 'store_true',
                      help="For each rain gauge, create the watershed summary for each day there is data in the precipitation_radar table." )
    (options, args) = parser.parse_args()
    if( options.xmlConfigFile == None ):
      parser.print_usage()
      parser.print_help()
#      sys.exit(-1)
    xmrgData = dhecXMRGProcessing(options.xmlConfigFile)
    if(options.getLatest):
      xmrgData.getLatestHourXMRGData()
    elif(options.importXMRGDir):      
      xmrgData.importFiles()
    elif(options.createRadarSummaries):
      xmrgData.createWatershedSummaries()
      
      
  except Exception, E:
    print( traceback.print_exc() )
    