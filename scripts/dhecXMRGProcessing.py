import sys
import os
import traceback
import optparse
import time
sys.path.insert(0, "C:\\Documents and Settings\\dramage\\workspace\\BeachAdvisory") 
from xmrgFile import xmrgFile,processXMRGData,hrapCoord,LatLong
from dhecRainGaugeProcessing import dhecDB
from dhecRainGaugeProcessing import processDHECRainGauges
from xeniatools import getRemoteFiles

import osgeo.ogr
import osgeo.osr

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
      
    except Exception, E:
      self.lastErrorMsg = str(E) 
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])      
      self.logger.error( self.lastErrorMsg )
    

  def getLatestHourXMRGData(self):    
    try: 
      self.remoteFileDL = getRemoteFiles.remoteFileDownload( self.configSettings.baseURL, self.configSettings.xmrgDLDir, 'b', False, None, True)

      #Clean out any data older than xmrgKeepLastNDays.
      db = dhecDB(self.configSettings.dbSettings['dbName'], self.loggerName)
            
      #Current time minus N days worth of seconds.
      timeNHoursAgo = time.time() - ( self.xmrgKeepLastNDays * 24 * 60 * 60 ) 
      currentDateTime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime(timeNHoursAgo))
      db.cleanPrecipRadar(currentDateTime)
            
      dateList=[]
      #The latest completed hour will be current hour - 1.
      hr = time.time()-3600
      latestHour = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(hr))
      #add it to our list to process
      dateList.append(latestHour)
      
      #Are we going to try to backfill any gaps in the data?
      if(self.backfillLastNDays):
        baseTime = time.time()-3600
        #Now let's build a list of the last N hours of data we should have to see if we have any holes
        #to fill.
        lastNHours = self.backfillLastNDays * 24
        for x in range(lastNHours):
          datetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(baseTime - ((x+1) * 3600)))          
          dateList.append(datetime)
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
      db.DB.close()
      
      for date in dateList:
        fileName = self.buildXMRGFilename(date)
        #Now we try to download the file.
        fileName = self.getXMRGFile(fileName)
        if( fileName != None ):
          self.logger.info( "Processing XMRG File: %s" %(fileName))
          #xmrg = xmrgFile( self.loggerName )
          #xmrg.openFile( fileName )
          self.processXMRGFile( fileName )
        else:
          self.logger.error( "Unable to download file: %s" %(fileName))
          
    except Exception, E:
      self.lastErrorMsg = str(E) 
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])      
      self.logger.error(self.lastErrorMsg)
    return(None)

  def processXMRGFile(self,fileName):
    if( self.minLL != None and 
        self.maxLL != None ):
      self.logger.debug( "Using BBOX. LL-Latitude %f LL-Longitude: %f UR-Latitude: %f UR-Longitude: %f"\
                          %(self.minLL.latitude,self.minLL.longitude,self.maxLL.latitude,self.maxLL.longitude))
    if(self.writePrecipToDB):
      if(self.writeLatLonDB( fileName, self.configSettings.dbSettings['dbName'], self.minLL, self.maxLL ) == False):
        return(False)
    if(self.writePrecipToShapefile):
      if(self.writeShapefile( fileName, self.minLL, self.maxLL ) == False):
        return(False)
    return(True)
  
  def writeShapefile(self, fileName, minLatLong=None, maxLatLong=None,newCellSize=None,interpolate=False):
    xmrg = xmrgFile( self.loggerName )
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
          
          shapeData = driver.CreateDataSource('%s/%s.shp' %(directory,filetime))
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
          if( newCellSize == None ):
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
                  feature.SetField('latitude', latlonUL.latitude)
                  feature.SetField('longitude', latlonUL.longitude)
                  feature.SetField('HRAPX', hrap.column)
                  feature.SetField('HRAPY', hrap.row)

                  # Save feature
                  layer.CreateFeature(feature)

                  featureId += 1                            
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
      
      xmrg.cleanUp(True,False)
      return(True)

  def writeLatLonDB(self, fileName, dbFile, minLatLong=None, maxLatLong=None,newCellSize=None,interpolate=False):

    db = dhecDB(dbFile, self.loggerName)     
    print("Loading spatialite: %s" %(self.configSettings.spatiaLiteLib))
    if(db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib) == False):
      print("Error loading: %s Error: %s" %(self.configSettings.spatiaLiteLib,db.lastErrorMsg))

    xmrg = xmrgFile( self.loggerName )
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
          if( newCellSize == None ):
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
          #We are resizing the grid.
          else:
            endProcessing = False           
            spacing = (1.0 / newCellSize)
            #If we are using a bounding box, let's start going through the grid at the bbox's 
            #row and column instead of running through the whole grid to get to that point
            for row in range(startRow,xmrg.MAXY):
              if( row < xmrg.MAXY - 1):
                for col in range(startCol,xmrg.MAXX):
                  if( col < xmrg.MAXX - 1 ):
                    for i in range( newCellSize ):
                      for j in range( newCellSize ):
                        x = spacing * i 
                        y = spacing * j     
                        #Are we interpolating the data?
                        if( interpolate ):  
                          z0 = xmrg.grid[row][col]
                          z1 = xmrg.grid[row][col+1]
                          z2 = xmrg.grid[row+1][col]
                          z3 = xmrg.grid[row+1][col+1]
                          val = 0
                          #If all the data points are 0, no need to run the interpolation.
                          if( z0 != 0 and z1 != 0 and z3 != 0 ):
                            val = xmrg.biLinearInterpolatePoint( x,y, z0, z1, z2, z3 )
                        else:
                          val = xmrg.grid[row][col] 
                        #If there is no precipitation value, or the value is erroneous 
                        if( val <= 0 ):
                          if(self.saveAllPrecipVals):
                            val = 0
                          else:
                            continue
                        else:                          
                          val /= dataConvert          
  
                        hrap = hrapCoord( xmrg.XOR + col + x, xmrg.YOR + row + y )              
                        latlon = xmrg.hrapCoordToLatLong( hrap )              
                        latlon.longitude *= -1
                        insertSQL = False   
                        #If we are using a bounding box, let's see if the coordinate is inside it.               
                        if( minLatLong != None and maxLatLong != None ):
                          if( xmrg.inBBOX( latlon, minLatLong, maxLatLong ) ):
                            insertSQL = True
                        else:
                          insertSQL = True
                        if( insertSQL ):
                          wkt = "POINT(%f %f)" %(latlon.longitude, latlon.latitude)
                          sql = "INSERT INTO precipitation_radar \
                                (insert_date,collection_date,latitude,longitude,precipitation) \
                                VALUES('%s','%s',%f,%f,%f);" \
                                %( datetime,filetime,latlon.latitude,latlon.longitude,val)
                          cursor = db.executeQuery( sql )
                          #Problem with the query, since we are working with transactions, we have to rollback.
                          if( cursor == None ):
                            self.logger.error( db.lastErrorMsg )
                            db.lastErrorMsg = None
                            db.DB.rollback()
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
          db.commit()
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
      
      xmrg.cleanUp(True,False)
      return(True)
  def createWatershedSummaries(self, outputFilepath):
    dhecData = processDHECRainGauges(options.xmlConfigFile)
    dhecData.db.loadSpatiaLiteLib(self.configSettings.spatiaLiteLib)
    where = "WHERE platform_handle LIKE '%raingauge%'"
    rainGauges = dhecData.db.getPlatforms(where)
    
    for rainGauge in rainGauges:
      #Get the unique dates for the radar data.
      sql = "SELECT DISTINCT(strftime('%Y-%m-%d',collection_date)) as date FROM precipitation_radar;"
      rainDatesCursor = dhecData.db.executeQuery(sql)            
      outFile = open( "outputFilepath/%s-radardata.csv" %(rainGauge['short_name']), "wt")  
      outSrcFile = open( "outputFilepath/%s-source-radardata.csv" %(rainGauge['short_name']), "wt")  
      outFile.write( 'date,total,avg\n' )
      outSrcFile.write('date,lat,lon,precip\n')
      print("Processing: %s"%(rainGauge['short_name']))
      dates = []
      for dateRow in rainDatesCursor:         
        date = dateRow['date']
        dates.append(date)
      rainDatesCursor.close()
      for date in dates:
        strtDate = '%sT00:00:00' %(date)
        endDate = '%sT23:59:59' %(date)
        
        sql = "SELECT collection_date,latitude,longitude,precipitation FROM precipitation_radar \
                WHERE\
                collection_date >='%s' AND collection_date <='%s' AND\
                precipitation > 0 AND\
                (Intersects( geom, \
                            (SELECT Geometry FROM boundaries WHERE AOI ='%s'))=1)\
                            ORDER BY collection_date ASC"\
                %(strtDate,endDate,rainGauge['short_name'])
        dbCursor = dhecData.db.executeQuery(sql)        
        if(dbCursor != None):
          #radarData = dbCursor.fetchall()
          total = 0.0
          cnt = 0
          date = ''
          for row in dbCursor:
            lat = row['latitude']
            lon = row['longitude']
            precip = row['precipitation']
            outSrcFile.write( '%s,%f,%f,%f\n' %(row['collection_date'],lat,lon,precip)) 
            total += precip
            cnt += 1
          dbCursor.close()
          avg = 0
          if(cnt > 0):
            avg = total/cnt
          outFile.write( '%s,%f,%f\n' %( strtDate,total,avg) )
      outFile.close() 
      outSrcFile.close()             
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
    xmrgData = dhecXMRGProcessing(options.xmlConfigFile)
    xmrgData.getLatestHourXMRGData()    
    
  except Exception, E:
    info = sys.exc_info()        
    excNfo = traceback.extract_tb(info[2],1)
    items = excNfo[0]    
    print( str(E) +" File: %s Line: %d Function: %s" % (items[0],items[1],items[2]) )
    