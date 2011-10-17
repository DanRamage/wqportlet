import sys
import optparse
import time
import os
import os.path
import logging
import logging.config
from pysqlite2 import dbapi2 as sqlite3      
import datetime
from datetime import tzinfo
from pytz import timezone

from xmrgFile import xmrgFile,xmrgDB,hrapCoord,LatLong

class nexradProcess(object):
  def __init__(self, bbox, polygons, dbObj, logger, outputFilename, outputInches=True):
    self.bbox = None
    if(bbox != None):
      self.bbox = bbox
      #Each long/lat pair is seperated by a comma, so let's braek up the input into the pairs.
      bboxParts = options.bbox.split(',')    
      self.minLatLong = LatLong()
      self.maxLatLong = LatLong()
      #Each long/lat pair is seperated by a space.
      pairs = bboxParts[0].split(' ')
      self.minLatLong.longitude = float(pairs[0]) 
      self.minLatLong.latitude = float(pairs[1])
      pairs = bboxParts[1].split(' ')
      self.maxLatLong.longitude = float(pairs[0]) 
      self.maxLatLong.latitude = float(pairs[1])
    
    self.polygonDict = polygons
    self.dbObj = dbObj
    self.logger = logger
    self.outputFilename = outputFilename
    self.shapefilePath = None
    self.dataInInches = outputInches
    
  
  def writeShapefiles(self, shapefilePath):
    self.shapefilePath = shapefilePath
    
  def importFilesIntoDB(self, xmrgDir, deleteDataFiles):     
    try:
      outputFile = None
      #Get a list of the files in the import dir.
      fileList = os.listdir(xmrgDir)
      fileList.sort()          
      if(self.outputFilename):     
        #Add starting date-ending date to file name.
        xmrg = xmrgFile()
        
        #Convert into Eastern time.
        eastern = timezone('UTC')  
        estDate = eastern.localize(datetime.datetime.strptime(xmrg.getCollectionDateFromFilename(fileList[0]), "%Y-%m-%dT%H:%M:%S"))
        startDate = estDate.astimezone(timezone('US/Eastern')).strftime("%Y-%m-%dT%H:%M:%S")
        estDate = eastern.localize(datetime.datetime.strptime(xmrg.getCollectionDateFromFilename(fileList[-1]), "%Y-%m-%dT%H:%M:%S"))
        endDate = estDate.astimezone(timezone('US/Eastern')).strftime("%Y-%m-%dT%H:%M:%S")
               
        startDate = startDate.replace(':', '_')
        endDate = endDate.replace(':', '_')
        nameSubs = {"start" : startDate, "end" : endDate }
        filename = self.outputFilename % (nameSubs)
        outputFile = open(filename, "w")
        if(self.logger != None):
          self.logger.debug("Output file: %s opened" % (filename))
        outputFile.write("Start Time, End Time, Weighted Average\n")
      for fileName in fileList:    
        fullPath = "%s/%s" %(xmrgDir,fileName)  
        #Make sure we are trying to import a file and not a directory.
        if(os.path.isfile(fullPath) != True):
          self.logger.debug("%s is not a file, skipping" % (fullPath))
          continue       

        xmrg = xmrgFile("nexrad_proc_logger")
        xmrg.openFile(fullPath)
        if( xmrg.readFileHeader() ):     
          if(self.logger != None):
            self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
          if(xmrg.readAllRows()):          
            if(self.outputFilename):           
              self.processData(xmrg, outputFile)
            if(self.shapefilePath):
              self.writeShapefile(xmrg)
          else:
            if(self.logger != None):
              self.logger.error("Unable to read rows all rows in from file.")
              
          if(deleteDataFiles):
            xmrg.cleanUp(True,True)
          else:
            xmrg.cleanUp(True,False)
          xmrg.xmrgFile.close()
                  
        else:
          self.logger.error("Unable to process: %s" %(fileName))
      outputFile.close()
    except Exception, E:
      if(self.logger != None):
        self.logger.exception(E)
        
  def doCalcs(self, outputFile, startTime, endTime):
    #Convert the times to EST, internally we are UTC
    utcTZ = timezone('UTC')  
    utcDate = utcTZ.localize(datetime.datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S"))
    estStartTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    utcDate = utcTZ.localize(datetime.datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S"))
    estEndTime = (utcDate.astimezone(timezone('US/Eastern'))).strftime("%Y-%m-%dT%H:%M:%S")
    for polygonKey in self.polygonDict:
      polygonPtList = self.polygonDict[polygonKey].split(',')
      radarCursor = self.dbObj.getRadarDataForBoundary(polygonPtList, startTime, endTime)
      data = ""
      if(radarCursor != None):
        for row in radarCursor:
          if(len(data)):
            data += ","
          data += "Longitude: %s Latitude: %s PrecipValue: %s" % (row['longitude'],row['latitude'],row['precipitation'])
      weightedAvg = self.dbObj.calculateWeightedAvg(polygonPtList, startTime, endTime)
      if(self.dataInInches):
        #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
        #inches , need to divide by 2540.
        weightedAvg /= (25.4 * 100.0)
      #Convert to mm
      else:
        weightedAvg /= 100.0
      outputFile.write("%s,%s,%f\n" %(estStartTime,estEndTime,weightedAvg))
      outputFile.flush()
      if(self.logger != None):
        self.logger.debug("Polygon: %s Weighted Avg: %f StartTime: %s EndTime: %s, %s" % (polygonKey,weightedAvg,startTime,endTime,data))
    
  def processData(self, xmrg, outputFile):
    if(self.logger != None):
      self.logger.debug("File: %s BBOX: %f,%f %f,%f"\
                        %(xmrg.fileName,self.minLatLong.latitude,self.minLatLong.longitude,self.maxLatLong.latitude,self.maxLatLong.longitude))

    self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
    try:      
      #This is the database insert datetime.           
      #datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
      nowTime = (datetime.datetime.now()).strftime("%Y-%m-%dT%H:%M:%S")
      #Parse the filename to get the data time.
      (directory,filetime) = os.path.split(xmrg.fileName)
      (filetime,ext) = os.path.splitext( filetime )
      filetime = xmrg.getCollectionDateFromFilename(filetime)
  
      #Flag to specifiy if any non 0 values were found. No need processing the weighted averages 
      #below if nothing found.
      rainDataFound=False 
      #If we are using a bounding box, let's get the row/col in hrap coords.
      llHrap = None
      urHrap = None
      startCol = 0
      startRow = 0
      if( self.minLatLong != None and self.maxLatLong != None ):
        llHrap = xmrg.latLongToHRAP(self.minLatLong,True,True)
        urHrap = xmrg.latLongToHRAP(self.maxLatLong,True,True)
        startCol = llHrap.column
        startRow = llHrap.row
      recsAdded = 0
      for row in range(startRow,xmrg.MAXY):
        for col in range(startCol,xmrg.MAXX):
          val = xmrg.grid[row][col]
          #If there is no precipitation value, or the value is erroneous 
          #if( val <= 0 ):
          #  if(self.saveAllPrecipVals):
          #    val = 0
          #  else:
          #    continue
          #else:
            
          hrap = hrapCoord( xmrg.XOR + col, xmrg.YOR + row )
          latlon = xmrg.hrapCoordToLatLong( hrap )                                
          latlon.longitude *= -1
          insertSQL = False
          if( self.minLatLong != None and self.maxLatLong != None ):
            if( xmrg.inBBOX( latlon, self.minLatLong, self.maxLatLong ) ):
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
                  %( nowTime,filetime,latlon.latitude,latlon.longitude,val,wkt)
            cursor = self.dbObj.executeQuery( sql )
            #Problem with the query, since we are working with transactions, we have to rollback.
            if( cursor == None ):
              self.logger.error( self.dbObj.lastErrorMsg )
              self.dbObj.lastErrorMsg = None
              self.dbObj.db.rollback()
            else:
              recsAdded += 1
      #Commit the inserts.    
      self.dbObj.db.commit()
      if( self.logger != None ):
        self.logger.info( "Processed: %d rows. Added: %d records to database." %((row + 1),recsAdded))
        
      self.doCalcs(outputFile, filetime,filetime)
                  
    except Exception, E:
      self.logger.exception(E)
      return(False)      
    return(True)

  def writeShapefile(self, xmrg):
    import osgeo.ogr
    import osgeo.osr  
    try:                
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
      shapeFilename = '%s/%s.shp' %(self.shapefilePath,filetime)
      if(os.path.exists(shapeFilename)):
        os.remove(shapeFilename)
      shapeData = driver.CreateDataSource(shapeFilename)
      if(shapeData == None):
        if(self.logger != None):
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
      filetime = xmrg.getCollectionDateFromFilename(filetime)

      #If we are using a bounding box, let's get the row/col in hrap coords.
      llHrap = None
      urHrap = None
      startCol = 0
      startRow = 0
      if( self.minLatLong != None and self.maxLatLong != None ):
        llHrap = xmrg.latLongToHRAP(self.minLatLong,True,True)
        urHrap = xmrg.latLongToHRAP(self.maxLatLong,True,True)
        startCol = llHrap.column
        startRow = llHrap.row
      recsAdded = 0
      featureId = 0
      for row in range(startRow,xmrg.MAXY):
        for col in range(startCol,xmrg.MAXX):
          val = xmrg.grid[row][col]
          #If there is no precipitation value, or the value is erroneous 
          if( val > 0 ):
            #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
            #inches , need to divide by 2540.
            if(self.dataInInches):
              val /= (25.4 * 100.0)
            #convert to mm
            else:
              val /= 100.0
            
          hrap = hrapCoord( xmrg.XOR + col, xmrg.YOR + row )
          latlon = xmrg.hrapCoordToLatLong( hrap )                                
          latlon.longitude *= -1
          insertRow = False
          if( self.minLatLong != None and self.maxLatLong != None ):
            if( xmrg.inBBOX( latlon, self.minLatLong, self.maxLatLong ) ):
              insertRow = True
          else:
            insertRow = True
          if( insertRow ):
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
      
    return(True)


if __name__ == '__main__':
  try:
    import psyco
    psyco.full()
  except Exception, E:
    print("Psyco package not available")
  
  parser = optparse.OptionParser()  
  parser.add_option("-d", "--DatabaseFile", dest="databaseFile",
                    help="Full path to the database used to store the imported file." )
  parser.add_option("-s", "--SpatialiteLib", dest="spatialiteLib",
                    help="Full path to the spatialite library. For windows this will be a DLL, for Linux a shared object file." )
  parser.add_option("-n", "--NexradDir", dest="nexradDir",
                    help="Directory to the nexrad xmrg files to import and process." )
  parser.add_option("-o", "--OutputFile", dest="outputFile",
                      help="The file to write the output csv file to. Can use substitutions %(start) %(end).")  
  parser.add_option("-f", "--ShapeFileDir", dest="shapefileDir",
                      help="The directory to write a shapefile for the processed XMRG file to.")  
  parser.add_option("-b", "--BBOX", dest="bbox",
                    help="The bounding box to use to select the area of interest from the source XMRG file and store into the database.\
                          If not provided, the entire XMRG file is imported." )
  parser.add_option("-p", "--Polygons", dest="polygons",
                    help="An Id=Polygon semicolon separated list of polygons. Polygons1=0 1,1 1,1 0,0 1;Polygons2=0 1,1 1,1 0,0 1" )
  parser.add_option("-l", "--LogConfigFile", dest="logConf",
                    help="The config File to use for the logger." )
  parser.add_option("-c", "--CleanOutDB", dest="cleanDB", action="store_true",
                    help="If set, cleans out the database after processing." )
  parser.add_option("-x", "--RemoveDataFiles", dest="delFiles", action="store_true",
                    help="If set, deletes the NEXRAD data files after processing." )
  parser.add_option("-i", "--OutputInches", dest="outInches", action="store_true",
                    help="If set, the output is converted into inches, native is millimeters." )
  

  (options, args) = parser.parse_args()
  
  if(options.polygons == None):
    parser.print_usage()
    parser.print_help()
    sys.exit(-1)
  
  logger = None  
  try:
    logging.config.fileConfig(options.logConf)
    logger = logging.getLogger("nexrad_proc_logger")
    logger.info("Session started")

    if(logger != None):
      logger.debug("Command line options: %s" % (options)) 
        
    db = xmrgDB(logger)
    if(db.connect(options.databaseFile, options.spatialiteLib) != True):
      logger.debug("Unable to connect to database: %s, cannot continue" %(options.databaseFile))   
  
    #Create our polygon dictionary
    polygons = dict(arg.split('=') for arg in (options.polygons.split(';')))
    
    nexradProc = nexradProcess(options.bbox, polygons, db, logger, options.outputFile, options.outInches)
    if(options.shapefileDir):
      nexradProc.writeShapefiles(options.shapefileDir)
    if(options.nexradDir):
      nexradProc.importFilesIntoDB(options.nexradDir, options.delFiles)

    if(options.cleanDB):
      import datetime
      from datetime import tzinfo
      from pytz import timezone

      nowTime = datetime.datetime.now(timezone('UTC'))
      nowTime = nowTime.strftime("%Y-%m-%dT%H:%M:%S")
      if(logger != None):
        logger.debug("Cleaning up database, removing all dates older than: %s" % (nowTime)) 
      
      if(db.cleanUp(nowTime)):
        if(logger != None):
          logger.debug("Vacuuming database.") 
        if(db.vacuumDB() == False):
          if(logger != None):
            logger.error(db.lastErrorMsg)
      else:
        if(logger != None):
          logger.error(db.lastErrorMsg)
          
    if(logger != None):
      logger.info("Finished processing.")
  except Exception,E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)