import sys
import os
import traceback
import optparse
import time
#sys.path.insert(0, "C:\\Documents and Settings\\dramage\\workspace\\BeachAdvisory") 
from xmrgFile import xmrgFile,processXMRGData,hrapCoord,LatLong
from dhecRainGaugeProcessing import dhecDB
from xeniatools import getRemoteFiles



class dhecXMRGProcessing(processXMRGData):
  def __init__(self,xmlConfigFile):
    #Call base class init.
    processXMRGData.__init__(self,xmlConfigFile)

    

  def getLatestHourXMRGData(self,writeToDB=True,writeCSVFile=False,writeASCIIGrid=False):    
    try: 
      self.remoteFileDL = getRemoteFiles.remoteFileDownload( self.configSettings.baseURL, self.configSettings.xmrgDLDir, 'b', False, None, True)

      #Clean out any data older than xmrgKeepLastNDays.
      db = dhecDB(self.configSettings.dbSettings['dbName'], self.loggerName)
      #Current time minus N days worth of seconds.
      timeNHoursAgo = time.time() - ( self.configSettings.xmrgKeepLastNDays * 24 * 60 * 60 ) 
      currentDateTime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime(timeNHoursAgo))
      db.cleanPrecipRadar(currentDateTime)
            
      dateList=[]
      #The latest completed hour will be current hour - 1.
      hr = time.time()-3600
      latestHour = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(hr))
      #add it to our list to process
      dateList.append(latestHour)
      
      #Are we going to try to backfill any gaps in the data?
      if(self.configSettings.backfillLastNDays):
        baseTime = time.time()-3600
        #Now let's build a list of the last N hours of data we should have to see if we have any holes
        #to fill.
        lastNHours = self.configSettings.backfillLastNDays * 24
        for x in range(lastNHours):
          datetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(baseTime - ((x+1) * 3600)))          
          dateList.append(datetime)
        sql = "SELECT DISTINCT(collection_date) as date FROM precipitation_radar ORDER BY collection_date DESC;"
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
      db.DB.close()
      
      for date in dateList:
        fileName = self.buildXMRGFilename(date)
        #Now we try to download the file.
        fileName = self.getXMRGFile(fileName)
        if( fileName != None ):
          self.logger.info( "Processing XMRG File: %s" %(fileName))
          xmrg = xmrgFile( self.loggerName )
          xmrg.openFile( fileName )
          self.processXMRGFile( xmrg )
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

  def processXMRGFile(self,xmrgFile):
    if( self.configSettings.minLL != None and 
        self.configSettings.maxLL != None ):
      self.logger.debug( "Using BBOX. LL-Latitude %f LL-Longitude: %f UR-Latitude: %f UR-Longitude: %f"\
                          %(self.configSettings.minLL.latitude,self.configSettings.minLL.longitude,self.configSettings.maxLL.latitude,self.configSettings.maxLL.longitude))

    return( self.writeLatLonDB( xmrgFile, self.configSettings.dbSettings['dbName'], self.configSettings.minLL, self.configSettings.maxLL ) )              

  def writeLatLonDB(self, xmrgFile, dbFile, minLatLong=None, maxLatLong=None,newCellSize=None,interpolate=False):

    db = dhecDB(dbFile, self.loggerName)     
    if( xmrgFile.readFileHeader() ):     
      self.logger.debug( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(xmrgFile.XOR,xmrgFile.YOR,xmrgFile.MAXX,xmrgFile.MAXY))
      try:
        if( xmrgFile.readAllRows() ):
          
          #This is the database insert datetime.           
          datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
          #Parse the filename to get the data time.
          (directory,filetime) = os.path.split( xmrgFile.fileName )
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
            llHrap = xmrgFile.latLongToHRAP(minLatLong,True,True)
            urHrap = xmrgFile.latLongToHRAP(maxLatLong,True,True)
            startCol = llHrap.column
            startRow = llHrap.row
          recsAdded = 0
          if( newCellSize == None ):
            for row in range(startRow,xmrgFile.MAXY):
              for col in range(startCol,xmrgFile.MAXX):
                val = xmrgFile.grid[row][col]
                #If there is no precipitation value, or the value is erroneous 
                if( val <= 0 ):
                  val = 0
                  #continue
                else:
                  val /= dataConvert
                  
                hrap = hrapCoord( xmrgFile.XOR + col, xmrgFile.YOR + row )
                latlon = xmrgFile.hrapCoordToLatLong( hrap )                                
                latlon.longitude *= -1
                insertSQL = False
                if( minLatLong != None and maxLatLong != None ):
                  if( xmrgFile.inBBOX( latlon, minLatLong, maxLatLong ) ):
                    insertSQL = True
                else:
                  insertSQL = True
                if( insertSQL ):
                  #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                  #that has each point in the grid for a given point.                  
                  hrapNewPt = hrapCoord( xmrgFile.XOR + col, xmrgFile.YOR + row + 1)
                  latlonUL = xmrgFile.hrapCoordToLatLong( hrapNewPt )
                  latlonUL.longitude *= -1
                  hrapNewPt = hrapCoord( xmrgFile.XOR + col + 1, xmrgFile.YOR + row)
                  latlonBR = xmrgFile.hrapCoordToLatLong( hrapNewPt )
                  latlonBR.longitude *= -1
                  hrapNewPt = hrapCoord( xmrgFile.XOR + col + 1, xmrgFile.YOR + row + 1)
                  latlonUR = xmrgFile.hrapCoordToLatLong( hrapNewPt )
                  latlonUR.longitude *= -1
                  wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))"\
                        %(latlon.longitude, latlon.latitude,
                          latlonUL.longitude, latlon.latitude, 
                          latlonUR.longitude, latlonUR.latitude, 
                          latlonBR.longitude, latlonBR.latitude, 
                          latlon.longitude, latlon.latitude, 
                          )
                  #wkt = "POINT(%f %f)" %(latlon.longitude, latlon.latitude)
                  sql = "INSERT INTO precipitation_radar \
                        (wkt_geometry,insert_date,collection_date,latitude,longitude,precipitation) \
                        VALUES('%s','%s','%s',%f,%f,%f);" \
                        %( wkt,datetime,filetime,latlon.latitude,latlon.longitude,val)
                  cursor = db.executeQuery( sql )
                  #Problem with the query, since we are working with transactions, we have to rollback.
                  if( cursor == None ):
                    self.logger.error( db.lastErrorMsg )
                    db.lastErrorMsg = None
                    db.rollback()
                  recsAdded += 1
          #We are resizing the grid.
          else:
            endProcessing = False           
            spacing = (1.0 / newCellSize)
            #If we are using a bounding box, let's start going through the grid at the bbox's 
            #row and column instead of running through the whole grid to get to that point
            for row in range(startRow,xmrgFile.MAXY):
              if( row < xmrgFile.MAXY - 1):
                for col in range(startCol,xmrgFile.MAXX):
                  if( col < xmrgFile.MAXX - 1 ):
                    for i in range( newCellSize ):
                      for j in range( newCellSize ):
                        x = spacing * i 
                        y = spacing * j     
                        #Are we interpolating the data?
                        if( interpolate ):  
                          z0 = xmrgFile.grid[row][col]
                          z1 = xmrgFile.grid[row][col+1]
                          z2 = xmrgFile.grid[row+1][col]
                          z3 = xmrgFile.grid[row+1][col+1]
                          val = 0
                          #If all the data points are 0, no need to run the interpolation.
                          if( z0 != 0 and z1 != 0 and z3 != 0 ):
                            val = xmrgFile.biLinearInterpolatePoint( x,y, z0, z1, z2, z3 )
                        else:
                          val = xmrgFile.grid[row][col] 
                        #If there is no precipitation value, or the value is erroneous 
                        if( val <= 0 ):
                          val = 0
                          #continue
                        else:                          
                          val /= dataConvert          
  
                        hrap = hrapCoord( xmrgFile.XOR + col + x, xmrgFile.YOR + row + y )              
                        latlon = xmrgFile.hrapCoordToLatLong( hrap )              
                        latlon.longitude *= -1
                        insertSQL = False   
                        #If we are using a bounding box, let's see if the coordinate is inside it.               
                        if( minLatLong != None and maxLatLong != None ):
                          if( xmrgFile.inBBOX( latlon, minLatLong, maxLatLong ) ):
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
                            db.rollback()
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
      return(True)
    
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
    xmrgData.getLatestHourXMRGData(True)
  except Exception, E:
    info = sys.exc_info()        
    excNfo = traceback.extract_tb(info[2],1)
    items = excNfo[0]    
    print( str(E) +" File: %s Line: %d Function: %s" % (items[0],items[1],items[2]) )
    