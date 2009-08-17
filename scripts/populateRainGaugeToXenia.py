import os
import sys
import optparse
import time
import traceback

from xeniatools.xenia import xeniaPostGres
sys.path.insert(0, "C:\\Documents and Settings\\dramage\\workspace\\BeachAdvisory") 
from dhecRainGaugeProcessing import readRainGaugeData
from dhecRainGaugeProcessing import dhecConfigSettings
from dhecRainGaugeProcessing import dhecDB
from xeniatools.xmlConfigFile import xmlConfigFile

def checkForPlatformAndSensor( rainGauge, xeniaDB, configSettings ):
  platformHandle = "dhec.%s.raingauge" %(rainGauge)
  id =  xeniaDB.platformExists( platformHandle )
  #Error occured.
  if( id == None ):
    print( "DB Error: %s Code: %s\n" %(xeniaDB.self.lastErrorMsg, xeniaDB.lastErrorCode) )
  #Platform doesn't exist, let's add it.
  elif( id == -1 ):
    info={}
    #Check to see if the organization exists.
    orgID = xeniaDB.organizationExists('dhec');
    if( orgID == -1 ):
      info['short_name'] = 'dhec'
      #info['active'] = 1
      info['description'] = 'South Carolina Department of Heath and Environmental Control'
      orgID = xeniaDB.addOrganization(info)
      if( orgID != None ):
        print( "Successfully added organization: %s to database\n" %(info['short_name']) )
      else:
        print( "Error adding organization: %s to database.\nError: %s" %(info['short_name'],xeniaDB.lastErrorMsg) )
        sys.exit(-1)
    dhecDatabase = dhecDB(configSettings.dbSettings['dbName'], None )
    sql = "SELECT * FROM platforms WHERE name = '%s';" %( rainGauge )
    dbCursor = dhecDatabase.executeQuery(sql)
    if( dbCursor != None ):        
      row = dbCursor.fetchone()
      info['fixed_latitude'] = row['latitude']
      info['fixed_longitude'] = row['longitude']
      info['description'] = row['description']
      info['active'] = 't' #int(row['active'])
      info['platform_handle'] = platformHandle
      info['short_name'] = rainGauge
      info['organization_id'] = orgID
      id = xeniaDB.addPlatform( info )
      if( id != None ):
        print( "Successfully added platform: %s to database\n" %(platformHandle) )
      else:
        print( "Error adding platform: %s to database.\nError: %s" %(platformHandle,xeniaDB.lastErrorMsg) )

  obsID = xeniaDB.sensorExists( 'precipitation', 'millimeter', platformHandle, 1 )
  if(obsID == -1):
    obsID = xeniaDB.addSensor('precipitation','millimeter',platformHandle, 't', 0, 1, False)
    if( obsID != None ):
      print( "Successfully added obs: %s on platform: %s to database\n" %('precipitation', platformHandle) )
    else:
      print( "Error adding obs: %s on platform: %s to database\nError: %s" %('precipitation', platformHandle,xeniaDB.lastErrorMsg) )
      
  
if __name__ == '__main__':   
  try:
    parser = optparse.OptionParser()
    parser.add_option("-c", "--RainGaugeXMLConfigFile", dest="xmlRainGaugeConfig",
                      help="XML Configuration file for rain gauge processing." )  
    parser.add_option( "-U", "--User", dest="dbUser",
                       help="User info for PostGres Xenia database")
    parser.add_option( "-d", "--Database", dest="dbName",
                       help="Database name for PostGres Xenia database")
    parser.add_option( "-o", "--Host", dest="dbHost",
                       help="Database host address for PostGres Xenia database")
    parser.add_option( "-W", "--Password", dest="dbPwd",
                       help="Database password for PostGres Xenia database")
    parser.add_option( "-P", "--CheckPlatformAndSensors", dest="checkPlatformAndSensor", 
                       action= 'store_true', help="Used to verify the platform and sensors exist in the database. Usually only need to run once for an initialization.")
    parser.add_option( "-S", "--SQLOutFilename", dest="sqlOutFilename",
                       help="If set, a SQL file is output to the file given in this option. Otherwise statements written directly to DB" )
    parser.add_option( "-x", "--WriteToXeniaDB", dest="writeToXeniaDB",
                       help="If set, the data is written directly to the Xenia database described by the connection parameters above." )
    
    (options, args) = parser.parse_args()
    if( options.xmlRainGaugeConfig is None or 
        options.dbUser is None or
        options.dbName is None  ):
      parser.print_usage()
      parser.print_help()
      sys.exit(-1)
  
    configSettings = dhecConfigSettings( options.xmlRainGaugeConfig )

    xeniaDB = xeniaPostGres()
    if( xeniaDB.connect( None, options.dbUser, options.dbPwd, options.dbHost, options.dbName ) ):
      print( "Successfully connected to Xenia DB: Name: %s at %s\n" % ( options.dbName, options.dbHost) )
    else:
      print( "Failed to connect to Xenia DB: Host: %s Name: %s User: %s(%s)\nError: %s" %( options.dbHost, options.dbName, options.dbUser, options.dbPwd, xeniaDB.lastErrorMsg ) )
      sys.exit(-1)
      
    sqlFile = None
    if( options.sqlOutFilename != None ):
      try:
        sqlFile = open(options.sqlOutFilename,'w')
        print( "Opened SQL file: %s.\n" %(options.sqlOutFilename) )
      except Exception, e:
        info = sys.exc_info()        
        excNfo = traceback.extract_tb(info[2],1)
        items = excNfo[0]
        print( "Error attempting to open file: %s.\n%s File: %s Line: %d Function: %s" %(options.sqlOutFilename,str(e),items[0],items[1],items[2]))
        sys.exit(-1)
    
      
    fileList = os.listdir( configSettings.rainGaugeFileDir )
    
    linesSkipped = 0
    dbRowsNotInserted = 0        
    for file in fileList:
      #Make sure we are trying to process a file and not a directory.
      fullPath = configSettings.rainGaugeFileDir + file
      if( os.path.isfile(fullPath) != True ):
        print( "%s is not a file, skipping" % (fullPath) )
        continue
        
      print( "Begin processing file: %s" % fullPath )
      try:
        rainGaugeFile = readRainGaugeData()
        rainGaugeFile.openFile( fullPath )
        #Get the row id and the summary id.
        rainGaugeId = file.split('.')
        platformHandle = "dhec.%s.raingauge" %(rainGaugeId[0])
        dhecDatabase = dhecDB(configSettings.dbSettings['dbName'], None )
        sql = "SELECT * FROM platforms WHERE name = '%s';" %( rainGaugeId[0] )
        dbCursor = dhecDatabase.executeQuery(sql)
        if( dbCursor == None ):
          print( "Unable to get rain gauge info from dhec database.\n" )
          sys.exit(-1)                  
        rainGaugeNfo = dbCursor.fetchone()
        dbCursor.close()
        
        #Verify the platform and sensor exists in the xenia database.
        if( options.checkPlatformAndSensor ):
          checkForPlatformAndSensor(rainGaugeId[0], xeniaDB, configSettings)
  
        dataRow = rainGaugeFile.processLine()
        while( dataRow != None ):
          if( dataRow.ID > 0 ):
            #The idea behind this is that there are 2 ID types in the data. One with a signature of xx1 is a normal
            #10 minute interval sample, one with an xx2 signature is the 24 hour summary. So if the first bit is
            #set, my assumption is that it's a 10 minute sample.
            updateType = dataRow.ID & 1
            if(updateType == 1):      
              if(options.writeToXeniaDB != None):        
                values = []
                values.append( dataRow.rainfall * 25.4 )#Rainfall is in inches in file, we divide by 25.4 to convert to mm.
                #DHEC data time is local time, for xenia we want to put it into UTM.
                #DST is not set on the rain gauges, so to correctly calc the UTM time, we've got to test if it is DST
                #then adjust the time.
                isdst = time.localtime()[-1]
                dstOffset = 0
                if( isdst ):
                  dstOffset = 3600 #We want to add 3600 secs(1 hour) to adjust for DST.
                
                dateTime=time.strptime( dataRow.dateTime, '%Y-%m-%dT%H:%M:%S')
                dateTime = time.strftime('%Y-%m-%dT%H:%M:%S',time.gmtime(time.mktime(dateTime) + dstOffset))
                isdst = time.localtime()[-1]
                dstOffset = 5
                if( isdst ):
                  dstOffset = 4
                
                if(xeniaDB.addMeasurement( 'precipitation', 
                                        'millimeter', 
                                        platformHandle, 
                                        dateTime, 
                                        rainGaugeNfo['latitude'], 
                                        rainGaugeNfo['longitude'],
                                        0,
                                        values,
                                        1 )):
                  print( "Added value: %f(%s) on %s Date: %s to multi_obs.\n" % (values[0],'millimeter',platformHandle,dataRow.dateTime) )
                else:
                  if( xeniaDB.lastErrorCode != None and xeniaDB.lastErrorCode == '23505' ):
                    print( "Duplicate error adding value: %f(%s) on %s Date: %s to multi_obs. Rolling back.\nError: %s Code: %s" % (values[0],'millimeter',platformHandle,dataRow.dateTime,xeniaDB.lastErrorMsg,xeniaDB.lastErrorCode) )
                    xeniaDB.DB.rollback()
                  else:
                    print( "Error adding value: %f(%s) on %s Date: %s to multi_obs.\nError: %s Code: %s" % (values[0],'millimeter',platformHandle,dataRow.dateTime,xeniaDB.lastErrorMsg,xeniaDB.lastErrorCode) )
              if(sqlFile != None):
                sensorID = xeniaDB.sensorExists('precipitation', 'millimeter', platformHandle, 1)
                if(sensorID == -1 ):
                  print( "Unable to add measurement. Sensor: %s(%s) does not exist on platform: %s. No entry in sensor table." %('precipitation','millimeter',platformHandle) )
                  continue 
                elif(sensorID == None):
                  print(xeniaDB.lastErrorMsg)
                  sys.exit(-1)
                mTypeID = xeniaDB.getMTypeFromObsName('precipitation', 'millimeter', platformHandle, 1)
                if(mTypeID == -1 ):
                  print("Unable to add measurement. Sensor: %s(%s) does not exist on platform: %s. No entry in m_type table." %('precipitation','millimeter',platformHandle))
                  continue 
                elif(mTypeID == None):
                  print(xeniaDB.lastErrorMsg)
                  sys.exit(-1)                
                sql = "INSERT INTO multi_obs (platform_handle,sensor_id,m_type_id,m_date,m_lat,m_lon,m_z,m_value) "\
                      "VALUES ('%s',%d,%d,'%s',%f,%f,%.2f,%.2f);\n" %(platformHandle,sensorID,mTypeID,dataRow.dateTime,rainGaugeNfo['latitude'],rainGaugeNfo['longitude'],0,dataRow.rainfall * 25.4)
                sqlFile.write(sql)                
          else:
            print( 'No record processed from line: %d' % rainGaugeFile.file.line_num )
            linesSkipped += 1
                                    
          dataRow = rainGaugeFile.processLine()
     
      except StopIteration,e:
        if( linesSkipped ):
          print( 'Unable to process: %d lines out of %d lines' % (linesSkipped, rainGaugeFile.file.line_num) )
        else:
          print( 'Total lines processed: %d' % rainGaugeFile.file.line_num )
        print( 'EOF file: %s.' % file )
        
    if(sqlFile != None):
      print( "Closed SQL file: %s.\n" %(options.sqlOutFilename) )
      sqlFile.close()
              
  except Exception,e:
    info = sys.exc_info()        
    excNfo = traceback.extract_tb(info[2],1)
    items = excNfo[0]
    print( str(e) + " File: %s Line: %d Function: %s" % (items[0],items[1],items[2]) )
    sys.exit(-1)
