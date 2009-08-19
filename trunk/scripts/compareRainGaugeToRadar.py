import sys
import math
import time
import optparse

sys.path.append("C:\Documents and Settings\dramage\workspace\BeachAdvisory") 
from dhecRainGaugeProcessing import processDHECRainGauges



def plotRainGaugeVsRadar(dbCon, name, strTime):
  
  degToRadians = math.pi / 180
  convertFactor = earthRadiusMeters * degToRadians
  
  fileName="c:\\temp\\%s-rainGauge.csv" % (name)
  rainGaugeFile = open( fileName, "w" )
  fileName="c:\\temp\\%s-radar.csv" % (name)
  radarFile = open( fileName, "w" )
  
  epochStrt = time.mktime( time.strptime(strTime,"%Y-%m-%dT%H:00:00") ) - (12*60*60) #Subtract off 12 hours so we have buffer around times
  epochEnd  = time.mktime( time.strptime(strTime,"%Y-%m-%dT%H:00:00") ) + (12*60*60) #Add on 12 hours
  
  timeInc = 3600
  cnt = epochStrt
  while cnt < epochEnd:
    startTime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(cnt) )
    endTime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(cnt + timeInc) )
    sql = "SELECT SUM(rainfall) as rainfall FROM precipitation WHERE (date >= '%s' AND date < '%s') AND rain_gauge='%s';"\
           %(startTime,endTime,name)
    dbCursor = dbCon.executeQuery(sql)
    if(dbCursor != None):
      for row in dbCursor:        
        val = row['rainfall']
        if(val != None):
          val = float(val)
        else:
          val=0.0
        rainGaugeFile.write( "%s,%f\n" %(endTime, val) )
      dbCursor.close()

    sql = "SELECT precipitation,(DISTANCE(GeomFromText(wkt_geometry),GeomFromText('%s'))*%f) AS Distance "\
          "FROM precipitation_radar WHERE collection_date = '%s' "\
          "AND (DISTANCE(GeomFromText(wkt_geometry),GeomFromText('%s'))*%f) < 2000;"\
         % (rainGaugeWKTGeo,convertFactor,endTime,rainGaugeWKTGeo,convertFactor)
         
    dbCursor = dbCon.executeQuery(sql)
    if(dbCursor != None):
      bFoundRow = False
      for row in dbCursor:
        val = row['precipitation']
        if(val != None):
          val = float(val)
        else:
          val=0.0
        radarFile.write( "%s,%f,%f\n" %(endTime, val,float(row['Distance'])) )
        bFoundRow = True
      if( not bFoundRow ):
        radarFile.write( "%s,%f,%f\n" %(endTime, 0.0,-1.0) )

      dbCursor.close()

    cnt += timeInc    
  
  radarFile.close()
  rainGaugeFile.close()

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                    help="XML Configuration file." )

  (options, args) = parser.parse_args()
  if( options.xmlConfigFile == None ):
    parser.print_usage()
    parser.print_help()
    sys.exit(-1)
    
  dhecData = processDHECRainGauges(options.xmlConfigFile)
  #Load the spatialite library.
  dhecData.db.loadSpatiaLiteLib(dhecData.configSettings.spatiaLiteLib)
  
 
  #Get the rain gauges
  rainGauges = dhecData.db.getRainGauges()
  #Get dates for radar data.
  sql = "SELECT DISTINCT(collection_date) as date FROM precipitation_radar ORDER BY collection_date ASC;"
  dbCursor = dhecData.db.executeQuery(sql)
  radarDateList = []
  if(dbCursor != None):
    for row in dbCursor:
      radarDateList.append(row['date'])
    dbCursor.close()
  earthRadiusMeters = 6371000.0
  degToRadians = math.pi / 180
  convertFactor = earthRadiusMeters * degToRadians
  for rainGauge in rainGauges:
    name = rainGauge['name']
    #Get the platform info for each rain gauge
    sql = "SELECT wkt_geometry FROM platforms WHERE name='%s';" %(name)
    dbCursor = dhecData.db.executeQuery(sql)
    if(dbCursor != None):
      row = dbCursor.fetchone()
      rainGaugeWKTGeo=row['wkt_geometry']
      dbCursor.close()
    else:
      continue
    for date in radarDateList:
      timeTuple = time.strptime(date, "%Y-%m-%dT%H:%M:%S")
      #Start time is previous hour.
      #We take the radar date/time, and go back an hour to sum the rain gauge data.
      strTime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(time.mktime(timeTuple)-3600) )
      endTime = time.strftime( "%Y-%m-%dT%H:00:00", timeTuple )
      #For each rain gauge, let's get the rain gauge summary for that hour and compare to the radar 
      #value at that coord.
      sql = "SELECT SUM(rainfall) as rainfall FROM precipitation WHERE (date >= '%s' AND date < '%s') AND rain_gauge='%s';"\
             %(strTime,endTime,name)
      dbCursor = dhecData.db.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        sum = row['rainfall']
        dbCursor.close()
        if( sum > 0.0 ):
          
          plotRainGaugeVsRadar(dhecData.db, name, strTime)
          
          sql = "SELECT collection_date,wkt_geometry,precipitation,(DISTANCE(GeomFromText(wkt_geometry),GeomFromText('%s'))*%f) AS Distance "\
                "FROM precipitation_radar WHERE collection_date = '%s' "\
                "AND (DISTANCE(GeomFromText(wkt_geometry),GeomFromText('%s'))*%f) < 2000;"\
               % (rainGaugeWKTGeo,convertFactor,endTime,rainGaugeWKTGeo,convertFactor)
          dbCursor = dhecData.db.executeQuery(sql)
          if(dbCursor != None):
            for row in dbCursor:
              print( "Radar: Date: %s LongLat: %s Radar Precip: %s Dist: %s Rain Gauge: LongLat: %s Precip: %f"\
                     %(row['collection_date'],row['wkt_geometry'],row['precipitation'],row['Distance'],rainGaugeWKTGeo, sum))
            dbCursor.close()
            
