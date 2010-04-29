import os
import sys
import time
import logging
import logging.handlers
from pysqlite2 import dbapi2 as sqlite3
from xeniatools.xenia import xeniaSQLite

"""
Class: dhecDB
Purpose: Interface to the dhec beach advisory prediction database.
"""
class dhecDB(xeniaSQLite):
  """
  Function: __init__
  Purpose: Initializes the database object. Connects to the database passed in on the dbName parameter.
  """
  def __init__(self, dbName, loggerName=None):
    xeniaSQLite.__init__(self)
    self.logger = None
    if(loggerName != None):
      self.logger = logging.getLogger(loggerName)
      self.logger.info("creating an instance of dhecDB")
    self.totalRowsProcd = 0
    self.rowErrorCnt = 0
    self.lastErrorMsg = None
    if(xeniaSQLite.connect(self, dbName) == False):
      if(self.logger != None):
        self.logger.error(self.db.lastErrorMsg)
      sys.exit(-1)
  def __del__(self):
    self.DB.close()
  
  
  """
  Function: vacuumDB
  Purpose: Cleanup the database. 
  Parameters: None
  Return: True if successful, otherwise False.
  """    
  def vacuumDB(self):
    try:
      sql = "VACUUM;"
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)    
      return(True)    
    except sqlite3.Error, e:        
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)      
    except Exception, E:
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)      
    return(False)

    
  """
  Function: writePrecip
  Purpose: Writes an entry into the precipitation table. Normally this is what we do when we
   are parsing the rain gauge data file to.
  Parameters: 
    datetime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    batt_voltage is the battery voltage of the rain gauge
    program_code is teh program code for the rain gauge
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def writePrecip(self, datetime, rain_gauge, batt_voltage, program_code, rainfall, wind=None, windDir=None):
    
    #Get the platform info so we can get the lat/long for the sensor.
    platformHandle = "dhec.%s.raingauge" % (rain_gauge)
    platformCursor = xeniaSQLite.getPlatformInfo(self, platformHandle)
    if(platformCursor != None):
      nfo = platformCursor.fetchone()
      if(nfo != None):
        mVals = []
        mVals.append(rainfall)
        mVals.append(batt_voltage)
        mVals.append(program_code)                                 
        if(xeniaSQLite.addMeasurement(self,
                                   'precipitation', 'in',
                                   platformHandle,
                                   datetime,
                                   nfo['fixed_latitude'], nfo['fixed_longitude'],
                                   0,
                                   mVals,
                                   1,
                                   False) != True):
          self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
          xeniaSQLite.clearErrorInfo(self)
          return(False)
        
        if(wind != None):
          mVals = []
          mVals.append(wind)
          if(xeniaSQLite.addMeasurement(self,
                                     'wind_speed', 'mph',
                                     platformHandle,
                                     datetime,
                                     nfo['fixed_latitude'], nfo['fixed_longitude'],
                                     0,
                                     mVals,
                                     1,
                                     False) != True):           
            self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
            xeniaSQLite.clearErrorInfo(self)
            return(False)
        if(windDir != None):    
          mVals.append(windDir)
          if(xeniaSQLite.addMeasurement(self,
                                     'wind_from_direction', 'degrees_true',
                                     platformHandle,
                                     datetime,
                                     nfo['fixed_latitude'], nfo['fixed_longitude'],
                                     0,
                                     mVals,
                                     1,
                                     False) != True):
            self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
            xeniaSQLite.clearErrorInfo(self)
            return(False)
      else:
        self.logger.error( "Platform: %s not found. Cannot add measurement." %(platformHandle) )
    else:
      self.logger.error( "%s" %(self.getErrorInfo()) )
      
    return(True)  
  """
  Function: write24HourSummary
  Purpose: Writes an entry into the precip_daily_summary table. This is the days summary for the rain gauge.
  Parameters: 
    datetime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def write24HourSummary(self, datetime, rain_gauge, rainfall):
    #Get the platform info so we can get the lat/long for the sensor.
    platformHandle = "dhec.%s.raingauge" % (rain_gauge)
    platformCursor = xeniaSQLite.getPlatformInfo(self, platformHandle)
    if(platformCursor != None):
      nfo = platformCursor.fetchone()
      if(nfo != None):
        mVals = []
        mVals.append(rainfall)
        if(xeniaSQLite.addMeasurement(self,
                                   'precipitation_accumulated_daily', 'in',
                                   platformHandle,
                                   datetime,
                                   nfo['fixed_latitude'], nfo['fixed_longitude'],
                                   0,
                                   mVals,
                                   1,
                                   True) != True):
          self.logger.error( "%s Function: %s Line: %s File: %s"\
                           %(self.lastErrorMsg,self.lastErrorFunc, self.lastErrorLineNo, self.lastErrorFile) )
          xeniaSQLite.clearErrorInfo(self)
          return(False)

    
  """
  Function: getInspectionDates
  Purpose: Queries the dhec_beach table and returns the dates of the inspections for the given
    station.
  Parameters: 
    station is the name of the station we are quering the dates for.
  Return: A list of the dates. If none were found, the list is empty. 
  """
  def getInspectionDates(self, station, whereClause="", convertToUTM=False):
    dateList = []
    where = ''
    if(len(whereClause)):
      where = "AND %s " %(whereClause)
    sql = "SELECT insp_date,insp_time FROM dhec_beach WHERE station = '%s' %s ORDER BY insp_date ASC"\
         % (station, where)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      for row in dbCursor:
        timeVal = int(row['insp_time']) 
        #insp_time is in a hhmm format, so we break it apart.
        hour = timeVal / 100    #Get the hours
        minute = timeVal - (hour * 100) # Get the minutes
        #The dates are EST, the data in the multi_obs table is in UTM, so if we want to convert
        #do it here.
        date = "%sT%02d:%02d:00" % (row['insp_date'], hour, minute)
        if(convertToUTM):
          date = time.strptime(date, "%Y-%m-%dT%H:%M:00")
          date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.mktime(date)))          
        dateList.append(date)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.critical("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))      
      sys.exit(- 1)
    return(dateList)
  """
  Function: getMoonPhase
  Purpose: For the given day, return the moon phase(percentage of moon visible).
  Parameters:
    date is the day of interest
  Return:
    a floating point number representing the percentage of moon visible
  """
  def getMoonPhase(self, date):
    moonPhase = -9999.0
    sql = "SELECT phase FROM moon_phase WHERE date = strftime( '%%Y-%%m-%%d','%s')" % (date)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      val = dbCursor.fetchone()
      if(val['phase'] != None):
        moonPhase = float(val['phase'])

    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.critical("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))      
      sys.exit(- 1)
      
    return(moonPhase)
  
  """
  Function: writeSummaryForStation
  Purpose:  Writes the entry for day into the summary table. 
  Parameters:
    datetime is the date for the summary
    station is the name of the station we are quering the dates for.
  Return: A list of the dates. If none were found, the list is empty. 
  """
  def writeSummaryForStation(self, datetime, station):
    #import datetime
    
    sql = "SELECT  dhec_beach.station,dhec_beach.insp_type,dhec_beach.etcoc,dhec_beach.tide,dhec_beach.salinity,dhec_beach.weather,monitoring_stations.rain_gauge,precip_daily_summary.rainfall \
          FROM dhec_beach,monitoring_stations,precip_daily_summary \
          WHERE \
          dhec_beach.station = monitoring_stations.station AND \
          monitoring_stations.rain_gauge = precip_daily_summary.rain_gauge AND \
          dhec_beach.station = '%s' AND \
          dhec_beach.insp_date = strftime('%%Y-%%m-%%d', datetime('%s') ) and \
          precip_daily_summary.date = strftime('%%Y-%%m-%%dT23:59:00',datetime('%s'))" % (station, datetime, datetime)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      beachData = dbCursor.fetchone()
      if(beachData != None):
        rainGauge = beachData['rain_gauge']
        if(rainGauge != None):
          #Query the rainfall totals over the given hours range. 
          #Get the last 24 hours summary
          sum24 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 24)
          #Get the last 48 hours summary
          sum48 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 48)
          #Get the last 72 hours summary
          sum72 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 72)
          #Get the last 96 hours summary
          sum96 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 96)
          #Get the last 120 hours summary
          sum120 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 120)
          #Get the last 144 hours summary
          sum144 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 144)
          #Get the last 168 hours summary
          sum168 = self.getLastNHoursSummaryFromPrecipSummary(datetime, rainGauge, 168)
          #calculate the X day delay totals
          #1 day delay
          sum1daydelay = sum48 - sum24
          #2 day delay
          sum2daydelay = sum72 - sum48
          #3 day delay
          sum3daydelay = sum96 - sum72
          
          #Get the preceding dry days count, if there are any.
          dryCnt = self.getPrecedingDryDaysCount(datetime, rainGauge)
          
          #Get the 24 hour rainfall intensity
          rainfallIntensity = self.calcRainfallIntensity(rainGauge, datetime, 10)
                  
          #Write the summary table
          etcoc = -9999
          if(beachData['etcoc'] != None and beachData['etcoc'] != ''):
            etcoc = int(beachData['etcoc'])
          salinity = -9999
          if(beachData['salinity'] != None and beachData['salinity'] != ''):
            salinity = int(beachData['salinity'])
          tide = -9999
          if(beachData['tide'] != None and beachData['tide'] != ''):
            tide = int(beachData['tide'])
          if(tide == -9999):
            tide = self.getTideLevel(8661070, datetime)
          weather = -9999
          if(beachData['weather'] != None and beachData['weather'] != ''):
            weather = beachData['weather']
          
          #rainfall = 0.0
          #if( beachData['rainfall'] != None and beachData != '' ):
          #  rainfall = beachData['rainfall']
          moon = self.getMoonPhase(datetime)  
          
          
          #Query the rainfall totals over the given hours range. 
          #Get the last 24 hours summary
          radarSum24 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 24)
          #Get the last 48 hours summary
          radarSum48 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 48)
          #Get the last 72 hours summary
          radarSum72 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 72)
          #Get the last 96 hours summary
          radarSum96 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 96)
          #Get the last 120 hours summary
          radarSum120 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 120)
          #Get the last 144 hours summary
          radarSum144 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 144)
          #Get the last 168 hours summary
          radarSum168 = dhecData.db.getLastNHoursSummaryFromRadarPrecip(datetime, rainGauge, 168)
      
          radarIntensity = dhecData.db.calcRadarRainfallIntensity( rainGauge, datetime, 60)
          if(radarSum24 == -9999):
            radarIntensity = -9999
          
          radarDryCnt = -9999
          if(radarSum24 != None):
            if(radarSum24 != -9999):
              radarDryCnt = dhecData.db.getPrecedingRadarDryDaysCount(datetime, rainGauge)
          
          #calculate the X day delay totals
          #1 day delay
          radarsum1daydelay = -9999.0
          if(radarSum48 != -9999.0 and radarSum24 != -9999.0):
            radarsum1daydelay = sum48 - sum24
          #2 day delay
          radarsum2daydelay = -9999.0
          if(radarSum72 != -9999.0 and radarSum48 != -9999.0):
            radarsum2daydelay = radarSum72 - radarSum48
          #3 day delay
          radarsum3daydelay = -9999.0
          if(radarSum96 != -9999.0 and radarSum72 != -9999.0):
            radarsum3daydelay = radarSum96 - radarSum72
                
          avgWindSpdSUN2 = dhecData.db.getAvgWindSpeed('carocoops.SUN2.buoy', datetime)
          avgWindDirSUN2,cardPtSUN2 = dhecData.db.getAvgWindDirection('carocoops.SUN2.buoy', datetime)
          avgSalinitySUN2 = dhecData.db.getAvgSalinity('carocoops.SUN2.buoy', datetime)
          avgWaterTempSUN2 = dhecData.db.getAvgWaterTemp('carocoops.SUN2.buoy', datetime)
      
          avgWindSpdNOS = dhecData.db.getAvgWindSpeed('nos.8661070.WL', datetime)
          avgWindDirNOS,cardPtNOS = dhecData.db.getAvgWindDirection('nos.8661070.WL', datetime)
          avgWaterTempNOS = dhecData.db.getAvgWaterTemp('nos.8661070.WL', datetime)
          avgWaterLevelNOS = dhecData.db.getAvgWaterLevel('nos.8661070.WL', datetime)
          
          sql = "INSERT INTO station_summary \
                (date,station,rain_gauge,etcoc,salinity,tide,moon_phase,weather, \
                rain_summary_24,rain_summary_48,rain_summary_72,rain_summary_96,rain_summary_120,rain_summary_144,rain_summary_168, \
                rain_total_one_day_delay,rain_total_two_day_delay,rain_total_three_day_delay,\
                preceding_dry_day_count,inspection_type,rainfall_intensity_24,\
                radar_rain_summary_24,radar_rain_summary_48,radar_rain_summary_72,radar_rain_summary_96,radar_rain_summary_120,radar_rain_summary_144,\
                radar_rain_summary_168,radar_rain_total_one_day_delay,radar_rain_total_two_day_delay,radar_rain_total_three_day_delay,radar_preceding_dry_day_cnt,\
                radar_rainfall_intensity_24,sun2_wind_speed,sun2_wind_dir,sun2_water_temp,sun2_salinity,nos8661070_wind_spd,nos8661070_wind_dir,\
                nos8661070_water_temp,nos8661070_water_level ) \
                VALUES('%s','%s','%s',%d,%d,%d,%.2f,%d, \
                        %.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f, \
                        %.2f,%.2f,%.2f,\
                        %d,'%s',%.2f,\
                        %f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%f,%f'%s',%f,%f,%f,'%s',%f,%f )" % \
                (datetime, station, rainGauge, etcoc, salinity, tide, moon, weather,
                  sum24, sum48, sum72, sum96, sum120, sum144, sum168,
                  sum1daydelay, sum2daydelay, sum3daydelay,
                  dryCnt, beachData['insp_type'], rainfallIntensity,
                  sum24,sum48,sum72,sum96,sum120,sum144,sum168,sum1daydelay,sum2daydelay,sum3daydelay,dryCnt,intensity,
                  avgWindSpdSUN2,cardPtSUN2,avgWaterTempSUN2,avgSalinitySUN2,avgWindSpdNOS,cardPtNOS,avgWaterTempNOS,avgWaterLevelNOS)
          dbCursor.execute(sql)
          #self.dbCon.commit()
          if(self.logger != None):
            self.logger.info("Adding summary for station: %s date: %s." % (station, datetime))
          else:
            print("Adding summary for station: %s date: %s." % (station, datetime))
        return(True)
      else:
        if(self.logger != None):
          self.logger.info("No data for station: %s date: %s. SQL: %s" % (station, datetime, sql))
        else:
          print("No data for station: %s date: %s. SQL: %s" % (station, datetime, sql))
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
        
    except Exception, e:
      if(self.logger != None):
        self.logger.critical(str(e) + ' Terminating script.')
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
      sys.exit(-1)
    
    return(False)
  
  def getLastNHoursPrecipSummary(self, datetime, mTypeID, platformHandle, prevHourCnt):
    sql = "SELECT SUM(m_value) \
           FROM multi_obs \
           WHERE\
             m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') ) AND\
             m_type_id = %d AND\
             platform_handle = '%s'"\
             % (datetime, prevHourCnt, datetime,mTypeID,platformHandle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      sum = dbCursor.fetchone()[0]
      if(sum != None):
        return(float(sum))      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    return(-9999.0)
  
  """
  Function: getLastNHoursSummaryFromRadarPrecipSummary
  Purpose: Calculate the rainfall summary for the past N hours for a given rain_gauge.
  Parameters:
    datetime is the date and time we want to start our collection
    rain_gauge is the rain gauge we are query.  
    prevHourCnt is the number of hours to go back from the given datetime above.
  """
  def getLastNHoursSummaryFromPrecipSummary(self, datetime, rain_gauge, prevHourCnt):
    platformHandle = 'dhec.%s.raingauge' % (rain_gauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_accumulated_daily', 'in',platformHandle,1)
    sum = self.getLastNHoursPrecipSummary(datetime, mTypeID, platformHandle, prevHourCnt)
    return(sum)
    """
    sql = "SELECT SUM(m_value) \
           FROM multi_obs \
           WHERE\
             m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') ) AND\
             m_type_id = %d AND\
             platform_handle = '%s'"\
             % (datetime, prevHourCnt, datetime,mTypeID,platformHandle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      sum = dbCursor.fetchone()[0]
      if(sum != None):
        return(float(sum))      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    return(- 1.0)
    """
  """
  Function: getLastNHoursSummaryFromRadarPrecipSummary
  Purpose: Calculate the rainfall summary for the past N hours for a given rain_gauge/radar.
  Parameters:
    datetime is the date and time we want to start our collection
    rain_gauge is the rain gauge we are query. Even though we are looking up radar data, the 
      rain_gauge name is used to denote the radar area of interest.
    prevHourCnt is the number of hours to go back from the given datetime above.
  """
  def getLastNHoursSummaryFromRadarPrecip(self, datetime, rain_gauge, prevHourCnt):
    sum = -9999.0
    platformHandle = 'nws.%s.radar' % (rain_gauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_radar_weighted_average', 'in',platformHandle,1)
    if(mTypeID != None):
      sum = self.getLastNHoursPrecipSummary(datetime, mTypeID, platformHandle, prevHourCnt)
    return(sum)

  def getLastNHoursSummary(self, datetime, rain_gauge, prevHourCnt):
    platformHandle = 'dhec.%s.raingauge' % (rain_gauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation', 'in',platformHandle,1)
    sql = "SELECT SUM(m_value) \
           FROM multi_obs \
           WHERE\
             m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') ) AND\
             m_type_id = %d AND\
             platform_handle = '%s'"\
             % (datetime, prevHourCnt, datetime,mTypeID,platformHandle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      sum = dbCursor.fetchone()[0]
      if(sum != None):
        return(float(sum))      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    return(-9999.0)
  """
  Function: getPrecedingDryDaysCount
  Purpose: For the given date, this function calculates how many days previous had no rainfall, if any.
  Parameters: 
    datetime is the datetime we are looking for
    rainGauge is the rain  gauge we are query the rainfall summary for.
  """
  def getPrecedingDryDaysCount(self, datetime, rainGauge):
    iDryCnt = 0
    secondsInDay = 24 * 60 * 60
    platformHandle = "dhec.%s.raingauge" %(rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self,"precipitation_accumulated_daily","in",platformHandle,1)
    #When determining the number of days back to go, we use 2 in the  m_date <=strftime('%%Y-%%m-%%dT23:59:00', datetime('%s', '-2 day') )
    #comparison below since the dates on the rain gauge summary data is at hour 23:59.
    #Might change this to drop the hour/minute/second format which would then allow -1 day to be
    #used and be more sensible. Need to test first.
    sql = "Select A.m_date, A.row_id,  A.m_value, "\
          "Case "\
          "When A.m_value= 0  Then "\
              "IFNULL( (Select Max(B.row_id) From multi_obs As B WHERE B.row_id < A.row_id AND B.m_value=0   ), "\
                       "(A.row_id) ) "\
                         "End As grp "\
          "FROM multi_obs As A WHERE "\
          "m_date <= strftime('%%Y-%%m-%%dT23:59:00', datetime('%s', '-2 day') ) AND "\
          "m_type_id = %d AND "\
          "platform_handle = '%s' "\
          "ORDER BY datetime(m_date) DESC;"\
          %(datetime,mTypeID,platformHandle)          
    dbCursor = xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):    
      #We subtract off a day since when we are looking for the summaries we start on -1 day from the date provided.
      t1 = time.mktime(time.strptime(datetime, '%Y-%m-%dT%H:%M:00')) - secondsInDay
      for row in dbCursor:
        if(row['grp'] != None):
          t2 = time.mktime(time.strptime(row['m_date'], '%Y-%m-%dT%H:%M:00'))         
          #We have to make sure the data points are 1 day apart, according the thesis if we are missing
          #a data point, we don't calculate the preceding dry days.
          if((t1 - t2) <= secondsInDay):
            iDryCnt += 1
            t1 = t2
          else:
            iDryCnt = -9999
            break
        else:
          break
    else:
      iDryCnt = -9999
    return(iDryCnt)
  
  """
  Function: getPrecedingRadarDryDaysCount
  Purpose: For the given date, this function calculates how many days previous had no rainfall, if any.
  Radar data differs from the rain gauge data in that when there is no rainfall recorded, we will not have
  a record showing 0 rain fall. This function exploits this fact by querying the database and limiting the
  return data to 1 record which will be the last date that we had rainfall.
  Parameters: 
    datetime is the datetime we are looking for
    rainGauge is the rain  gauge we are query the rainfall summary for.
  """
  def getPrecedingRadarDryDaysCount(self, dateTime, rainGauge):
    iDryCnt = 0
    secondsInDay = 24 * 60 * 60
    platformHandle = "nws.%s.radar" %(rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self,"precipitation_radar_weighted_average","in",platformHandle,1)
    sql = "SELECT DISTINCT(strftime('%%Y-%%m-%%d', m_date)) AS m_date\
          FROM multi_obs WHERE \
          m_date <= strftime('%%Y-%%m-%%d', datetime('%s', '-1 day') ) AND \
          m_type_id = %d AND \
          platform_handle = '%s' \
          ORDER BY datetime(m_date) DESC LIMIT 1;"\
          %(dateTime,mTypeID,platformHandle)          
    dbCursor = xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):    
      row = dbCursor.fetchone()
      if(row != None):
        #Now let's figure out the dry days.
        #Convert the dateTime into a struct_time.
        startDate = time.strptime(dateTime, "%Y-%m-%dT%H:%M:%S")
        #Now we get rid of the hour/min/sec portion and just deal with the day/
        startDate = time.strptime(time.strftime('%Y-%m-%dT00:00:00', startDate), '%Y-%m-%dT00:00:00')
        #Convert to an epoch time.
        startDate = time.mktime(startDate)
        #Convert to an epoch time using the year month and day.
        endDate = time.mktime(time.strptime(row['m_date'], "%Y-%m-%d"))
        #Determine the day count.
        iDryCnt = (startDate - endDate) / secondsInDay      
        
    return(iDryCnt)
      
  
  """  
  Function: calcRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 10 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    platformHandle is the name of the platform we are investigating.
    mTypeID is the id of the tpye of rain observation we are looking up. 
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.
  """ 
  def calcIntensity(self, platformHandle, mTypeID, date, intervalInMinutes):
    rainfallIntensity = -9999.0
    #Get the entries where there was rainfall for the date, going forward the minutes number of minutes. 
    sql = "SELECT m_value from multi_obs \
            WHERE \
            m_date >= strftime('%%Y-%%m-%%dT00:00:00', datetime('%s','-1 day') ) AND m_date < strftime('%%Y-%%m-%%dT00:00:00', '%s' ) AND\
            m_type_id = %d AND\
            platform_handle = '%s';"\
            % (date, date, mTypeID, platformHandle)
    dbCursor =  xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):
      totalRainfall = 0
      numRainEntries = 0    
      hasData = False  
      for row in dbCursor:
        rainfall = float(row['m_value'])
        hasData = True
        if(rainfall > 0.0):
          totalRainfall += rainfall
          numRainEntries += 1
      
      #We want to check to make sure we have data from our query, if we do, let's zero out rainfallIntesity.
      #Otherwise we want to leave it at -1 to denote we had no data for that time.
      if(hasData == True):
        rainfallIntensity = 0.0
          
      if(numRainEntries):  
        rainfallIntensity = totalRainfall / (numRainEntries * intervalInMinutes)
    else:    
      self.logger.error("%s Function: %s Line: %s File: %s" % (self.lastErrorMsg,self.lastErrorFunc, self.lastErrorLineNo, self.LastErrorFile))
  
    return(rainfallIntensity)
  
  """  
  Function: calcRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 10 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    rainGauge is the name of the rain gauge we are investigating.
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.

  """ 
  def calcRainfallIntensity(self, rainGauge, date, intervalInMinutes=10):
    rainfallIntensity = -9999.0
    platformHandle = 'dhec.%s.raingauge' % (rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation', 'in',platformHandle,1)
    if(mTypeID != None):
      rainfallIntensity = self.calcIntensity( platformHandle, mTypeID, date, intervalInMinutes)
    else:
      self.logger.error("No m_type_id for: precipitation(in) found for platform: %s" %(platformHandle))
                        
    return(rainfallIntensity)
    
  """  
  Function: calcRadarRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 60 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    rainGauge is the name of the rain gauge we are investigating.
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.
  """ 
  def calcRadarRainfallIntensity(self, rainGauge, date, intervalInMinutes=60):
    rainfallIntensity = -9999.0
    platformHandle = 'nws.%s.radar' % (rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_radar_weighted_average', 'in',platformHandle,1)
    if(mTypeID != None):
      rainfallIntensity = self.calcIntensity( platformHandle, mTypeID, date, intervalInMinutes)
      #We do not store radar data that has 0 for precipitation, so we want to make sure not to send
      #-1.0 as the return value as -1.0 indicates no data due to a data problem.
      if(rainfallIntensity == -9999.0):
        rainfallIntensity = 0.0              
    else:
      self.logger.error("No m_type_id for: precipitation_radar_weighted_average(in) found for platform: %s" %(platformHandle))
    return(rainfallIntensity)

  """
  Function: getTideLevel
  Purpose: For the given tide station and date, this retrieves the tidal information.
  Parameters: 
    tideStationID is the station to retrieve the data from.
    date is the date to retrieve.
  """    
  def getTideLevel(self, tideStationID, date):
    tideLevel = -9999
    #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
    #the 2000 for flood or 4000 for ebb.
    tideStages = [0, 100, 200, 300]
    sql = "SELECT date,level,level_code FROM daily_tide_range \
            WHERE station_id = %d AND ( date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '-12 hours') AND \
            date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '12 hours') ) \
            ORDER BY date ASC" % (tideStationID, date, date) 
    try:
      epochT1 = time.mktime(time.strptime(date, '%Y-%m-%dT%H:%M:%S'))
      prevTime = -1.0
      curTime = -1.0
      prevRow = []
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      for row in dbCursor:
        curTime = time.mktime(time.strptime(row['date'], '%Y-%m-%dT%H:%M:%S'))
        if(prevTime != -1.0):
          if((epochT1 >= prevTime) and 
              (epochT1 < curTime)):
              #Figure out if the tide is in Ebb or Flood
              tideState = -1
              prevLevel = float(prevRow['level'])
              curLevel = float(row['level'])
              if(prevLevel < curLevel):
                tideState = 2000
              else:
                tideState = 4000
              #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
              #up into 4 pieces, then figure out where our query time falls.
              totalTime = curTime - prevTime
              qtrTime = totalTime / 4.0
              tidePos = -1
              
              for i in range(0, 4):
                if((epochT1 >= prevTime) and
                    (epochT1 < (prevTime + qtrTime))):
                   tidePost = i
                   tideLevel = tideState + tideStages[i]
                   return(tideLevel)
                   
                prevTime += qtrTime
        prevRow = row
        prevTime = curTime
                
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
    return(tideLevel)
  """
  Function: getPlatforms
  Purpose: Returns the platforms(rain gauges, monitoring stations) from the platforms table
  """
  def getPlatforms(self,where=""):
    try:
      sql = "SELECT * from platform %s;" %(where) 
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      return(dbCursor)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):      
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        
    return(None)
  """
  Function: getRainGauges
  Purpose: Returns the rain gauges from the platforms table
  """
  def getRainGauges(self, active=None):
    try:
      whereClause = '';
      if(active != None):
        whereClause = " AND active=%d" % (active)
      sql = "SELECT * from platform WHERE platform_handle LIKE '%%raingauge%%' %s" % (whereClause)
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      return(dbCursor)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
    return(None)
  
  """
  Function: getLastXMRGDate
  Purpose: Returns the last date collected for radar data.
  Parameters:
  Return: String with the last date, or None if nothing found.
  """
  def getLastXMRGDate(self):
    try:
      sql = "SELECT max(collection_date) AS date FROM precipitation_radar"
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row != None):
          return(row['date'])
    except Exception, E:
      self.logger.error(str(E) + ' Terminating script')
      sys.exit(- 1)
    return(None)
 
  """
  Function: cleanPrecipRadar
  Purpose: This function will remove all data older the olderThanDate from the precipitation_radar table.
  Parameters:
    olderThanDate is the comparison date to use.
  Return: 
    True if successful, otherwise False.
  """
  def cleanPrecipRadar(self, olderThanDate):
    sql = "DELETE FROM precipitation_radar WHERE collection_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s');" % (olderThanDate)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.DB.commit()
        return(True)  
      except sqlite3.Error, e:
        self.rowErrorCnt += 1
        if(self.logger != None):
          self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        else:
          print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        
    return(False)  
  """
  Function: cleanIOOSData
  Purpose: Deletes IOOS data from the database that is older than the number of days we want to keep, set in the
    parameter NDaysToKeep.
  Parameters:
    NDaysToKeep is the number of days we want to remain in the database.
  """
  def cleanIOOSData(self, NDaysToKeep):
    sql = "DELETE FROM multi_obs WHERE m_date < datetime('now', '-%d days') AND platform_handle NOT LIKE '%dhec%';"
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.DB.commit()
        return(True)  
      except sqlite3.Error, e:
        if(self.logger != None):
          self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        else:
          print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        return(False)
    return(None)
  """
  Function: getRadarDataForBoundary
  Purpose: For the given rain gauge(boundaryName), this function will return the radar data that is in that POLYGON.
  """
  def getRadarDataForBoundary(self, boundaryName,strtTime,endTime):
    sql = "SELECT latitude,longitude,precipitation FROM precipitation_radar \
            WHERE\
            collection_date >= '%s' AND collection_date < '%s'\
            Intersects( Geom, \
                        (SELECT Geometry FROM boundaries WHERE AOI ='%s'))"\
            %(strtTime,endTime,boundaryName)
    return(self.executeQuery(self,sql))
  
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
  def calculateWeightedAvg(self, watershedName, startTime, endTime):
    weighted_avg = -9999
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.      
    sql = "SELECT * FROM(\
           SELECT (Area(Intersection(radar.geom,bounds.Geometry))/Area(bounds.Geometry)) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar, boundaries bounds\
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                bounds.AOI = '%s' AND\
                Intersects(radar.geom, bounds.geometry))"\
                %(startTime,endTime,watershedName)
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
  
  """
  Function: importTideFile
  Purpose: Imports the tide file passed in the tideFilePath string.
  Parameters:
    tideFilePath is the fully qualified path the the file to import.
  """
  def importTideFile(self,tideFilePath):
    tideFile = processTideData()
    tideFile.openFile(tideFilePath)
    stationID, date, level, tideType = tideFile.readLine()
    
    while(stationID != None and stationID != ""):      
      if(stationID != None):
        #print( "ID: %s Date: %s Level: %f Type: %s" %(stationID, date, level, tideType))
        stationID, date, level, tideType = tideFile.readLine()
        sql = "INSERT INTO daily_tide_range (station_id,date,level,level_code)\
                VALUES(%d,'%s',%f,'%s')"\
                %(int(stationID),date,level,tideType)
        dbCursor = self.executeQuery(sql)
        if(dbCursor == None):
          #We had an error, so we're going to bail.
          return(None)
        
      else:
        if(self.logger != None):
          self.error("Error importing tide file: %s on line: %d" %(tideFilePath,tideFile.lineNo))
        return(False)
      
      stationID, date, level, tideType = tideFile.readLine()
    #Commit the entries into the database.
    self.commit()
    return(True)  
  """
  Function: getAverageForObs\
  Purpose: For the given observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    obsName string representing the observation we are calcing the average for
    uom the units of measurement the observation is stored in
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAverageForObs(self, obsName, uom, platformHandle, startDate, prevNHours):
    avg = -9999
    
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    sensorID = self.sensorExists(obsName, uom, platformHandle)
    if(sensorID != None and sensorID != -1):
      sql = "SELECT AVG(m_value) as m_value_avg  FROM multi_obs\
             WHERE sensor_id = %d AND\
             m_date >= strftime( '%%Y-%%m-%%dT00:00:00', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%d00:00:00', '%s' )"\
            %(sensorID, startDate, prevNHours, startDate) 
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row['m_value_avg'] != None):
          avg = float(row['m_value_avg'])
        return(avg)
    return(None)
  
  """
  Function: getAvgWindSpeed
  Purpose: For the wind_speed observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWindSpeed(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('wind_speed', 'm_s-1', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    else:
      #Convert to knots.
      if(avg != -9999):
        avg *= 1.9438444924406
    return(avg)

  """
  Function: getAvgWindDirection
  Purpose: For the wind_from_direction observation on the platform, will compute the data average from the starting date
  back N hours. Returns the Cardinal Compass Point.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  Return:
    The avg direction as well as the cardinal point
  """
  def getAvgWindDirection(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('wind_from_direction', 'degrees_true', platformHandle, startDate, prevNHours)
    cardinalPt = ""
    if(avg == None):
      avg = -9999
    else:
      cardinalPt = self.compassDirToCardinalPt(avg)
    return(avg,cardinalPt)

  """
  Function: getAvgWaterTemp
  Purpose: For the water_temperature observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWaterTemp(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('water_temperature', 'celsius', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)
 
  """
  Function: getAvgSalinity
  Purpose: For the salinity observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgSalinity(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('salinity', 'psu', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)

  """
  Function: getAvgWaterLevel
  Purpose: For the water_level observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWaterLevel(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('water_level', 'm', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)
    
  """
  Function: compassDirToCardinalPt
  Purpose: Given a 0-360 compass direction, this function will return the cardinal point for it.
    Compass is broken into 8 points: N, NE, E, SE, S, SW, W, NW
  Parameters:
    compassDir is the compass direction to compute the cardinal point for.
  """
  def compassDirToCardinalPt(self, compassDir):
    if(compassDir >= 0 and compassDir <= 360):
      #Get the cardinal point. We use an 8 point system, N, NE, E, SE, S, SW, W, NW
      #We have to "wrap" N since out valid compassDir values are 0-360. 
      cardinalPts = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "N"]
      degreeOffset = 360 / 8
      cardPt = int(round(compassDir / degreeOffset, 0))
      if(cardPt < 9):
        return(cardinalPts[cardPt])
    return(None)              


  