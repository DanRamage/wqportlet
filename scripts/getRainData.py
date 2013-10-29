"""
Revisions
Author: DWR
Date: 2012-06-21
Function: archiveXMRGFiles
Changes: Added function to archive the XMRG files to keep the main download directory clean.

Date: 2011-07-27
Function: vacuum
Changes: Use the dhecXMRGProcessing object to vacuum the database instead of the processDHECRainGauges object.
  processDHECRainGauges is being deprecated.
"""
import sys
import optparse
import time
import datetime
import logging.config
#sys.path.append("C:\Documents and Settings\dramage\workspace\BeachAdvisory") 
from dhecRainGaugeProcessing import dhecDB
from dhecXMRGProcessing import dhecXMRGProcessing
from xmrgFile import xmrgCleanup
from dhecThreddsData import dhecThreddsData
from xeniatools.xmlConfigFile import xmlConfigFile

"""
def getRainGaugeData(configFile):
  dhecData = processDHECRainGauges(configFile)
#  dhecData.deleteRainGaugeDataFiles()
#  dhecData.ftpRainGaugeData()
  dhecData.processFiles()
  #Check to make sure data flowed.
  #dhecData.checkDataFlow()
  #Create KML output.
  dhecData.writeKMLFile()
"""
def getXMRGData(configFile):
  xmrgData = dhecXMRGProcessing(configFile)
  xmrgData.getLatestHourXMRGData()

def getModelData(iniFile):
  dhecThredds = dhecThreddsData(iniFile)
  dhecThredds.processData()
  return
  
def createXMRGSummaryFiles(configFile):
  
  try:
    
    xmrgData = dhecXMRGProcessing(configFile)
    db = dhecDB(xmrgData.configSettings.dbSettings['dbName'], xmrgData.configSettings.loggerName)
    if(db.logger != None):
      db.logger.debug("Beginning createXMRGSummaryFiles");
    else:
      print("Beginning createXMRGSummaryFiles")      
    outputDirectory = xmrgData.configSettings.getEntry('//xmrgData/processingSettings/summaryDirectory')
    if(outputDirectory == None or len(outputDirectory) == 0):
      if(db.logger != None):
        db.logger.error("No output directory given in config file for //xmrgData/processingSettings/summaryDirectory. Cannot continue.")
      else:
        print("No output directory given in config file for //xmrgData/processingSettings/summaryDirectory. Cannot continue.")      
          
    #Starting date is yesterday at 12am. We take the current date and subtract a day, then set time to 12am.
    dateTime = (time.mktime(time.localtime()) - (24 * 3600))
    dateTime = time.localtime(dateTime)
    #now set the hour,min,second to 12am(00:00:00)
    dateTime = time.struct_time((dateTime[0],dateTime[1],dateTime[2],0,0,0,0,0,0))
    #Now finally let's get the UTC time.
    localTime = time.strftime("%Y-%m-%d_%H-%M-%S", dateTime)
    dateTime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.mktime(dateTime)))
    rgCursor = db.getRainGauges();
    for row in rgCursor:    
      platformHandle = row['platform_handle']
      shortName = row['short_name']
      if(db.logger != None):
        db.logger.debug("Processing %s" %(platformHandle));
      else:
        print("Processing %s" %(platformHandle))      

      #Open the output file.
      fileName = "%s/%s-%s.csv" %(outputDirectory, shortName,localTime)
      if(db.logger != None):
        db.logger.debug("Opening output file %s" %(fileName));
      else:
        print("Opening output file %s" %(fileName))      
      outFile = open(fileName, "w")

      xmrgData = db.createXMRGStats(dateTime, shortName)
      #Loop through and convert values to strings.
      for key in xmrgData:
        xmrgData[key] = ("" if xmrgData[key] == None else str(xmrgData[key])) 
      outbuf = ''
      header = '';
      if(shortName == 'nmb1'):
        header = ''
        outbuf = ''
      
      elif(shortName == 'nmb2'):
        header = 'Sum past 6 days rainfall'
        outbuf = xmrgData['radarSum144']
      
      elif(shortName == 'nmb3'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,Sum past 2 days rainfall,Sum past 6 days rainfall"
        outbuf = "%s,%s,%s,%s" %\
        (xmrgData['radarSum24'],
         xmrgData['radarIntensity'],
         xmrgData['radarSum48'],
         xmrgData['radarSum144'])
      
      elif(shortName == 'mb1'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,# Previous dry days,Two day delay rain,Sum past 2 days rainfall"
        outbuf = "%s,%s,%s,%s,%s"\
           % (xmrgData['radarSum24'],
              xmrgData['radarIntensity'],
              xmrgData['radarDryCnt'],
              xmrgData['radarsum2daydelay'],
              xmrgData['radarSum48'])
      
      elif(shortName == 'mb2'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,# Previous dry days,One day delay rain,Sum past 6 days rainfall"
        outbuf = "%s,%s,%s,%s,%s"\
        % (xmrgData['radarSum24'],
           xmrgData['radarIntensity'],
           xmrgData['radarDryCnt'],
           xmrgData['radarsum1daydelay'],
           xmrgData['radarSum144'])
      
      elif(shortName == 'mb3'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,One day delay rain,Sum past 3 days rainfall,Sum past 6 days rainfall,Sum past 7 days rainfall"
        outbuf = "%s,%s,%s,%s,%s,%s"\
        % (xmrgData['radarSum24'],
           xmrgData['radarIntensity'],
           xmrgData['radarsum1daydelay'],
           xmrgData['radarSum72'],
           xmrgData['radarSum144'],
           xmrgData['radarSum168'])
  
      elif(shortName == 'mb4'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,# Previous dry days,Two day delay rain,Three day delay rain,Sum past 2 days rainfall,Sum past 3 days rainfall,Sum past 7 days rainfall"
        outbuf = "%s,%s,%s,%s,%s,%s,%s,%s" \
        % (xmrgData['radarSum24'],
           xmrgData['radarIntensity'],
           xmrgData['radarDryCnt'],
           xmrgData['radarsum2daydelay'],
           xmrgData['radarsum3daydelay'],
           xmrgData['radarSum48'],
           xmrgData['radarSum72'],
           xmrgData['radarSum168'])
  
      elif(shortName == 'surfside'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,# Previous dry days,Two day delay rain,Sum past 2 days rainfall,Sum past 3 days rainfall,Sum past 4 days rainfall,Sum past 5 days rainfall,Sum past 6 days rainfall"
        outbuf = "%s,%s,%s,%s,%s,%s,%s,%s,%s"\
        %( xmrgData['radarSum24'],
           xmrgData['radarIntensity'],
           xmrgData['radarDryCnt'],
           xmrgData['radarsum2daydelay'],
           xmrgData['radarSum48'],
           xmrgData['radarSum72'],
           xmrgData['radarSum96'],
           xmrgData['radarSum120'],
           xmrgData['radarSum144'])
        
      elif(shortName == 'gardcty'):
        header = "Yesterday's rainfall,Intensity of yesterday's rain,Sum past 5 days rainfall"
        outbuf = "%s,%s,%s"\
        %(xmrgData['radarSum24'],
          xmrgData['radarIntensity'],
          xmrgData['radarSum120'])
        
      outFile.write(header + "\n")
      outFile.write(outbuf + "\n")
      if(db.logger != None):
        db.logger.debug("Closing output file %s" %(fileName));
      else:
        print("Closing output file %s" %(fileName))      
      
      outFile.close()
  except Exception, e:
    if(db.logger != None):
      db.logger.critical(str(e) + ' Terminating script.', exc_info=1)
    #else:
    #  print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    sys.exit(-1)
    
  return

def vacuum(configFile):
  dhecData = dhecXMRGProcessing(configFile)
  dhecData.vacuumDB()

def backupData(configFile):
  
  dbFile = configSettings.getEntry('//environment/database/db/name')
  db = dhecDB(dbFile, "dhec_testing_logger")
  backupDb = configSettings.getEntry('//environment/database/db/backup/filePath')
  dbSchema = configSettings.getEntry('//environment/database/db/backup/sqlSchemaFile')
  db.backupData(backupDb, dbSchema)
  
def archiveXMRGFiles(configFile):
  dhecData = dhecXMRGProcessing(configFile)
  archiveDir = dhecData.configSettings.getEntry('//xmrgData/archiveFilesDir')
  if(archiveDir):
    cleanUp = xmrgCleanup(dhecData.configSettings.xmrgDLDir, archiveDir)
    cleanUp.organizeFilesIntoDirectories(datetime.datetime.utcnow())
  
if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                    help="XML Configuration file." )
  parser.add_option("-t", "--GetModelData", dest="getModelData", 
                    action= 'store_true', help="Get the model data from the Thredds server" )
  parser.add_option("-m", "--ModelIniFile", dest="modelIniFile", 
                    help="INI File for processing the model data." )
  parser.add_option("-x", "--GetXMRGData", dest="getXMRGData", 
                    action= 'store_true', help="Process the XMRG Radar data." )  
  parser.add_option("-v", "--Vacuum", dest="vacuum", 
                    action= 'store_true', help="Use to vacuum the database to free unused space and shrink filesize." )
  parser.add_option("-b", "--BackupPrecipitation", dest="backupPrecip", action= 'store_true', 
                    help="Used to roll precipitation data out of the working database and into a backup database." )
  parser.add_option("-f", "--CreateXMRGSummaryFiles", dest="createXMRGSummaryFiles", action= 'store_true', 
                    help="Specifies creation of the XMRG summary files for DHEC excel sheets." )
  parser.add_option("-a", "--ArchiveXMRGFiles", dest="archiveXMRG", action= 'store_true', 
                    help="If true, then files in the XMRG download directory are moved to the archival directory." )
  (options, args) = parser.parse_args()
  if( options.xmlConfigFile == None ):
    parser.print_usage()
    parser.print_help()
    sys.exit(-1)

  configSettings = xmlConfigFile(options.xmlConfigFile)

  logFile = configSettings.getEntry('//environment/logging/logConfigFile')
  logging.config.fileConfig(logFile)
  logger = logging.getLogger("dhec_processing_logger")
  logger.info("Session started")
  
  if( options.vacuum ):
    vacuum(options.xmlConfigFile )    
  else:
    if( options.getXMRGData ):
      getXMRGData(options.xmlConfigFile)
    if(options.getModelData):
      getModelData(options.modelIniFile)
    if( options.backupPrecip ):
      backupData( options.xmlConfigFile )
    if(options.createXMRGSummaryFiles):
      createXMRGSummaryFiles(options.xmlConfigFile)
    if(options.archiveXMRG):
      archiveXMRGFiles(options.xmlConfigFile)

  logger.info("Session stopped")