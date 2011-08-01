"""
Revisions
Date: 2011-07-27
Function: vacuum
Changes: Use the dhecXMRGProcessing object to vacuum the database instead of the processDHECRainGauges object.
  processDHECRainGauges is being deprecated.
"""
import sys
import optparse
import time
#sys.path.append("C:\Documents and Settings\dramage\workspace\BeachAdvisory") 
from dhecRainGaugeProcessing import dhecDB
from dhecRainGaugeProcessing import rainGaugeData
from dhecRainGaugeProcessing import processDHECRainGauges 
from dhecRainGaugeProcessing import dhecConfigSettings
from dhecXMRGProcessing import dhecXMRGProcessing




def getRainGaugeData(configFile):
  dhecData = processDHECRainGauges(configFile)
#  dhecData.deleteRainGaugeDataFiles()
#  dhecData.ftpRainGaugeData()
  dhecData.processFiles()
  #Check to make sure data flowed.
  #dhecData.checkDataFlow()
  #Create KML output.
  dhecData.writeKMLFile()

def getXMRGData(configFile):
  xmrgData = dhecXMRGProcessing(configFile)
  xmrgData.getLatestHourXMRGData()

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
    else:
      print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    sys.exit(-1)
    
  return

def vacuum(configFile):
  dhecData = dhecXMRGProcessing(configFile)
  dhecData.vacuumDB()

def backupData(configFile):
  dhecData = processDHECRainGauges(configFile)
  dhecData.backupData()

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-c", "--XMLConfigFile", dest="xmlConfigFile",
                    help="XML Configuration file." )
  parser.add_option("-r", "--GetRainGaugeData", dest="getRainGaugeData", 
                    action= 'store_true', help="Process the rain gauge data." )
  parser.add_option("-x", "--GetXMRGData", dest="getXMRGData", 
                    action= 'store_true', help="Process the XMRG Radar data." )  
  parser.add_option("-v", "--Vacuum", dest="vacuum", 
                    action= 'store_true', help="Use to vacuum the database to free unused space and shrink filesize." )
  parser.add_option("-b", "--BackupPrecipitation", dest="backupPrecip", action= 'store_true', 
                    help="Used to roll precipitation data out of the working database and into a backup database." )
  parser.add_option("-f", "--CreateXMRGSummaryFiles", dest="createXMRGSummaryFiles", action= 'store_true', 
                    help="Specifies creation of the XMRG summary files for DHEC excel sheets." )
  (options, args) = parser.parse_args()
  if( options.xmlConfigFile == None ):
    parser.print_usage()
    parser.print_help()
    sys.exit(-1)
  
  if( options.vacuum ):
    vacuum(options.xmlConfigFile )    
  else:
    if( options.getRainGaugeData ):
      getRainGaugeData( options.xmlConfigFile )
    if( options.getXMRGData ):
      getXMRGData( options.xmlConfigFile )
    if( options.backupPrecip ):
      backupData( options.xmlConfigFile )
    if(options.createXMRGSummaryFiles):
      createXMRGSummaryFiles(options.xmlConfigFile)
    else:
      print( "No options specified. No actions taken.\n" )

