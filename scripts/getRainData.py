import sys
import optparse

sys.path.append("C:\Documents and Settings\dramage\workspace\BeachAdvisory") 
#print(sys.path)
from dhecRainGaugeProcessing import dhecDB
from dhecRainGaugeProcessing import rainGaugeData
from dhecRainGaugeProcessing import processDHECRainGauges 
from dhecRainGaugeProcessing import dhecConfigSettings
from xmrgFile import processXMRGData




def getRainGaugeData(configFile):
  dhecData = processDHECRainGauges(configFile)
  dhecData.deleteRainGaugeDataFiles()
  dhecData.ftpRainGaugeData()
  dhecData.processFiles()
  #Check to make sure data flowed.
  #dhecData.checkDataFlow()
  #Create KML output.
  dhecData.writeKMLFile()

def getXMRGData(configFile):
  xmrgData = processXMRGData(configFile)
  xmrgData.getLatestHourXMRGData(True)
  
def vacuum(configFile):
  dhecData = processDHECRainGauges(configFile)
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
    else:
      print( "No options specified. No actions taken.\n" )

