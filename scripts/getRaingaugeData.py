import sys

#sys.path.append("C:\Documents and Settings\dramage\workspace\BeachAdvisory") 
#print(sys.path)
from dhecRainGaugeProcessing import dhecDB
from dhecRainGaugeProcessing import rainGaugeData
from dhecRainGaugeProcessing import processDHECRainGauges 

if __name__ == '__main__':
  if( len(sys.argv) < 2 ):
    print( "Usage: xmrgFile.py xmlconfigfile")
    sys.exit(-1)    

  dhecData = processDHECRainGauges(sys.argv[1])
  dhecData.deleteRainGaugeDataFiles()
  dhecData.ftpRainGaugeData()
  dhecData.processFiles()
  #Check to make sure data flowed.
  dhecData.checkDataFlow()
  #Create KML output.
  dhecData.writeKMLFile()
