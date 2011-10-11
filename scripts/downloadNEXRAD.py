import sys
import traceback
import time
import logging
import logging.config
import optparse
import datetime
from datetime import tzinfo
from pytz import timezone

from xeniatools import getRemoteFiles



class downloadNEXRAD(object):
  def __init__(self, nexradURL, downloadDir, logger):
    self.url = nexradURL
    self.logger = logger
    self.remoteFileDL = getRemoteFiles.remoteFileDownload( nexradURL, 
                                                           downloadDir,
                                                           'b',
                                                            False,
                                                            None,
                                                            True,
                                                            self.logger)
    
    
  """
  Function: buildXMRGFilename
  Purpose: Given the desiredDateTime, creates an XMRG filename for that date/time period.
  Parameters:
    desiredDateTime is the desired data and time for the xmrg file we want to download.
  Returns:
    A string containing the xmrg filename.
  """    
  def buildXMRGFilename(self, desiredDateTime):
    #Hourly filename format is: xmrgMMDDYYYYHHz.gz WHERE MM is month, DD is day, YYYY is year
    #HH is UTC hour     
    hour = desiredDateTime.strftime("%H")
    date = desiredDateTime.strftime("%m%d%Y")
    fileName = 'xmrg%s%sz.gz' % ( date,hour )
    return(fileName)
  
  
  """
  Function: getXMRGFile
  Purpose: Attempts to download the file name given in fileName.
  Parameters: 
    fileName is the name of the xmrg file we are trying to download.
  Returns:
  
  """
  def getXMRGFile(self, fileName):
    return(self.remoteFileDL.getFile( fileName, None, False))


  """
  Function: buildFilelist
  Purpose: Given the starting and ending date/times, this builds a list of nexrad files to download. NEXRAD
    files are created hourly, so this function will build a file name per hour between the start/end times.
  Parameters: 
    startDate a datetimes object representing the starting date/time in UTC
    endDate a datetimes object representing the ending date/time in UTC
  Returns:
    A list object with the filenames.
  """
  def buildFilelist(self, startDate, endDate):    
    #dateDiff = (endDate - startDate)
    #numHours = ((dateDiff.microseconds + (dateDiff.seconds + dateDiff.days * 24 * 3600) * 10**6) / 10**6) / 3600  
    dateList = []
    curDate = startDate
    while curDate >= endDate:      
      xmrgFilename = self.buildXMRGFilename(curDate)
      curDate = curDate - datetime.timedelta(hours=1)
      dateList.append(xmrgFilename)    
    return(dateList)
    
  """
    Function: downloadFiles
    Purpose: 
    Parameters: 
      startDate a datetimes object representing the starting date/time in UTC
      endDate a datetimes object representing the ending date/time in UTC
    Returns:
      A list object with the filenames.
  """
  def downloadFiles(self, startDate, endDate):
    if(self.logger != None):
      self.logger.debug("Download files between %s and %s" %(startDate.strftime("%Y-%m-%dT%H:%M:%S"), endDate.strftime("%Y-%m-%dT%H:%M:%S")))
    fileList = self.buildFilelist(startDate, endDate)
    for file in fileList:
      self.getXMRGFile(file)


if __name__ == '__main__':
  try:
    import psyco
    psyco.full()
  except Exception, E:
    print("Psyco package not available")
    
  logger = None
  
  try:
    parser = optparse.OptionParser()  
    parser.add_option("-b", "--BeginDate", dest="beginDate",
                      help="Starting datetime of files to download." )
    parser.add_option("-e", "--EndDate", dest="endDate",
                      help="Ending datetime of files to download." )  
    parser.add_option("-d", "--FileDirectory", dest="fileDir",
                      help="Directory to download NEXRAD files to." )
    parser.add_option("-u", "--NEXRADUrl", dest="url",
                      help="URL to grab NEXRAD files from." )
    parser.add_option("-l", "--LogConfigFile", dest="logConf",
                      help="Config file to use for the logging." )
  
    (options, args) = parser.parse_args()
    if(options.endDate == None or options.beginDate == None or options.fileDir == None or options.url == None):
      parser.print_usage()
      parser.print_help()
      sys.exit(-1)
      
    if(options.logConf != None):
      logging.config.fileConfig(options.logConf)
      logger = logging.getLogger("nexrad_proc_logger")
      logger.info("Session started")

    eastern = timezone('US/Eastern')  
    if(options.beginDate):    
      estDate = eastern.localize(datetime.datetime.strptime(options.beginDate, "%Y-%m-%dT%H:%M:%S"))
      beginDate = estDate.astimezone(timezone('UTC'))
          
    if(options.endDate):
      estDate = eastern.localize(datetime.datetime.strptime(options.endDate, "%Y-%m-%dT%H:%M:%S"))
      endDate = estDate.astimezone(timezone('UTC'))
            
    dlNexrad = downloadNEXRAD(options.url, options.fileDir, logger)
    dlNexrad.downloadFiles(beginDate, endDate)
    
    if(logger != None):
      logger.info("Finished processing.")
      
  except Exception, E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)