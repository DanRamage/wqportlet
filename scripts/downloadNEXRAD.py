import shutil
import os
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
  def __init__(self, downloadDir, logger):
    self.logger = logger
    self.destDirectory = downloadDir
    
    
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
  def downloadFiles(self, startDate, endDate, remoteFileDL):
    if(self.logger != None):
      self.logger.debug("Download files between %s and %s" %(startDate.strftime("%Y-%m-%dT%H:%M:%S"), endDate.strftime("%Y-%m-%dT%H:%M:%S")))
    fileList = self.buildFilelist(startDate, endDate)
    for file in fileList:
      remoteFileDL.getFile(file, None, False)    
      #self.getXMRGFile(file)

  def copyFiles(self, startDate, endDate, directory):
    try:
      if(self.logger != None):
        self.logger.debug("Copy files between %s and %s" %(startDate.strftime("%Y-%m-%dT%H:%M:%S"), endDate.strftime("%Y-%m-%dT%H:%M:%S")))
      fileList = self.buildFilelist(startDate, endDate)
      for file in fileList:
        srcFilepath = "%s/%s" % (directory, file)
        if(os.path.isfile(srcFilepath)):
          destFilepath = "%s/%s" % (self.destDirectory,file)
          shutil.copy(srcFilepath, destFilepath)
          if(self.logger != None):
            self.logger.debug("Copied file: %s to %s" % (srcFilepath, destFilepath))
        else:
          if(self.logger != None):
            self.logger.debug("File %s does not exist." %(srcFilepath))      
    except Exception, e:
      if(self.logger != None):
        self.logger.exception(e)
    
  def getFiles(self, startDate, endDate, url, directory):
    if(url != None):
      remoteFileDL = getRemoteFiles.remoteFileDownload( url, 
                                                             self.destDirectory,
                                                             'b',
                                                              False,
                                                              None,
                                                              True,
                                                              self.logger)
      self.downloadFiles(startDate, endDate, remoteFileDL)
    elif(directory != None):
      self.copyFiles(startDate, endDate, directory)
      
    else:
      if(self.logger != None):
        self.logger.error("No URL or directory provided to get data from, cannot continue.")
      

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
    parser.add_option("-i", "--NEXRADLocal", dest="localFiles",
                      help="Directory to grab NEXRAD files from." )    
    parser.add_option("-l", "--LogConfigFile", dest="logConf",
                      help="Config file to use for the logging." )
  
    (options, args) = parser.parse_args()
    if(options.endDate == None or options.beginDate == None or options.fileDir == None):
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
    
    #dlNexrad = downloadNEXRAD(options.url, options.fileDir, logger)
    #dlNexrad.downloadFiles(beginDate, endDate)
    dlNexrad = downloadNEXRAD(options.fileDir, logger)
    dlNexrad.getFiles(beginDate, endDate, options.url, options.localFiles)
    
    if(logger != None):
      logger.info("Finished processing.")
      
  except Exception, E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)