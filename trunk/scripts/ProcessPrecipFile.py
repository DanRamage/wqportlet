import sys
from urllib2 import Request, urlopen, URLError, HTTPError
import time
import re

####################################################################################################################
# Class will get the html doc file listing for a URL and then allow a file by file pulling down of the 
# data. Can be made "smart" by using the fetch log which will keep track of what files have been pulled and
# can be further refined to see if the modification date has changed.
####################################################################################################################
class remoteFileDownload:
  def __init__ ( self, baseURL, destDir, fileMode, useFetchLog, fetchLogDir, log ):
    self.baseURL = baseURL      #The base url we will be pulling the file(s) from.
    self.log       = log            #1 to log out message, 0 otherwise
    self.destDir = destDir      #Directory on local machine file(s) to be created.
    self.fileMode= fileMode     #File open() mode "a" = ASCII "b=binary"
    self.strLastError = ''            #If an error occured, this string will contain it.
    self.useFetchLog = useFetchLog #1 to create a log file to keep track of what file(s) we have downloaded.
    self.fetchLogDir = fetchLogDir #Directory to store the fetch log files
    
  def SetBaseURL(self, baseURL ):
    self.baseURL = baseURL      #The base url we will be pulling the file(s) from.
  
  ####################################################################################################################
  #Function: checkForNewFiles
  #Purpose: Connects to the url given in the init function and attempts to pull out a file listing using a regexp.
  #Params:
  #  filter is a user passed in regexp to replace the default("href\s*=[\s|\"]*(.*?)[\s|\"]") one.
  #Return:
  #  A list containing the file names.
  ####################################################################################################################
    
  #Taken from http://www.techniqal.com/blog/2008/07/31/python-file-read-write-with-urllib2/
  def checkForNewFiles(self, filter):
    fileList = ['']
    try:
      #create the url and the request
      strUrl = self.baseURL
      req = Request(strUrl)
      # Open the url
      f = urlopen(req)
      HTMLDirList = f.read()
      if( len( filter ) == 0 ):
        fileList = re.findall(r'href\s*=[\s|\"]*(.*?)[\s|\"]', HTMLDirList)
      else:
        fileList = re.findall(filter, HTMLDirList)
     
    #handle errors
    except HTTPError, e:
      self.strLastError = e.code + ' ' + strUrl
      print "HTTP Error:",e.code , strUrl
    except URLError, e:
      self.strLastError = e.reason + ' ' + strUrl
      print "URL Error:",e.reason , strUrl
    except Exception, E:  
      self.strLastError = e.reason
      print "Error:",e.reason
    
    if( len(self.strLastError ) ):
      self.logMsg( self.strLastError )
        
    return( fileList )

  ####################################################################################################################
  #Function: writeFetchLogFile
  #Purpose: Writes a fetched log file into the directory, fetchLogDir, passed into the init function. File naming convention
  #  takes the remote file name, strips the extension and adds a .log extension. The modification date for the server file
  #  is stored in the fetch log file.
  #  
  #Params:
  #  fileName is the fetch log we are looking for. The path is added from the fetchLogDir used in the init function.
  #  dateTime is the modded time as pulled from the HTML header and convereted to seconds UTC.
  #Return:
  #  1 if file is created, otherwise 0.
  ####################################################################################################################
  def writeFetchLogFile(self, fileName, dateTime):
    try:
      strFilePath = self.fetchLogDir + fileName
      fetchLog = open( strFilePath, 'w' )      
      fetchLog.write( ( "%d\n" % dateTime ) )
      self.logMsg( "writeFetchLogFile::Creating fetchlog: %s Modtime: %d" % (strFilePath,dateTime) )
      return(1)
    except IOError, e:
      self.strLastError = str(e)
      self.logMsg( self.strLastError )
    return(0)
  
  ####################################################################################################################
  #Function: checkFetchLogFile
  #Purpose: Searches for a fetched log file in the directory, fetchLogDir, passed into the init function. File naming convention
  #  takes the remote file name, strips the extension and adds a .log extension. 
  #Params:
  #  fileName is the fetch log we are looking for. The path is added from the fetchLogDir used in the init function.
  #Return:
  #  Returns the file modded date if there, otherwise -1.
  ####################################################################################################################
  def checkFetchLogFile(self, fileName):
    ModDate = -1
    try:
      strFilePath = self.fetchLogDir + fileName

      LogFile = open( strFilePath, 'r' )
      ModDate = LogFile.readline()     
      if( len(ModDate) ):
        ModDate = float( ModDate )

      self.logMsg( "checkFetchLogFile::Fetchlog %s exists. Modtime: %d" % (strFilePath,ModDate) )

    except IOError, e:
      self.strLastError = str(e)
      if( e.errno != 2 ):
        self.logMsg( self.strLastError )
      else:
        self.logMsg( "checkFetchLogFile::Fetchlog: %s does not exist" % (strFilePath) )

    return(ModDate)

  ####################################################################################################################
  #Function: getFile
  #Purpose: Pulls the file given by remoteFileName onto the local machine and stores it in the directory
  #  destDir passed into the init function. User can also specify a destFileName to name the file something different
  #  when storing on the local machine. If the user specified to use the fetch log, setup in the init, this function
  #  checks to see if the file already exists. If it does, and the user has not specified to further check the server side
  #  files modification date, the file is not pulled. If the user does wish to determine if the file has been modded, it will be
  #  downloaded if it is newer than the current local file.
  #Params:
  #  remoteFileName is the remote file that is to be downloaded.
  #  destFileName is the optional filename to save the remoteFileName to. If you wish to keep the remoteFileName, pass an empty string.
  #  compareModDate set to 1 will compare the remoteFileName mod date with the fetch log files date, if it exists. If the remoteFileName is
  #    newer, it will be downloaded. 0 is a simple check to see if the file has already been downloaded by looking to see if the fetch log
  #    file for that file exists.
  #Return:
  #  The filepath of the filedownloaded, otherwise an empty string.
  ####################################################################################################################
  def getFile( self, remoteFileName, destFileName, compareModDate ):
    retVal = ''
    try:    
      self.logMsg( '-----------------------------------------------------------------' )
      self.logMsg( 'getFile::Processing file: ' + remoteFileName + ' from URL: ' + self.baseURL )
      
      strUrl = self.baseURL + remoteFileName
      req = Request(strUrl)   
      htmlFile = urlopen(req)
      
      downloadFile = 1
      if( self.useFetchLog ):
        strFetchLogFile = ''
        if( len(destFileName) ):
          strFetchLogFile = destFileName
        else:
          strFetchLogFile = remoteFileName
                
        #We just want the file name, not the extension, so split it up.
        fileParts = strFetchLogFile.split( "." )
        strFetchLogFile = fileParts[0] + '.log'
        
        info = htmlFile.info()
        ModDate = htmlFile.headers['Last-Modified']
        #Convert the time into a seconds notation to make comparisons easier.
        date = time.strptime( ModDate, '%a, %d %b %Y %H:%M:%S %Z' )
        ModDate = time.mktime(date)
          
        logFileDate = self.checkFetchLogFile(strFetchLogFile)
        
        writeFetchLogFile = 0
        #If logFileDate the fetch log file doesn't exist, so we need to create it.
        if( logFileDate == -1 ):
          writeFetchLogFile = 1    
          
        if( compareModDate ):
          if( ModDate <= logFileDate ):
            downloadFile = 0
          else:            
            self.logMsg( ("getFile::File modification date is now: %.1f, previous mod date: %.1f" % (ModDate, logFileDate)) )
            writeFetchLogFile = 1
            
        #Not comparing file mod dates, but need to see if we had grabbed that file already. 
        else:
          #If we got a logFileDate back from the check above, we already have the file and don't
          #need to dl it.
          if( logFileDate != -1 ):
            downloadFile = 0
            
      if( writeFetchLogFile ):       
        self.writeFetchLogFile(strFetchLogFile, ModDate)
        
      if( downloadFile ):
        strDestFilePath = self.destDir + remoteFileName          
        self.logMsg( 'getFile::Downloading file: ' +  strDestFilePath )
        DestFile = open(strDestFilePath, "w" + self.fileMode)
        #Write to our local file
        DestFile.write(htmlFile.read())
        DestFile.close()
        retVal = strDestFilePath
        
    #handle errors
    except HTTPError, e:
      self.strLastError = "getFile::HTTP Error:",e.code , url
      print "HTTP Error:",e.code , url
    except URLError, e:
      self.strLastError = "getFile::URL Error:",e.reason , url
      print "URL Error:",e.reason , url
    except Exception, E:  
      self.strLastError = "getFile::Error:",str(E)
      print "Error:",str(E)

    if( len(self.strLastError ) ):
      self.logMsg( self.strLastError )

    self.logMsg( '-----------------------------------------------------------------' )
    
    return(retVal)
    
  def getFiles( self, fileList, fileFilter ):
    try:
      for fileName in fileList :    
        if( re.match( fileFilter, fileName ) != None ):
          
          strUrl = baseURL + fileName
          req = Request(strUrl)   
          htmlFile = urlopen(req)
          
          info = htmlFile.info()
          ModDate = htmlFile.headers['Last-Modified']
          
          strDestFilePath = self.destDir + fileName
            
          DestFile = open(strDestFilePath, "w" + fileMode)
          #Write to our local file
          DestFile.write(htmlFile.read())
          DestFile.close()
          
      return( 1 )
    #handle errors
    except HTTPError, e:
      print "HTTP Error:",e.code , url
      return( -1 )
    except URLError, e:
      print "URL Error:",e.reason , url
      return( -1 )
    except Exception, E:  
      print "Error:",str(E)
      return(-1)
    
    if( len( self.strLastError ) ):
      self.logMsg( self.strLastError )
    
    return(0)

  ####################################################################################################################
  #Function: logMsg
  #Purpose: Logs messages if the user passed a 1 in for the log parameter in the init function.
  ####################################################################################################################
  def logMsg(self, msg ):
    if( self.log ):
      print( msg )


class processNOAAPrecipData:
  def __init__(self, xmlConfigFile):
    from lxml import etree
    try:
      xmlTree = etree.parse(xmlConfigFile)
      baseURL = xmlTree.xpath( '//data/baseURL' )[0].text
      self.fileNameFilter = xmlTree.xpath( '//data/fileNameFilter' )[0].text 
      remoteDir = xmlTree.xpath( '//data/remoteDir' )[0].text
      #If we need to refine the directory further based on date/time then let's do the substitutions.
      #For the shapefiles, the directory structure is: /YYYY/YYYYMM/YYYYMMDD/
      #The format for the filename is nws_precip_YYYYMMDDHH.tar.gz where HH is the hour, GMT time.   
      #The XMRG directory has no such refinement, so we would not have this item configured in the config file.   
      if( len( remoteDir ) ):
          baseURL = baseURL + time.strftime(remoteDir, time.gmtime())

      dlDir   = xmlTree.xpath( '//data/downloadDir' )[0].text
      logMsgs = xmlTree.xpath( '//logging/logMsgs' )[0].text
      useFetchLog = int( xmlTree.xpath( '//fetchLogging/use' )[0].text )
      checkModDate = 0
      fetchLogDir = ''
      if( useFetchLog ):       
        checkModDate = int( xmlTree.xpath( '//fetchLogging/checkModDate' )[0].text )
        fetchLogDir  = xmlTree.xpath( '//fetchLogging/logDir' )[0].text
      
      self.remoteFileDL = remoteFileDownload( baseURL, dlDir, 'b', useFetchLog, fetchLogDir, logMsgs)

    except Exception, e:
      print "processNOAAPrecipData::init:Error:",str(e)
      exit(-1)

  def processFiles(self):
    filesProcd = 0
    fileList = []
    #See if there are any new files to try and download.
    fileList = self.remoteFileDL.checkForNewFiles('')
    filesDLd = []
    #Loop through our list of files, filter out the ones based on the approximate name we should have, then download them.
    for fileName in fileList :    
      if( re.match( self.fileNameFilter, fileName ) != None ):
        fileDLd = self.remoteFileDL.getFile( fileName, '', 1 )
        #If we pulled a file down, store it in the list for processing.
        if( len(fileDLd ) ):
          filesDLd.append( fileDLd ) 
        
        

if __name__ == '__main__':  
  procPrecip = processNOAAPrecipData(sys.argv[1])
  procPrecip.processFiles()
