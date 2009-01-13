from urllib2 import Request, urlopen, URLError, HTTPError
import time
import re

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
      
  #Taken from http://www.techniqal.com/blog/2008/07/31/python-file-read-write-with-urllib2/
  def checkForNewFiles(self, filter, log):
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

  def writeFetchLogFile(self, fileName, dateTime):
    try:
      strFilePath = self.fetchLogDir + fileName
      
      fetchLog = open( strFilePath, 'w' )
      
      fetchLog.write( ( "%d\n" % dateTime ) )
      return(1)
    except IOError, e:
      self.strLastError = str(e)
      self.logMsg( self.strLastError )
    return(0)
  
  def checkFetchLogFile(self, fileName):
    ModDate = -1
    try:
      strFilePath = self.fetchLogDir + fileName

      LogFile = open( strFilePath, 'r' )
      ModDate = LogFile.readline()     
      if( len(ModDate) ):
        ModDate = float( ModDate )

    except IOError, e:
      self.strLastError = str(e)
      if( e.errno != 2 ):
        self.logMsg( self.strLastError )

    return(ModDate)
  def getFile( self, remoteFileName, destFileName, compareModDate ):
    retVal = 0
    try:    
      self.logMsg( '-----------------------------------------------------------------' )
      self.logMsg( 'Processing file: ' + remoteFileName + ' from URL: ' + self.baseURL )
      
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

        #If logFileDate the fetch log file doesn't exist, so we need to create it.
        if( logFileDate == -1 ):          
          self.writeFetchLogFile(strFetchLogFile, ModDate)
          
        if( compareModDate ):
          if( ModDate <= logFileDate ):
            downloadFile = 0
          else:
            Msg = "File modification date is now: %.1f, previous mod date: %.1f" % (ModDate, logFileDate)
            self.logMsg( Msg )
            self.writeFetchLogFile(strFetchLogFile, ModDate)
        #Not comparing file mod dates, but need to see if we had grabbed that file already. 
        else:
          #If we got a logFileDate back from the check above, we already have the file and don't
          #need to dl it.
          if( logFileDate != -1 ):
            downloadFile = 0
             
      if( downloadFile ):
        strDestFilePath = self.destDir + remoteFileName          
        self.logMsg( 'Downloading file: ' +  strDestFilePath )
        DestFile = open(strDestFilePath, "w" + self.fileMode)
        #Write to our local file
        DestFile.write(htmlFile.read())
        DestFile.close()
        retVal = 1
        
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
    
    if( len(self.strLastError ) ):
      self.logMsg( self.strLastError )
    
    return(0)

  def logMsg(self, msg ):
    if( self.log ):
      print( msg )

if __name__ == '__main__':
  
  baseURL = 'http://www.srh.noaa.gov/rfcshare/p_download_hourly/'
  destDir = 'c:\\temp\\nws_precip\\'
  strFetchDir= 'c:\\temp\\nws_precip\\fetchlog\\'
  logMsgs = 1
  #The format for the directory is /YYYY/YYYYMM/YYYYMMDD/
  #The format for the filename is nws_precip_YYYYMMDDHH.tar.gz where HH is the hour, GMT time.
  #Build the various parts of the directory/file name to get the precip file.
  strY = time.strftime('%Y', time.gmtime())
  strYM= time.strftime('%Y%m', time.gmtime())
  strYMD  = time.strftime('%Y%m%d', time.gmtime())
  strFile = 'nws_precip_2009011216.tar.gz'
  
  baseURL = baseURL + strY + '/' + strYM + '/' + strYMD + '/'
  #baseURL, destDir, fileMode, useFetchLog, fetchLogDir, log ):
  remoteFileDL = remoteFileDownload( baseURL, destDir, '', 1, strFetchDir, logMsgs)
 
  fileList = ['']
  fileList = remoteFileDL.checkForNewFiles('', 0)
  for fileName in fileList :    
    if( re.match( 'nws_precip_', fileName ) != None ):
      remoteFileDL.getFile( fileName, '', 1 )
