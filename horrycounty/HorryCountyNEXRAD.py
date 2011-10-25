#!/usr/bin/python
import os
import sys
import datetime
import optparse
import logging
import logging.config




if __name__ == '__main__':
  retVal = ""
  try:
    import psyco
    psyco.full()
  except Exception, E:
    print("Psyco package not available")
    
  logger = None

  try:    
    parser = optparse.OptionParser()  
    parser.add_option("-c", "--SendDateControlFile", dest="sendDateControlFile",
                      help="File used to determine when to send data to Horry County." )
    parser.add_option("-l", "--LogConfigFile", dest="logConf",
                      help="Config file to use for the logging." )
    parser.add_option("-d", "--DailyFileDirecory", dest="dailyFileDirecory",
                      help="Directory where the daily files are stored." )
  
    (options, args) = parser.parse_args()

    if(options.logConf != None):
      logging.config.fileConfig(options.logConf)
      logger = logging.getLogger("nexrad_proc_logger")
      logger.info("Session started")

    today = datetime.datetime.now()
    today = today.replace(hour=0,minute = 0,second = 0,microsecond = 0)

    dataSendFile = open(options.sendDateControlFile, "r")
    sendDates = []
    for line in dataSendFile:
      line = line.rstrip()
      if(logger != None):
        logger.debug("Processing line: %s" % (line))
      if(len(line)):
        sendDate = datetime.datetime.strptime(line, '%B %d, %Y')
        sendDates.append(sendDate)
    
    
    i = 0
    endDate = None
    startDate = None
    while( i < len(sendDates)):
      endDate = sendDates[i]
      if(endDate == today):
        #Now get the previous date since it is when this period starts. If i = 0 then get the
        #last date in the list.
        j = -1
        if(i > 0):
          j = i - 1
        startDate = sendDates[j]
        if(logger != None):
          logger.debug("Start Date: %s End Date: %s" % (endDate.strftime("%B %d, %Y"), startDate.strftime("%B %d, %Y")))
        break
      i += 1
    
    if(endDate != None and startDate != None):
      retVal = "dogwash_%s_%s.csv" % (startDate.strftime("%Y-%m-%d"), endDate.strftime("%Y-%m-%d"))      
      
  except Exception, E:
    if(logger != None):
      logger.exception(E)
    else:
      print(E)
  
  sys.exit(retVal)