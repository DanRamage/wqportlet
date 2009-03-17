import os
import sys
import time
import csv

if __name__ == '__main__':
  if( len(sys.argv) < 2 ):
    print( "Usage: moonphase.py csvfiledirectory")
    sys.exit(-1)    
  
  fileList = os.listdir( sys.argv[1] )      
  for file in fileList:    
    fullPath = sys.argv[1] + '\\' + file
    #Make sure we are trying to process a file and not a directory.
    if( os.path.isfile(fullPath) != True ):
      print( "%s is not a file, skipping" % (fullPath) )
      continue

    inFile = csv.reader(open( fullPath, "rb" ))
    outFileName = ( "%s\\converted\\moonphase-converted.csv" % (sys.argv[1]) )
    outFile = open(outFileName, 'a' )
    
    filenameparts = file.split('-')
    year = filenameparts[0]
    header = ''  
    try:
      row = inFile.next()
      while( row != None ):
        if( len(row) ):
          if( len(header) != 0 ):
            cnt = len(row)
            i = 0
            day = ''
            while( i < cnt ):
              if( i == 0 ):
                day = row[i]
              else:
                if( len(row[i])):
                  month = ''
                  charCnt = len(header[i])
                  j = 0
                  while(j < charCnt):
                    if( j < 3 ):
                      month = month + header[i][j]
                    else: 
                      break
                    j += 1
                  phase = float(row[i])            
                  date = "%s-%s-%s" % (year, month, day)  
                  datetime = time.strptime( date,'%Y-%b-%d' )
                  outdate = time.strftime( '%Y-%m-%d', datetime )      
                  outFile.write( "%s,%.2f\n" % ( outdate,phase ) )   
                
              i += 1         
          else:
            header = row
        row = inFile.next()
        
    except StopIteration,e:
      print( "Finished processing: %s" % file )
      


