#Load the SQLite R package
library( RSQLite )

#Invoke the SQLite engine
SQLite( 10, 500, FALSE, FALSE )

#Create the database driver.
DB <- dbDriver("SQLite")
#Connect the driver to the database file.
Con <- dbConnect(DB, "C:\\Documents and Settings\\dramage\\workspace\\SVNSandbox\\beachadvisory\\data\\dhec.db" )

#Query all dates >= 2008-01-01 sort the results ascending.
RS <- dbSendQuery( Con, "SELECT datetime(insp_date),etcoc FROM dhec_beach WHERE datetime(insp_date) >= datetime('2008-01-01 00:00:00') ORDER BY insp_date ASC;" )

#Get the data from the results set.
Data <- fetch(RS)
