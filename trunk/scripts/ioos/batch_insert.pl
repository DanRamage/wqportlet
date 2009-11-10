#!/usr/local/bin/perl

use strict;
use DBI;

#note we assume $sth->err returns a matching error string for sqlite, but for postgres,etc it may return an integer, etc

#expect full paths for below command line args
my ($dbname,$sql_file) = @ARGV;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbname","","",{RaiseError => 0}) or die "Couldn't open dbfile.";
$dbh->func(60000, 'busy_timeout'); #timeout to wait in milliseconds, 60000 = 60 sec

open(SQL_FILE,"$sql_file");

my $row_count = 0;

foreach my $sql (<SQL_FILE>) {
  #print "$sql\n";
  $row_count++;

  my $sth;
  my $error = 1; #assume error to force at least one pass
  while($error){
        $sth = $dbh->prepare($sql);
        die "Couldn't prepare" unless defined $sth;
        
        #print "Execute sth\n";
        $sth->execute();
        #print "ERROR:".$sth->err."\n" if $sth->err;
        
        #clear error flag if no error so can proceed
	$error = '';
        if ($sth->err =~ 'locked') { print "logged case:".$sth->err."\n"; $error = 1; }
        #if ($sth->err =~ 'not unique') { print "logged case:".$sth->err."\n"; }
        if ($sth->err ne '') { 
		#note the if-elsif-else structure below (all statements dependent toward a single pass/fail result
		if ($sth->err eq '19') { $error = ''; } # print "logged case:".$sth->err."\n"; }  #duplicate row
		elsif ($sth->err eq '5') { $error = 1; } #database locked 
		else { $error = 1; print "new case:".$sth->err."\n"; }
	}
        
        $sth->finish;
        undef $sth;  #have to undef $sth to keep from always referencing old pass
        #sleep 1;
  }
} #foreach $sql

print "row_count: $row_count $sql_file\n";

$dbh->disconnect();

exit 0;

