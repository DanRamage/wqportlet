#!/usr/bin/perl
#script: obskml_to_xenia_sqlite.pl
#Author: Jeremy Cothran (jeremy.cothran[at]gmail.com)
#Organization: Carocoops/SEACOOS
#transforms ObsKML into SQL inserts for Xenia DB schema(version 2 sqlite)

use strict;
use XML::LibXML;
use DBI;
use LWP::Simple;
use POSIX qw(strftime);

#referencing Dan's copy of MeasurementConvert function from obsKMLSubRoutines
use lib ("/home/dramage/netcdf/");
use obsKMLSubRoutines;
my $XMLDoc = XML::LibXML->new->parse_file("/home/dramage/netcdf/UnitsConversion.xml");;


#log run out
my $date_now = `date +%Y-%m-%dT%H:%M:%S`;
chomp($date_now);
print "obskml_to_xenia_sqlite.pl run start $date_now\n";

my ($missing_count,$measurement_count);

####################
#config

#usage: perl <path>/obskml_to_xenia_sqlite.pl <input zipped kml files> <optional: debug output sql directory> <optional: output sql filename prefix> >> <log file>
#note if using debug output sql directory to create a 'archive_in' subdirectory there also which is written to in the script below

#usage: perl /var/www/cgi-bin/microwfs/obskml_to_xenia_sqlite.pl http://carocoops.org/obskml/feeds/seacoos_all_latest.zip >>/tmp/microwfs_debug.log 2>/dev/null

#usage debug: perl /var/www/cgi-bin/microwfs/obskml_to_xenia_sqlite.pl http://carocoops.org/obskml/feeds/seacoos_all_latest.zip /mydir >>/tmp/microwfs_debug.log 2>/dev/null 

#note the user process under which this runs needs to have permissions to write to the following paths

#a temporary directory for decompressing, processing files
my $temp_dir = './tmp';

my $dbname = '/home/dramage/scripts/dhec/dhec.db';

my $path_dir_sql = @ARGV[1];
if ($path_dir_sql eq '') { $path_dir_sql = '.'; }

my $filename_sql = @ARGV[2];
if ($filename_sql eq '') { $filename_sql = 'latest'; }

#my $path_sqlite = '/usr/bin/sqlite3';
my $path_batch_insert = 'perl ./batch_insert.pl';
my $path_sqlfile = "$path_dir_sql/$filename_sql.sql";
my $path_sqlfile_archive = "$path_dir_sql/archive_in/latest_$date_now.sql";
my $path_zipfile_archive = "$path_dir_sql/archive_in/latest_$date_now.zip";
my $path_log = $temp_dir.'/xenia_debug_db.log';

my $undefined_platform_type_id = 6;

#note: the subroutine get_m_type_id has custom substituted synonyms for certain terms

#provide input date cutoff for observation data
my $date_cutoff = `date --date='3 days ago' +%Y-%m-%dT%H:%M:%S`;
chomp($date_cutoff);
#$date_cutoff = '2008-01-18T13:00:00'; #manual set/debug
#print $date_cutoff;

#lat/long bounding box cutoff
my $lat_min_cutoff = 33.4;  #default=-90.00
my $lat_max_cutoff = 33.9;   #default=90.00
my $long_min_cutoff = -79.3; #default=-180.00
my $long_max_cutoff = -77.8; #default=180.00

#originally resolving seconds for dropping duplicates, but dropping time seconds resolution due to duplicates(:00, :40 secs) which are probably netcdf truncating related 
#see usage in code below
my $time_seconds_resolution_flag == 0;

open (SQL_OUT, ">$path_sqlfile");
open (SQL_OUT_ARCHIVE, ">$path_sqlfile_archive");
#print SQL_OUT "BEGIN IMMEDIATE TRANSACTION;\n";

###########################################################

#this is a zip of 1 or more ObsKML files which will all be processed into the necessary oostethys support files
#my $kml_zip_feed = 'http://carocoops.org/obskml/feeds/wq/wq_metadata_latest.kmz';
my $kml_zip_feed = @ARGV[0];

#create temp working directory
my $random_value = int(rand(10000000));
my $target_dir = "$temp_dir/gearth_$random_value";
`mkdir $target_dir`;
#print $target_dir."\n";

##################
#read input files to temp directory
##################

#zip_obskml_url
my $zip_filepath = "$target_dir/obskml.xml.zip";
#print $zip_filepath."\n";
getstore("$kml_zip_feed", $zip_filepath);
getstore("$kml_zip_feed", $path_zipfile_archive);
`cd $target_dir; unzip obskml.xml.zip`;

my $filelist = `ls $target_dir/*.kml`;
#print $filelist;
my @files = split(/\n/,$filelist);
#print @files;

#my $content = getstore('http://carocoops.org/obskml/scripts/to_xenia/seacoos_test1.kml','seacoos_test1.kml');
#die "Couldn't get document" unless defined $content;

#my @platform_ignore = qw(carocoops ndbc.MLRF1 vos);
my @platform_ignore = qw(vos);

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbname", "", "",
                    { RaiseError => 1, AutoCommit => 1 });
if(!defined $dbh) {die "Cannot connect to database!\n";}
my ($sql,$sth);

####initialize lookup hash for sub get_m_type_id

my %m_type_lookup = ();

$sql = qq{ select t0.row_id,t2.standard_name,t3.standard_name from m_type t0,m_scalar_type t1,obs_type t2,uom_type t3 where t0.m_scalar_type_id = t1.row_id and t0.num_types = 1 and t1.obs_type_id = t2.row_id and t1.uom_type_id = t3.row_id };
#print $sql."\n";
$sth = $dbh->prepare( $sql );
$sth->execute();

while (my ($row_id,$obs_type,$uom) = $sth->fetchrow_array) {
        #print "$row_id $obs_type $uom\n";
        $m_type_lookup{$obs_type.".".$uom} = $row_id;
};
####

foreach my $file (@files) {
my $xp = XML::LibXML->new->parse_file("$file");

#my $xp_xml = $xp->serialize; #debug
#print $xp_xml;

foreach my $platform ($xp->findnodes('//Placemark')) {

	#get org/platform related info - only processing obskml with Placemark id attribute utilized (like 'Placemark id=carocoops.CAP1.wls')
        my $platform_id = sprintf("%s",$platform->getAttribute('id'));
	#print "$platform_id\n";

	#could develop a generic 'none.none.none' placeholder, but problems with maintenance and clean-up with temporary platform,sensor ids
        if (!($platform_id)) { next; }
	#print "$platform_id\n";

	#skip platform_id in platform_ignore list	
        if (&search_array($platform_id, @platform_ignore)) { next; }

	my $operator_url = sprintf("%s",$platform->find('Metadata/obsList/operatorURL'));
	$operator_url = escape_literals($operator_url);
        my $platform_url = sprintf("%s",$platform->find('Metadata/obsList/platformURL'));
	$platform_url = escape_literals($platform_url);
        my $platform_desc = sprintf("%s",$platform->find('Metadata/obsList/platformDescription'));
	$platform_desc = escape_literals($platform_desc);

	my $lon_lat_string = sprintf("%s",$platform->find('Point/coordinates'));
	my ($longitude,$latitude) = split(/,/,$lon_lat_string);
	#only carry lon/lat precision to six decimal places
	$longitude = sprintf("%.6f", $longitude);
	$latitude = sprintf("%.6f", $latitude);
	#print "$lon $lat \n";

	if (($latitude < $lat_min_cutoff) || ($latitude > $lat_max_cutoff) || ($longitude < $long_min_cutoff) || ($longitude > $long_max_cutoff)) { print "rejecting lat/long\n"; next; }
	
        my $m_date = sprintf("%s",$platform->find('TimeStamp/when'));
        #print "$m_date :when \n";
  
	#calculate seconds to add to base time to get zulu time
	#note the below conversion could also be done using sqlite database command like
	#select datetime('2008-01-01T12:00:00','+$timezone hours')

	my ($timezone) = substr($m_date,19,3);
	my $timezone_sec = -3600 * $timezone;
	#print $timezone."\n";

	$m_date =~ s/T/ /; #swap space for 'T' for 'date' command below
	$m_date = substr($m_date,0,19); #drop timezone for 'date' command below
	my $date_converted_1 = `date --date='$m_date +0000' +%s` + $timezone_sec;
	#originally resolving seconds, but dropping second resolution due to duplicates(:00, :40) which are probably netcdf truncating related 
	if ($time_seconds_resolution_flag == 1) {
		$m_date = `date -u -d '1970-01-01 $date_converted_1 seconds' +"%Y-%m-%dT%H:%M:%S"`;
	}
	else {
		$m_date = `date -u -d '1970-01-01 $date_converted_1 seconds' +"%Y-%m-%dT%H:%M:00"`;
	}

	chomp($m_date);
        #print "$m_date :calculated\n";

	if ($m_date < $date_cutoff) { print "rejecting date: $m_date \n"; next; }

	##organization####################
	my ($organization_name,$platform_name,$platform_type) = split(/[\._]/,$platform_id);	

	$sql = qq{ SELECT row_id,short_name from organization where short_name like '$organization_name' };
	#print $sql."\n";
	$sth = $dbh->prepare( $sql );
	$sth->execute();
	my ($organization_row_id,$organization_name_lkp) = $sth->fetchrow_array;

	if ($organization_name_lkp) {} #print "$organization_name_lkp\n"; 
	else {
		print "organization $organization_name not found - inserting\n";
	
		$sql = qq{ INSERT into organization(row_entry_date,active,short_name,url) values (datetime('now'),'t','$organization_name','$operator_url'); };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();

		#requery for new row_id
		$sql = qq{ SELECT row_id from organization where short_name like '$organization_name' };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();
		($organization_row_id) = $sth->fetchrow_array;

	}

	##platform####################
	$sql = qq{ SELECT row_id,short_name from platform where organization_id = $organization_row_id and platform_handle like '$platform_id' };
	#print $sql."\n";
	$sth = $dbh->prepare( $sql );
	$sth->execute();
	my ($platform_row_id,$platform_name_lkp) = $sth->fetchrow_array;

	if ($platform_name_lkp) {} #print "$platform_name_lkp\n"; }
	else {
		print "platform $platform_name not found - inserting\n";

		$sql = qq{ INSERT into platform(row_entry_date,organization_id,type_id,short_name,platform_handle,fixed_longitude,fixed_latitude,long_name,description,url) values (datetime('now'),$organization_row_id,$undefined_platform_type_id,'$platform_name','$platform_id',$longitude,$latitude,'$platform_desc','$platform_desc','$platform_url'); };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();

		#requery for new row_id
		$sql = qq{ SELECT row_id from platform where organization_id = $organization_row_id and platform_handle like '$platform_id' };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();
		($platform_row_id) = $sth->fetchrow_array;
	}

        foreach my $observation ($platform->findnodes('Metadata/obsList/obs')) {
	        my $obs_type = sprintf("%s",$observation->find('obsType'));
	        my $uom_type = sprintf("%s",$observation->find('uomType'));
	        my $measurement = sprintf("%s",$observation->find('value'));
                my $elev = sprintf("%s",$observation->find('elev'));
                my $sorder = sprintf("%s",$observation->find('sorder'));
                my $qclevel = sprintf("%s",$observation->find('QCLevel'));

		if ($measurement eq '') { print "missing measurement: $platform_id:$obs_type.$uom_type $m_date\n"; $missing_count++; next; }
		if (!(isnumeric($measurement))) { print "nonnumeric measurement: $platform_id:$obs_type.$uom_type $m_date\n"; $missing_count++; next; }
		#check measurements for conversion from SI to Imperial units
		($measurement,$uom_type) = MeasurementCheck($measurement,$obs_type,$uom_type);	
		#print "measurement: $measurement $obs_type.$uom_type\n";

		#with latest version of database that does not include NOT NULL constraints for sensor.type_id and sensor.short_name could shorten below statements to just utilize m_type_id

                my $m_type_id = get_m_type_id($obs_type,$uom_type);
		if ($m_type_id eq '') { print "$obs_type.$uom_type undefined \n"; next; }

	##sensor####################

	if ($sorder eq '') { $sorder = 1; }

	$sql = qq{ SELECT row_id from sensor where m_type_id = $m_type_id and platform_id = $platform_row_id and s_order = $sorder };
	#print $sql."\n";
	$sth = $dbh->prepare( $sql );
	$sth->execute();
	my ($sensor_row_id) = $sth->fetchrow_array;

	if ($sensor_row_id) {} #print "$sensor_row_id\n"; }
	else {
		print "sensor $obs_type.$uom_type not found - inserting\n";

		$sql = qq{ INSERT into sensor(row_entry_date,platform_id,type_id,short_name,m_type_id,s_order) values (datetime('now'),$platform_row_id,-99999,'undefined',$m_type_id,$sorder); };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();

		#requery for new row_id
		$sql = qq{ SELECT row_id from sensor where m_type_id = $m_type_id and platform_id = $platform_row_id };
		#print $sql."\n";
		$sth = $dbh->prepare( $sql );
		$sth->execute();
		($sensor_row_id) = $sth->fetchrow_array;

	}

	##observation####################

	if ($elev eq '') { $elev = -99999; }

	$measurement_count++;
	if ($qclevel ne '' && $qclevel ne '0' && $qclevel ne '3') { print "qclevel suspect/bad/missing($qclevel) measurement: $platform_id:$obs_type.$uom_type $m_date\n"; next; }
  
  #Convert GMT to localtime.
  my $gmtDate = sprintf( "%s +0000", $m_date );
  $gmtDate =~ s/T/ /;
  
  my $tsttime = `date --date="$gmtDate" +"%s"`;
  my ($temp_sec,$temp_min,$temp_hour,$temp_mday,$temp_mon,$temp_year,$temp_wday,$temp_yday,$isdst) = localtime($tsttime); 
  my $localDate = sprintf("%d-%02d-%02dT%02d:%02d:%02d", $temp_year+1900,$temp_mon+1,$temp_mday,$temp_hour,$temp_min,$temp_sec);
  print("GMT: $m_date Local: $localDate\n");
	$sql = qq{ INSERT into multi_obs(row_entry_date,platform_handle,sensor_id,m_type_id,m_date,m_lon,m_lat,m_z,m_value) values (datetime('now'),'$platform_id',$sensor_row_id,$m_type_id,'$localDate',$longitude,$latitude,$elev,$measurement); };

	#tried running the above insert statement dynamically, like the others, but ran into problems with duplicates instead of being ignored
	#and the program continuing, causing the program to halt(tried getting around this using an 'eval' block but still had problems)
	#so just decided to write all these inserts to an output file and load them from there

	print SQL_OUT $sql."\n";
	print SQL_OUT_ARCHIVE $sql."\n";

	} #foreach $observation


	$sth->finish();
	undef $sth; # to stop "closing dbh with active statement handles"
         	    # http://rt.cpan.org/Ticket/Display.html?id=22688
} #foreach $platform

} #foreach $file

$dbh->disconnect();

#print SQL_OUT "COMMIT TRANSACTION;\n";
close (SQL_OUT);
close (SQL_OUT_ARCHIVE);

my $missing_ratio = sprintf("%d", ($missing_count/$measurement_count)*100);
print "missing_count/measurement_count = $missing_count/$measurement_count $missing_ratio% \n";

$date_now = `date +%Y-%m-%dT%H:%M:%S`;
chomp($date_now);
print "obskml_to_xenia_sqlite.pl run stop $date_now\n";
`echo "=====================\nrun start $date_now \n" >> $path_log`; 

#`$path_sqlite microwfs.db < latest.sql >> $path_log`;
`$path_batch_insert $dbname $path_sqlfile >> $path_log`;

$date_now = `date +%Y-%m-%dT%H:%M:%S`;
chomp($date_now);
`echo "run stop $date_now \n" >> $path_log`; 

exit 0;

####################################################
sub search_array {

my $search_term = shift @_;
my @search_array = @_;

foreach  my $search_page (@search_array) {
        if ($search_term =~ $search_page) { return 1; }
}

return 0;
}

sub get_m_type_id {
my ($obs_type,$uom) = @_;

##CONFIG_START

#conversions for the following which were labeled wrong on the way in to db
if ($obs_type eq 'dissolved_oxygen') { $obs_type = 'oxygen_concentration'; }
if ($uom eq 'millibar') { $uom = 'mb'; }
if ($uom eq 'percent_saturation') { $uom = 'percent'; }
if ($uom eq 'm(MLLW)') { $uom = 'm'; }

##CONFIG_END



my $ret_val = $m_type_lookup{$obs_type.".".$uom};
return $ret_val;
}

#--------------------------------------------------------------------
#                   escape_literals
#--------------------------------------------------------------------
# Must make sure values don't contain XML reserved chars
sub escape_literals {
my $str = shift;
$str =~ s/</&lt;/gs;
$str =~ s/>/&gt;/gs;
$str =~ s/&/&amp;/gs;
$str =~ s/"/&quot;/gs;
$str =~ s/'/&#39;/gs; 
return ($str);
}

sub isnumeric {
my $arg = shift;
if ($arg =~ /^-?(\d+\.?\d*|\.\d+)$/ ) { return 1; }  # match valid number
else { return 0; }
}

sub MeasurementCheck {
my ($measurement,$obs_type,$uom_type) = @_;

my ($uom_type_target,$uom_type_units);
if ($uom_type eq 'millimeter') { $uom_type_target = 'inch'; $uom_type_units = 'in'; }
if ($uom_type eq 'm_s-1' && $obs_type eq 'wind_speed') { $uom_type_target = 'mph'; $uom_type_units = 'mph'; }

#if no conversion needed, just return unchanged
if ($uom_type_target eq '') { return ($measurement,$uom_type); }

#convert
#MeasurementConvert ($Measurement,$strSourceUOM,$strDesiredUOM,$XMLDoc);
my $measurement_target = obsKMLSubRoutines::MeasurementConvert($measurement,$uom_type,$uom_type_target,$XMLDoc);
#print "ok:".$y."\n";

return ($measurement_target,$uom_type_units); 
}
