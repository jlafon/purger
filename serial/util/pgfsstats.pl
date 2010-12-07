#!/usr/bin/env perl
#
# fsstats: collect statistics about a filesystem hierarchy
#
# Author: Marc Unangst <munangst@panasas.com>
# Author: Shobhit Dayal <sdayal@andrew.cmu.edu>
#
# Copyright (c) 2005 Panasas, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#

use 5.006_00;
use strict;
use warnings;
use DBI;
use Config;
use Cwd ();
use File::Basename;
use File::Spec ();
use File::Temp ();
use File::stat ();
use Getopt::Std ();
use Data::Dumper ();


my $time_hires_not_found = "";
eval { require Time::HiRes };
if($@) {
    $time_hires_not_found = $@;
}

#my $progname = File::Basename::basename($0);
my $progname = "pgfsstats";
my $statvfs_not_found = "";

# Filesys::Statvfs is not part of the core perl5 distribution, so it
# may not be present on the system.  Since this is just used to print
# some info about the filesystem and provide a rough estimate of
# runtime, we won't consider it a fatal error if it's not present.
# The code that calls statvfs() later on checks to see whether the
# function is available before making the call.
eval { require Filesys::Statvfs };
if($@) {
    $statvfs_not_found = $@;
}

# software version number
use constant VERSION_NUM  => "1.4.5";
use constant VERSION_DATE => "03/22/2008";

# Set to 1 for debug-level output.  This includes a line of output for
# each directory processed, so it can be very verbose!
use constant DEBUG => 0;

# Interval (number of files processed) after which to report status.
use constant REPORT_INTERVAL => 10000;

# default number of files/sec we process (used for time estimates)
use constant DEFAULT_RATE => 80;

# default interval (in minutes) between checkpoints
use constant DEFAULT_CKPT_INTERVAL => 10;

# declare variables
my @dirlist;
my %hlink;
my %opts;
my $ckpt_file = undef;
my $output_file = undef;
my $reload_file = undef;
my $verbose = 0;
my $stats = 0;
my $err_count = 0;
my $since_last_report = 0;
my $skipped_hlink = 0;
my $start_time = time;
my $last_report = $start_time;
my $slink_relative = 0;
my $slink_absolute = 0;
my $skipped_snapshot = 0;
my $total_size = 0;
my $total_cap_used = 0;
my $interrupted = 0;
my $starting_dir = undef;
my $dir;
my $ckpt_interval = DEFAULT_CKPT_INTERVAL;
my $temp_ckpt_fn = undef;
my $temp_ckpt_fh = undef;
my $last_ckpt_time = undef;
my $ovhd_val;
my $size;
my $cap_used;
my $special_files = 0;
my $total;
my $avail;

my ($size_histo,
   $cap_histo,
   $pos_ovhd_histo,
   $neg_ovhd_histo,
   $dir_histo,
   $dirkb_histo,
   $fname_histo,
   $slink_histo,
   $hlink_histo,
   $mtime_files_histo,
   $mtime_bytes_histo,
   $ctime_files_histo,
   $ctime_bytes_histo,
   $atime_files_histo,
    $atime_bytes_histo);


##### main routine #####

# parse arguments
##Getopt::Std::getopts("c:hi:o:r:sqv", \%opts);
Getopt::Std::getopts("h", \%opts);

if($opts{h}) {
    usage();
    exit 0;
}

printf("fsstats v%s (%s) Copyright (c) 2005, 2007 Panasas, Inc.\n", VERSION_NUM, VERSION_DATE);

 # initialize new histogram data structures
$size_histo  = Histo->new(min => 0, incr => 1,  log_incr => 1, integer_vals => 0);
$cap_histo   = Histo->new(min => 0, incr => 1,  log_incr => 1, integer_vals => 0);
$pos_ovhd_histo  = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$neg_ovhd_histo  = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$dir_histo   = Histo->new(min => 0, incr => 1,  log_incr => 1);
$dirkb_histo = Histo->new(min => 0, incr => 1,  log_incr => 1, integer_vals => 0);
 #Be careful in choosing the value of 'max' for histos.
 #If the log_incr is set for them, the largest value+1 in the last bucket will be a power of 2.
 #If it is not set the largest value+1 in the last bucket will be some multiple of the 'max' value.
 #To be able to toggle log_incr off and on without having to change anything else 'max' should be
 #chosen carefully. Otherwise results may look wrong.
 #Hence if the max value chosen is 'n', then n+1 should be a power of 2. And n+1 should also be a 
 #multiple of the 'incr' value.
 #Dont play with min values.
##  $fname_histo = Histo->new(min => 0, max => 120, incr => 8);
$fname_histo = Histo->new(min => 0, max => 127, incr => 8);
 # $slink_histo = Histo->new(min => 0, max => 120, incr => 8);
$slink_histo = Histo->new(min => 0, max => 127, incr => 8);
$hlink_histo = Histo->new(min => 0, incr => 1,  log_incr => 1);
$mtime_files_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$mtime_bytes_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$ctime_files_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$ctime_bytes_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$atime_files_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);
$atime_bytes_histo = Histo->new(min => 0, incr => 1, log_incr => 1, integer_vals => 0);

my $table = "snapshot1";
my $f_size;
my $f_name;
my $ll_name;
my $hl_name;
my $mage;
my $cage;
my $aage;
my @result;
my $file_type_mask =    0b00000000000000001111000000000000;
my $regular_file_mask = 0b00000000000000001000000000000000;
my $sym_link_mask =     0b00000000000000001010000000000000;
my $dir_mask =          0b00000000000000000100000000000000;
my $tresult;
my $dir_cnt= 1;    # start at 1 because pstat does not count top level directory
my $pop_dir_cnt= 0;
my $i;
my $tmp_count = 0;
my $tmp_count_link = 0;
my $tmp_count_file = 0;
my $tmp_count_dir = 0;

my $dbh = DBI->connect('DBI:Pg:dbname=scratch2','','')
    or die "Couldn't connect to database: " . DBI->errstr;
##print "Connected !!!!!!!!\n";
my $query = "select size,filename,cast(mode as integer),block,nlink," .
           "extract (epoch from mtime)," .
           "extract (epoch from ctime)," .
           "extract (epoch from atime)," .
           "nlink,inode " .
    "from snapshot1";

##print "$query\n";
my $sth = $dbh->prepare($query);
$sth->execute();

if ($sth->rows != 0) {
    while (@result = $sth->fetchrow_array()) {
	$tresult = $file_type_mask & $result[2];
	$cap_used = ($result[3] / 2.0);  #force floating point divide and not round down.
	$total_cap_used += $cap_used;
	$size = $result[0]/1024.0;
	$total_size += $result[0]/1024.0;
#TEMPTEMP
#    if ($tresult == $regular_file_mask) {
	$tmp_count++;
#    }
#TEMPTEMP

	if(($tresult == $regular_file_mask) && $result[8] > 1) {
     # if it's a regular file and the link count is > 1, check to see
     # if we've seen this inode number before.  if we have, skip the
     # remainder of the stats, so we don't double-count a hardlinked
     # file.  if we haven't, make an entry for it in our hash.
	    if($hlink{$result[9]}) {
		$skipped_hlink++;
		goto next_status;
	    }
	    else {
		$hlink{$result[9]}++;
	    }
	}
   if ($tresult == $dir_mask ||
       $tresult == $sym_link_mask ||
       $tresult == $regular_file_mask) {
       if($cap_used >= $size) {
	   $ovhd_val = $cap_used - $size;
#          print "OVERHEAD VALUE:  $ovhd_val\n";
	   $pos_ovhd_histo->add($ovhd_val);
       }
       else { 
	   $ovhd_val = $size - $cap_used;
	   $neg_ovhd_histo->add($ovhd_val);
       }
   }
	$ll_name = basename($result[1]);
	$hl_name = dirname($result[1]);

	if ($tresult == $sym_link_mask) {
#      print "sym link $result[1]\n";
	    $tmp_count_link++;
	    $slink_histo->add($result[0]); # want to add bytes, not KB
	    $fname_histo->add(length $ll_name);
	}
	elsif ($tresult == $regular_file_mask) {
	    $tmp_count_file++;
	    $size_histo->add($result[0]/1024.0);
#      $total_size += $result[0]/1024.0;
#      $ll_name = basename($result[1]);
#      $hl_name = dirname($result[1]);
	    $fname_histo->add(length $ll_name);
	    $mage = (time - $result[5])/(60*60*24);
	    if ($mage < 1) {
		$mage = 0;
	    }
	    $mtime_files_histo->add($mage);
	    $mtime_bytes_histo->add($mage, $result[0]/1024.0);
	    $cage = (time - $result[6])/(60*60*24);
	    if ($cage < 1) {
		$cage = 0;
	    }
	    $ctime_files_histo->add($cage);
	    $ctime_bytes_histo->add($cage, $result[0]/1024.0);
	    $aage = (time - $result[7])/(60*60*24);
	    if ($aage < 1) {
		$aage = 0;
	    }
	    $atime_files_histo->add($aage);
	    $atime_bytes_histo->add($aage, $result[0]/1024.0);
	    $cap_histo->add($cap_used);
	    $hlink_histo->add($result[4]);
	}
	elsif ($tresult == $dir_mask) {
	    $tmp_count_dir++;
#      print "directory $result[1]\n";
#      $ll_name = basename($result[1]);
#      $hl_name = dirname($result[1]);
	    $fname_histo->add(length $ll_name);

	    $dirkb_histo->add($result[0]/1024.0);
	    $dir_cnt++;
	}
	else {
	    $special_files++;
	}
	next_status:
    } # end while fetrow
} # endif results from db
undef($sth);
$sth = $dbh->prepare('select filepath, count(*) from dir_contents group by filepath order by filepath');
$sth->execute();
if ($sth->rows != 0) {
    while (@result = $sth->fetchrow_array()) {
	$dir_histo->add($result[1]); 
	$pop_dir_cnt++;
    }
}
# now take care of directories with 0 files
for ($i=0; $i < ($dir_cnt - $pop_dir_cnt); $i++) {
    $dir_histo->add(0); 
}
undef($sth);
$dbh->disconnect;
$dbh= undef;

######## do top level directory ########
# not sure how this is going to be fixed



$fname_histo->add(length "/home2/atorrez/mpi");
$size     = (512 / 1024.0);  
$cap_used = ( 8 / 2.0);
$total_size     += $size;
$total_cap_used += $cap_used;
$dirkb_histo->add($size);
if($cap_used >= $size) {
    $ovhd_val = $cap_used - $size;
    $pos_ovhd_histo->add($ovhd_val);
}
else { 
    $ovhd_val = $size - $cap_used;
    $neg_ovhd_histo->add($ovhd_val);
}
# not sure how this is going to be fixed

##printf("XXXXXXXX tmp_count = %d %d %d %d\n", $tmp_count,$tmp_count_link,$tmp_count_file,$tmp_count_dir); 










# done with all directories, or we were interrupted midway.  print
# results so far.
if (!$interrupted) {
    print "----- CUT HERE ----- REPORT FOLLOWS ----- CUT HERE -----\n";
    printf("Generated by fsstats v%s (%s)\n", VERSION_NUM, VERSION_DATE);
}
print_rate($interrupted ? "interrupted" : "complete",
	   $fname_histo->{count}, time - $start_time);
if($skipped_hlink) {
    print "Skipped $skipped_hlink duplicate hardlinked files\n";
}
if($skipped_snapshot) {
    print "Skipped $skipped_snapshot snapshot dirs\n";
}
if($special_files) {
    print "Skipped $special_files special files\n";
}
if($err_count) {
    print "Encountered $err_count errors while walking filesystem tree\n";
    print "RESULTS MAY BE INCOMPLETE\n";
}
print_report($ckpt_file, $output_file);

# if we were interrupted, save a checkpoint to a temporary file
# so we can be restarted.
if($interrupted) {
    my ($fh, $fn) = File::Temp::tempfile("fsstats.XXXXX", DIR => File::Spec->tmpdir());

    if(defined $fh) {
	print STDERR "Interrupted while running.  Saving checkpoint to $fn.\n";
	print STDERR "Restart with \"fsstats -r $fn\".\n";
	write_ckpt($fh);
	close($fh);
    }
    else {
	print STDERR "$progname: creating checkpoint file failed: $!\n";
    }
}

##if($ckpt_interval && !defined $ckpt_file) {
 # delete periodic checkpoint if it's a temp file
##  close($temp_ckpt_fh);
##  unlink($temp_ckpt_fn);
##}

##### subroutines follow #####

# print current rate processing the files.
#
# the last three parameters ($last_cnt, $last_elap, $cwd) are optional
# and will be omitted if not passed in.
sub print_rate {
    my ($pfx, $cnt, $elap, $last_cnt, $last_elap, $cwd) = @_;

    my $rate_str = "";
    if(defined $last_cnt && $last_elap > 0) {
   $rate_str = sprintf(" (last %d at %.2f files/sec)", $last_cnt,
                       $last_cnt / $last_elap);
}
    elsif($elap > 0) {
	$rate_str = sprintf(" (at %.2f files/sec)", $cnt/$elap);
    }

 printf("%-10s: processed %d files in %d secs%s%s\n",
        $pfx, $cnt, $elap,
        $rate_str,
        ($cwd ? sprintf(", cwd \"%s\"", $cwd) : ""));
}

# print the final results.  if a dump file is specified, also dumps a
# checkpoint to that filename.  if an output file is specified, writes
# results to that file in CSV format.
sub print_report {
    my ($dump_file, $output_file) = @_;
    my $fh;

    if($output_file) {
	undef $fh;
	open $fh, ">$output_file";
	if(!defined $fh) {
	    warn "$progname: Can't open output file $output_file: $!\n";
	}
    }
    else {
	open $fh, ">&STDOUT";
	print "\n----- BEGIN CSV -----\n";
    }
    printf($fh "#Generated by fsstats v%s (%s)\n", VERSION_NUM, VERSION_DATE);
    printf($fh "#Comment: This is a comment line that can be modified or repeated before\n"); 
    printf($fh "#uploading to record voluntarily added information.\n\n");

    printf($fh "skipped special files,%d\n", $special_files);
    printf($fh "skipped duplicate hardlinks,%d\n", $skipped_hlink);
    printf($fh "skipped snapshot dirs,%d\n", $skipped_snapshot);
    printf($fh "total capacity used,%s\n", kb_to_print($total_cap_used));
    printf($fh "total user data,%s\n", kb_to_print($total_size));
    printf($fh "percent overhead,%f\n", ovhd_pct($total_size, $total_cap_used)/100);

    if(defined &Filesys::Statvfs::statvfs) {
	printf($fh "Filesystem total,%s\n", kb_to_print($total));
	printf($fh "Filesystem used,%s\n", kb_to_print($total-$avail));
    }

    print $fh "\n";
    $size_histo->print_csv($fh, "file size", "KB");
    $cap_histo->print_csv($fh, "capacity used", "KB");
    $pos_ovhd_histo->print_csv($fh, "positive overhead", "KB");
    $neg_ovhd_histo->print_csv($fh, "negative overhead", "KB");
    $dir_histo->print_csv($fh, "directory size (entries)", "ents");
    $dirkb_histo->print_csv($fh, "directory size", "KB");
    $fname_histo->print_csv($fh, "filename length", "chars");
    $hlink_histo->print_csv($fh, "link count", "links");
    $slink_histo->print_csv($fh, "symlink target length", "chars");
##  printf($fh "relative symlink target pct,%f\n" .
##           "absolute symlink target pct,%f\n",
##         $slink_relative ? $slink_relative / $slink_histo->{count} : 0,
##         $slink_absolute ? $slink_absolute / $slink_histo->{count} : 0);
    $mtime_files_histo->print_csv($fh, "mtime (files)", "days");
    $mtime_bytes_histo->print_csv($fh, "mtime (KB)", "days");
    $ctime_files_histo->print_csv($fh, "ctime (files)", "days");
    $ctime_bytes_histo->print_csv($fh, "ctime (KB)", "days");
    $atime_files_histo->print_csv($fh, "atime (files)", "days");
    $atime_bytes_histo->print_csv($fh, "atime (KB)", "days");

    if ($output_file) {
	close($fh) || warn "$progname: Can't close output file $output_file: $!\n";
	print "CSV WRITTEN TO $output_file\n";
    }
    else {
	print "\n----- END CSV -----\n";
    }
 # human-readable output
    print "\n----- BEGIN NORMAL -----\n";
# printf("skipped %d special files\n", $special_files);
# printf("skipped %d duplicate hardlinks\n", $skipped_hlink);
# printf("skipped %d snapshot dirs\n", $skipped_snapshot);

    if($dump_file) {
	undef $fh;
	open $fh, ">$dump_file";
	if(!defined $fh) {
	    warn "$progname: Can't open checkpoint file $dump_file: $!\n";
	}
	else {
	    write_ckpt($fh);
	    close($fh) || warn "$progname: Can't close checkpoint file $dump_file: $!\n";
	}
    }
}

# Write current program state to a checkpoint file.  We use
# Data::Dumper to write the checkpoint in a format that can be eval'd
# later to restore the state.
sub write_ckpt {
    my ($fh) = @_;

 my $d = Data::Dumper->new([\@dirlist, \%hlink,
                            $err_count, $skipped_hlink,
                            $slink_relative, $slink_absolute,
                            $skipped_snapshot,
                            $total_size, $total_cap_used,
                            $starting_dir,
                            $size_histo, $cap_histo, $pos_ovhd_histo, $neg_ovhd_histo, 
			         $dir_histo, $dirkb_histo, $fname_histo, $hlink_histo, $slink_histo, 
                            $mtime_files_histo, $mtime_bytes_histo, $ctime_files_histo, 
                            $ctime_bytes_histo, $atime_files_histo, $atime_bytes_histo],
                           [qw(*dirlist *hlink
                               err_count skipped_hlink
                               slink_relative slink_absolute
                               skipped_snapshot
                               total_size total_cap_used
                               starting_dir
                               size_histo cap_histo pos_ovhd_histo neg_ovhd_histo dir_histo
                               dirkb_histo fname_histo hlink_histo slink_histo 
                               mtime_files_histo mtime_bytes_histo ctime_files_histo
                               ctime_bytes_histo atime_files_histo $atime_bytes_histo)]);
    print $fh $d->Purity(1)->Indent(1)->Dump . "\n";
    print $fh "1;\n";
}

# convert a KB value to a "printable" value (GB, MB, or KB) depending
# on its magnitude. returns a string suitable for printing.
sub kb_to_print {
    my ($kb) = @_;
    my $num;
    my $unit;

    if($kb > 1024*1024*1024) {
	$num = $kb / (1024*1024*1024);
	$unit = "TB";
    }
    elsif($kb > 1024*1024) {
	$num = $kb / (1024*1024);
	$unit = "GB";
    }
    elsif($kb > 1024) {
	$num = $kb / 1024;
	$unit = "MB";
    }
    else {
	$num = $kb;
	$unit = "KB";
    }
    return sprintf("%.2f %s", $num, $unit);
}

# Convert a number of seconds to the format "NN days HH:MM:SS".  the
# "NN days" portion is optional and will be printed only if the time
# value is > 24 hrs.  Returns a string suitable for printing.
sub secs_to_print {
    my ($sec) = @_;
    my ($day,$hr,$min);

    $day = $hr = $min = 0;
    if($sec > 60*60*24) {
	$day = int($sec / (60*60*24));
	$sec -= $day*60*60*24;
    }
    if($sec > 60*60) {
	$hr = int($sec / (60*60));
	$sec -= $hr*60*60;
    }
    if($sec > 60) {
	$min = int($sec / 60);
	$sec -= $min*60;
    }
    if($day > 0) {
	return sprintf("%d day%s %d:%02d:%02d", $day, ($day > 1 ? "s" : ""), $hr, $min, $sec);
    }
    else {
	return sprintf("%d:%02d:%02d", $hr, $min, $sec);
    }
}

# Compute the percent overhead for a given capacity-used and size.
# This method of computing overhead computes "percentage of the
# capacity used that is overhead" and ranges from 0% (no overhead) to
# 100% (size==0 and cap>0, space is all overhead).
sub ovhd_pct {
    my ($size, $cap) = @_;

    if ($cap == 0) {
	return 0;
    }
    return (($cap - $size)/$cap)*100;
}

# Signal handler routine.  This is used to trap SIGINT and SIGQUIT.
# The first time we get a signal, we set a flag which is checked by
# the main directory loop, and causes the script to exit after it
# finishes processing the current directory and writes a checkpoint.
# If we get a second signal before we finish exiting, we just exit
# immediately, since the impatient user apparently wants us to do so.
sub sig_handler {
    my ($sig) = @_;

    if(!$interrupted) {
	$interrupted = 1;
	print "INTERRUPTED in $dir: shutting down\n";
    }
    else {
	print "Second signal received: immediate shutdown\n";
	exit 1;
    }
}

# Print usage message for the program.
sub usage {
    print "$progname: usage: $progname [options] path [path ...]\n";
    print "Options include:\n";
    print "  -c ckptfile        write a checkpoint before exiting normally\n";
    print "  -h                 print this help\n";
    print "  -o outfile         write results to named file in CSV format\n";
    print "  -r ckptfile        read initial state from named checkpoint\n";
    print "  -s                 print detailed timing statistics\n";
    print "  -v                 verbose mode\n";
}


##### Histo.pm #####

#
# Histo.pm
#
# Histogram module for Perl.
#
# Author: Marc Unangst <munangst@panasas.com>
#
# Copyright (c) 2005 Panasas, Inc.  All rights reserved.
#

use strict;

package Histo;

#
# Constructor for a new Histo object.  The arguments are a hash
# of parameter/value pairs.  The "min" and "incr" parameters
# must be supplied.  "max" and "log_incr" are optional.
#
sub new {
    my $type = shift;
    my %params = @_;
    my $self = {};

 die "Histo->new: required parameters not set\n"
     unless (defined $params{min} && defined $params{incr});

    $self->{min} = $params{min};
#  $self->{max} = $params{max}-1 if defined $params{max};
    $self->{max} = $params{max} if defined $params{max};
    $self->{incr} = $params{incr};
    if(defined $params{integer_vals}) {
	$self->{integer_vals} = $params{integer_vals};
    }  
    else {
	$self->{integer_vals} = 1;
    }

    $self->{count} = 0;
    $self->{total_val} = 0;


    if($params{log_incr}) {
	$self->{log_incr} = $params{log_incr};
	$self->{bucket_max} = [$self->{min}+$self->{log_incr}];
    }
    else {
	$self->{log_incr} = 0;
    }

    $self->{buckets} = [];
    $self->{buckets_val} = [];
    bless $self, $type;
}

#
# Add a new data point to the histogram.
#
# @arg  $val
#   Value to add to the histogram
# @arg  $count
#   Optional; if specified, the weight of the item being added.
#   Calling add($x, 3) is the same as calling add($x) three times.
#
sub add ($$;$) {
    my $self = shift;
    my ($val, $count) = @_;

    if(!defined $count) {
	$count = 1;
    }

    if(!defined $self->{min_val} || $val < $self->{min_val}) {
	$self->{min_val} = $val;
    }
    if(!defined $self->{max_val} || $val > $self->{max_val}) {
	$self->{max_val} = $val;
    }

#  if(int($val) != $val) {
#    $self->{integer_vals} = 0;
#  }

    $self->{count} += $count;
    $self->{total_val} += ($val*$count);
 #$self->{total_val} += $val;

    if(defined $self->{max} && $val > $self->{max}) {
	$self->{over_max} += $count;
	$self->{over_max_buckets_val} += $val*$count;
    }
    elsif($val < $self->{min}) {
	$self->{under_min} += $count;
	$self->{under_min_buckets_val} += $val*$count;
    }
    else {
	my $b;
	my $val_to_use = $val;


	if($self == $pos_ovhd_histo || $self == $neg_ovhd_histo) {
	    $val_to_use = $size;
	}

	if($self->{log_incr}) {
	    $b = 0;
	    my $x = $self->{bucket_max}[0];
	    while($val_to_use >= $x+1) {
		$x = $x*2 + 1;
		$b++;
		if($b > $#{$self->{bucket_max}}) {
		    $self->{bucket_max}[$b] = $x;
		}
	    }
	}
	else {
	    $b = int (($val_to_use - $self->{min}) / $self->{incr});
	}
   #print STDERR "sample $val into bucket $b\n";
	$self->{buckets}[$b] += $count;
	$self->{buckets_val}[$b] += $val*$count;

	if(!defined $self->{largest_bucket} ||
	   $self->{buckets}[$b] > $self->{largest_bucket}) {
	    $self->{largest_bucket} = $self->{buckets}[$b];
	}
    }
}

#
# Get maximum value of the specified bucket.
#
# @arg  $b
#   bucket number
#
# @internal
#
sub _get_bucket_max ($$) {
    my $self = shift;
    my ($b) = @_;
#  my $epsilon;   dont need this

#  if($self->{integer_vals}) {
#    $epsilon = 1;
#    $epsilon = 0;
#  }
#  else {
#   $epsilon = 0.1;
#  }

    if($self->{log_incr}) {
	if($b <= $#{$self->{bucket_max}}) {
#     return ($self->{bucket_max}[$b]-$epsilon);
	    return ($self->{bucket_max}[$b]);
	}
	else {
	    return undef;
	}
    }
    else {
   #return ($self->{incr}*($b+1))-$epsilon;
	return (($self->{incr}*($b+1)) -1); 
    }
}

#
# Get minimum value of the specified bucket.
#
# @arg  $b
#   bucket number
#
# @internal
#
sub _get_bucket_min ($$) {
    my $self = shift;
    my ($b) = @_;

    if($self->{log_incr}) {
	if($b == 0) {
	    return $self->{min};
	}
	elsif($b <= $#{$self->{bucket_max}}) {
#     return $self->{bucket_max}[$b-1]
	    return $self->{bucket_max}[$b-1]+1;
	}
	else {
	    return undef;
	}
    }
    else {
	return ($self->{min} + $self->{incr}*($b));
    }
}

#
# Print the histogram contents to STDOUT.
#
# @arg  $prefix
#   String to prefix each output line with.
# @arg  $unit_str
#   String that describes the units of the histogram items.
#
sub print ($$$) {
    my $self = shift;
    my ($prefix, $unit_str) = @_;
    my $c = 0;
    my $d = 0;
#  my $prev_pct = 0;

    my $width;
    my $fmt;
#  if ($self->{integer_vals}) {
    $width = length sprintf("%d", $self->_get_bucket_max($#{$self->{buckets}}));
    $fmt = "d";
# }
#  else {
#    $width = length sprintf("%.1f", $self->_get_bucket_max($#{$self->{buckets}}));
#    $fmt = ".1f"
#  }
    my $bwidth = 0;
    if (defined $self->{largest_bucket}) {
	$bwidth = length sprintf("%d", $self->{largest_bucket});
    }
    if($bwidth < 5) {
	$bwidth = 5;
    }

    my $bwidth_val = length sprintf("%.2f", $self->{total_val});

 printf("%scount=%d avg=%.2f %s\n", $prefix,
        $self->{count},
        $self->{count} > 0 ? $self->{total_val} / $self->{count} : 0,
        $unit_str);
    my ($min_val, $max_val);
    $min_val = defined $self->{min_val} ? $self->{min_val} : "0";
    $max_val = defined $self->{max_val} ? $self->{max_val} : "0";
 printf("%smin=%.2f %s max=%.2f %s\n", $prefix,
        $min_val, $unit_str, $max_val, $unit_str);

    if(defined $self->{under_min} &&  $self->{under_min} > 0) {
	$c += $self->{under_min};
	$d += $self->{under_min_buckets_val};
	printf("%s[%${width}s<%${width}${fmt} %s]: %${bwidth}d (%5.2f%%) (%6.2f%% cumulative) %${bwidth_val}.2f %s (%5.2f%%) (%6.2f%% cumulative)\n", $prefix, " ", $self->{min}, $unit_str, 
	       $c, ($c/$self->{count})*100, ($c/$self->{count})*100,
	       $d, $unit_str, ($d/$self->{total_val})*100, ($d/$self->{total_val})*100);
    }

    for(my $b = 0; $b <= $#{$self->{buckets}}; $b++) {
	if($self->{buckets}->[$b]) {
	    my $x = $self->{buckets}->[$b];
	    my $y = $self->{buckets_val}->[$b];

	    $c += $x;
	    $d += $y;

	    my $pct = ($x / $self->{count}) * 100;
	    my $cum_pct = ($c / $self->{count}) * 100;

     # if all the files parsed are zero bytes, the total_val will be zero but count will be a positive number
	    my $y_pct = 0;
	    my $y_cum_pct = 0;
	    if($self->{total_val}) {
		$y_pct = ($y / $self->{total_val}) * 100;
		$y_cum_pct = ($d / $self->{total_val}) * 100;
	    }

	    if ($self->{integer_vals}) {
		      printf("%s[%${width}${fmt}-%${width}${fmt} %s]: %${bwidth}d (%5.2f%%) (%6.2f%% cumulative) %${bwidth_val}.2f %s (%5.2f%%) (%6.2f%% cumulative) \n", $prefix,
			           $self->_get_bucket_min($b), $self->_get_bucket_max($b),
			     $unit_str, $x, $pct, $cum_pct, $y, $unit_str, $y_pct, $y_cum_pct);
		  }else {
		           printf("%s[%${width}${fmt}-%${width}${fmt} %s): %${bwidth}d (%5.2f%%) (%6.2f%% cumulative) %${bwidth_val}.2f %s (%5.2f%%) (%6.2f%% cumulative)\n", $prefix,
				        $self->_get_bucket_min($b), $self->_get_bucket_max($b)+1,
				  $unit_str, $x, $pct, $cum_pct, $y, $unit_str, $y_pct, $y_cum_pct);

		       }

#     $prev_pct = $cum_pct;
	}
    }
    if(defined $self->{over_max} && $self->{over_max} > 0) {
	$c += $self->{over_max};
	$d += $self->{over_max_buckets_val};
	printf("%s[%${width}s>%${width}${fmt} %s]: %${bwidth}d (%5.2f%%) (%6.2f%% cumulative) %${bwidth_val}.2f %s (%5.2f%%) (%6.2f%% cumulative)\n", $prefix, " ", $self->{max}, $unit_str, 
	       $self->{over_max}, ($self->{over_max} / $self->{count})*100, ($c / $self->{count})*100,
	       $self->{over_max_buckets_val}, $unit_str, ($self->{over_max_buckets_val} / $self->{total_val})*100, ($d/$self->{total_val})*100);
    }
}

#
# Print histogram contents to a CSV-format file.
#
# @arg  $fh
#   filehandle to print to
# @arg  $name
#   descriptive name of this histogram, to identify it in the file
# @arg  $unit_str
#   string that describes the units of the histogram items
#
sub print_csv {
    my $self = shift;
    my ($fh, $name, $unit_str) = @_;
    my $c = 0;
    my $d = 0;


    printf($fh "histogram,%s\n", $name);
 printf($fh "count,%d,items\n",
	$self->{count});
 printf($fh "average,%f,%s\n",
	$self->{count} > 0 ? $self->{total_val} / $self->{count} : 0,
	$unit_str);
    my ($min_val, $max_val);
    $min_val = defined $self->{min_val} ? $self->{min_val} : "0";
    $max_val = defined $self->{max_val} ? $self->{max_val} : "0";
    printf($fh "min,%d,%s\n", $min_val, $unit_str);
    printf($fh "max,%d,%s\n", $max_val, $unit_str);
    print $fh "bucket min,bucket max,count,percent,cumulative pct,val count,percent,cumulative pct\n";

    if (defined $self->{under_min} && $self->{under_min} > 0) {
	$c += $self->{under_min};
	$d += $self->{under_min_buckets_val};
   printf($fh "%d,%d,%d,%f,%f,%f,%f,%f\n",
	  -1, $self->{min}, $c, $c/$self->{count}, $c/$self->{count},
	  $d, $d/$self->{total_val}, $d/$self->{total_val});
    }

    for (my $b = 0; $b <= $#{$self->{buckets}}; $b++) {
	if (defined $self->{buckets}->[$b] && $self->{buckets}->[$b] != 0) {
	    my $x = $self->{buckets}->[$b];
	    my $y = $self->{buckets_val}->[$b];

	    $c += $x;
	    $d += $y;

	    my $pct = $x / $self->{count};
	    my $cum_pct = $c / $self->{count};

     # if all the files parsed are zero bytes, the total_val will be zero but count will be a positive number
	    my $y_pct = 0;
	    my $y_cum_pct = 0;
	    if($self->{total_val}) {
		$y_pct = $y / $self->{total_val};
		$y_cum_pct = $d / $self->{total_val};
	    }

	    if($self->{integer_vals}) {
        printf($fh "%d,%d,%d,%f,%f,%f,%f,%f\n",
              $self->_get_bucket_min($b), $self->_get_bucket_max($b),
	       $x, $pct, $cum_pct, $y, $y_pct, $y_cum_pct);
    }
	    else {
        printf($fh "%d,%d,%d,%f,%f,%f,%f,%f\n",
              $self->_get_bucket_min($b), $self->_get_bucket_max($b)+1,
	       $x, $pct, $cum_pct, $y, $y_pct, $y_cum_pct);
    }
	}
    }

    if (defined $self->{over_max} && $self->{over_max} > 0) {
	$c += $self->{over_max};
	$d += $self->{over_max_buckets_val};
    printf($fh "%d,%d,%d,%f,%f,%f,%f,%f\n",
	   $self->{max}, -1, $self->{over_max}, $self->{over_max}/$self->{count}, $c/$self->{count},
	   $self->{over_max_buckets_val},  $self->{over_max_buckets_val}/$self->{total_val}, $d/$self->{total_val});
    }
    print $fh "\n";
}

__END__

=head1 NAME

fsstats - Collect statistics about a filesystem

=head1 SYNOPSIS

   fsstats [options] dir [dir ...]

=head1 DESCRIPTION

Walk a file tree and gather statistics about it.  Currently we
collect the following pieces of information:

=over

=item *

file size

=item *

file capacity used

=item *

directory size (number of entries & KB)

=item *

filename length

=item *

symlink target length

=item *

number of hardlinks

=item *

file age (by file and by KB)

=back

The script intentionally uses only standard POSIX calls to obtain
information about the underlying filesystem.  This limits the
information which can be collected, but maximizes portability.

Output can be produced in three formats:

=over

=item 1.

Plain text format, printed to stdout.

=item 2.

A comma-separated value (CSV) file, suitable for spreadsheet import or
further processing by another script.

=item 3.

A checkpoint file that records the internal state of the program, for
restarting later or directly importing the data into another Perl
script.

=back

=head2 Options

=over

=item B<-c> I<ckptfile>

Write a checkpoint file to I<ckptfile> when exiting normally.  Without
this option, a checkpoint is only generated if B<fsstats> exits
prematurely.  This file is also used for the periodic checkpoints if
B<-i> I<ckpt_interval> is specified.

=item B<-h>

Print help, then exit.

=item B<-i> I<ckpt_interval>

Specify the interval (in minutes) between periodic checkpoints.  The
default is 10 minutes.  Setting I<ckpt_interval> to 0 disables
periodic checkpoints.

=item B<-o> I<outfile>

Write comma-separated value (CSV) output to I<outfile> when exiting
normally.  (Default is to print this to stdout.)

=item B<-r> I<ckptfile>

Read initial state from I<ckptfile>.  With this option,
any directories specified on the command line are ignored in favor of
the list of directories in the checkpoint.

=item B<-s>

Print detailed timing statistics.  Requires the C<Time::HiRes>
module.

=item B<-v>

Verbose mode.  Print incremental text-mode results in every periodic
status message.

=back

=head1 EXAMPLES

Collect filesystem statistics for the /usr1 heirarchy, printing
results on stdout:

   fsstats /usr1

Restart from a checkpoint file, writing output in CSV format to
/tmp/usr1.csv:

   fsstats -r /tmp/fsstats.abc123 -o /tmp/usr1.csv

=head1 CAVEATS

If your perl was compiled without 64-bit integer support, files larger
than 4 GB will not be tabulated correctly.  This is a limitation of
the C<stat>/C<lstat> calls built into perl, so there is no way to work
around it from the script.  B<fsstats> will print a warning if it
detects it is running in a perl interpreter without 64-bit int
support.

To enable printing C<statvfs> information about the filesystem,
you will need to install the C<Filesys::Statvfs> module.
It is available from CPAN
(http://search.cpan.org/~iguthrie/Filesys-Statvfs_Statfs_Df-0.75/).

To use the B<-s> option, you will need to install the C<Time::HiRes>
module, which may be packaged with your OS distribution
and is also available from CPAN.

=head1 AUTHOR

Marc Unangst, Panasas Inc. <munangst@panasas.com>

http://www.panasas.com/

=head1 COPYRIGHT

Copyright (c) 2005, 2006 Panasas, Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

=cut

# Local Variables:
# indent-tabs-mode: nil
# tab-width: 2
# End:
