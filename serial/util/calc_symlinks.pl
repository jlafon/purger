#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
my @result;
my $slink_absolute;
my $slink_relative;
my $tgt;
my $err_count;

my $dbh = DBI->connect('DBI:Pg:dbname=cap1','','')
             or die "Couldn't connect to database: " . DBI->errstr;
my $cnt_query = "select count(*) from snapshot where mode & B'00000000000000001010000000000000' = '00000000000000001010000000000000'";

my $query = "select filename from snapshot where mode & B'00000000000000001010000000000000' = '00000000000000001010000000000000'";

my $sth = $dbh->prepare($cnt_query);
$sth->execute();
my $symlink_cnt = $sth->fetchrow_array();

print "$symlink_cnt\n";
undef($sth);



$sth = $dbh->prepare($query);
$sth->execute();
if ($sth->rows != 0) {
  while (@result = $sth->fetchrow_array()) {
#    print "$result[0]\n";
     tgt = readlink($result[0]);
    if(defined $tgt) {
      if ($tgt =~ m/^\//) {
        $slink_absolute++;
      } 
      else {
        $slink_relative++;
      }
    }
    else {
      warn "Cannot readlink $result[0]; skipping: $!\n";
      $err_count++;
    } 
  }
}
else {
}
printf("relative symlink target pct,%f\n" .
       "absolute symlink target pct,%f\n",
        $slink_relative ? $slink_relative / $symlink_cnt : 0,
        $slink_absolute ? $slink_absolute / $symlink_cnt : 0);

undef($sth);
$dbh->disconnect;
$dbh= undef;

