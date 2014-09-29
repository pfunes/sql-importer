#!/usr/bin/perl

use File::Basename;

use lib (dirname($0));

use Pg;

$status=do("struct_lib.pl");

die "Failed to load struct_lib.pl, must reside in same directory as $0. Stopped"
  unless (defined($status));

exit(-1) unless ($status);

sub usage {
  print STDERR "$0: $_[0].\n\n",
    "STRUCT_GUESSER.PL Scan a data file and define a database structure 
suitable for it.\n\n",
    "usage: $0 [options] filename\n\n",
    "options:
-v:                 Give verbose output.
-s [sep]:           Separator (deafult: tab).
-ns [sep]:          Title row separator (default: same as separator).
-tfile [tfilename]: Name of file containing titles (if not first 
                    line of filename).
-varchar:           Use varchar(n) for string fields (defaults 
                    to using 'text' for all strings).

";
  exit(1);
}
$verbose=0;
$separator="\t";
$nameseparator=undef;
$tfile=undef;
$usetext=1;

while( $x=shift(@ARGV)) {
  if ($x eq "-v") { $verbose=1; }
  elsif ($x eq "-s") { $separator=shift(@ARGV); }
  elsif ($x eq "-ns") { $nameseparator=shift(@ARGV); }
  elsif ($x eq "-tfile") { $tfile=shift(@ARGV); }
  elsif ($x eq "-varchar") { $usetext=0; }
  elsif ($x =~ /^-/) { usage("bad argument $x");
		       exit(1); }
  else { $file = $x; }
}

unless(defined($file)) { usage("Missing file name"); }

$nameseparator=$separator unless(defined($nameseparator));

$struct=struct_guesser($verbose,$separator,$nameseparator,$file,$tfile,$usetext);
print "$struct\n";
