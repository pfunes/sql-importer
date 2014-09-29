#!/usr/bin/perl
#
$doc="
MKSHORTSCHEMA.PL  Create a new schema with short versions of the tables in the first one.
";

$usage = "oldschema newschema";
$options="
Options:

-max n:       Max entries per table in new schema
";

use File::Basename;

use lib (dirname($0));

use Pg;
$status=do("sqllib.pl");
exit(-1) if ($status == 0);

die "Failed to initialize sql library.\nSqllib.pl must be in the same directory as $0.\nStopped" 
  unless (defined($status));

sub usage { 
  print STDERR "$_[0]\nusage: $0 $usage\n";
  print STDERR "$doc\n$options\n";
  exit(1);
}

my $oldschema,$newschema;
my $max=1000;

while ($_ = shift(@ARGV)) {
  if (/^-/) {
    if ($_ eq "-max") { $max=shift(@ARGV); }
    else { usage("$_: wrong option"); }
  }
  else {
    if (defined($newschema)) { usage("Too many arguments"); }
    elsif (defined($oldschema)) { $newschema = $_; }
    else { $oldschema = $_; }
  }
}

usage("not enough arguments") unless defined($newschema);

@tables=gettables($oldschema);
sql_nocheck("drop schema $newschema cascade;");
sql("begin");
sql("create schema $newschema");
for $table (@tables) {
  sql("create table $newschema.$table as select * from $oldschema.$table limit $max");
}
sql("commit");
