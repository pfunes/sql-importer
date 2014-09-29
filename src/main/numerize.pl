#!/usr/bin/perl
#
$doc="
NUMERIZE.PL   Convert string fields to numbers.

Creates table n_tablename from table tablename, replacing string names
with numerical fields and, optionally, date fields with seconds after
GMT (unix time).

String fields are \"numerized\", that is: sorted first, then first
entry is assigned the number 1, second entry the number 2, and so on.

For each string field that is numerized, a table named t_fieldname is
built that has two columns:

    1. Fieldname, the new numerical value
    2. T_fieldname: the old, string value.

";

$usage = "tablename [-noreuse] [-relevant] [-dates] [-only field(s)] [-drop field(s)] [-verb field(s)] [-keep field(s)]";
$options="
Options:

-noreuse:    Recreate (as opposed to reuse) existing t_tables.

-[no]relevant:   (Don't) Drop fields that are 'irrelevant' (either have all the same
             or all different values). Default is -relevant, so irrelevant fields are delted. 

-dates:      Perform date translation (date fields are converted to seconds
             after GMT).

-drop:       List of fields to be ignored completely.

-keep:       (Relevant mode only) numerize this field even if irrelevant.

-only:       List of fields to be processed. Drop all others.

-numerize:   List of fields to be numerized, even if they were numeric already.

-verb:       List of verbatim fields (copied, not numerized).

-anyway:    Proceed even if resulting operation is trivial.
";

use File::Basename;

use lib (dirname($0));

use Pg;
$status=do("sqllib.pl");
exit(-1) if ($status == 0);

die "Failed to initialize sql library.\nSqllib.pl must be in the same directory as $0.\nStopped" 
  unless (defined($status));

$table=shift(@ARGV);

sub usage { 
  print STDERR "$_[0]\nusage: $0 $usage\n";
  print STDERR "$doc\n$options\n";
  exit(1);
}

usage() unless (defined ($table));

%keep=();
%drop=();
%verb=();
%renamed=();

$mode=0;
$only=0;
$noreuse=0;
$relevant=1;
$dates=0;
$anyway=0;

while ($_ = shift(@ARGV)) {
  if (/^-/) {
    if ($_ eq "-keep") { $mode=1; }
    elsif ($_ eq "-rename") { $mode=4; }
    elsif ($_ eq "-drop") { $mode=2; }
    elsif ($_ eq "-only") { $only=1; $mode=1;}
    elsif ($_ eq "-verb") { $mode=3; }
    elsif ($_ eq "-numerize") { $mode = 5; }
    elsif ($_ eq "-noreuse") { $noreuse=1; }
    elsif ($_ eq "-relevant") { $relevant=1; }
    elsif ($_ eq "-norelevant") { $relevant=0; }
    elsif ($_ eq "-dates") { $dates=1; }
    elsif ($_ eq "-anyway") { $anyway=1; }
    else { usage("$_: wrong option"); }
  }
  else {
    if ($mode == 1) { $keep{$_}=1; push @keep,$_; }
    elsif ($mode == 2) { $drop{$_}=1; }
    elsif ($mode == 3) { $verb{$_}=1; }
    elsif ($mode == 4) { $rename{$_}=shift(@ARGV); }
    elsif ($mode == 5) { $numerize{$_} = 1; }
    else { usage("$_ unknown"); }
  }
}


print "NUMERIZING TABLE $table into n_$table\n";

@alltables=gettables();

for (@alltables) {
  #print "TABLE $_\n";
  $tables{$_}=1;
  }
  
if ($tables{"n_$table"}) {
  sql("drop table n_$table");
}

if ($only) { @fields = @keep; }

@fields=getfields($table) if (@fields <= 0);

$ftype=getfield_types($table);

#print "types=" .%ftype ."\n";

@verbatim=();

sql("BEGIN;");

$count = (sql_select("select count(*) from $table"))->getvalue(0,0);

print "$table: $count records\n";

foreach $k (@fields) {
  unless ($drop{$k}) {
    $typ=$ftype->{$k};
    if (defined($numerize{$k})) {
      # user explicitly asked to numerize this field
      push @nonum,$k; 
    }
    else {
      if (($typ =~ /^int/) or ($typ =~ /^float/) or ($verb{$k})) {
	# user explicitly asked to keep field unchanged, or is numeric
	push @verbatim,$k;
      }
      elsif (($typ =~ /^date/) or ($typ =~/^time/) or ($typ =~ /^timestamp/)) {
	if ($dates) {
	  # date field, convert to seconds after gmt if -dates option
	  push @dates, $k;
	}
	else {
	  # date field, keep unchanged (default)
	  push @keep, $k; }
      }
      else {
	# not numeric, numerize this field
	push @nonum,$k
      };
    }
  }
}


foreach $rawfield (@nonum) {
  if (defined($rename{$rawfield})) { $field = $rename{$rawfield}; }
  else { $field = $rawfield; }

  print "numerizing $field...\n";
  $ntable="$n_$table_$field";
  if ($tables{"t_$field"} and not $noreuse) {
    print ("Table t_$field already exists, using it.\n");
    push @numerized, $rawfield;
  }
  else {
    if ($tables{"t_$field"}) {
      sql("drop table t_$field");
    }
    sql("create temporary sequence s");
    sql("create table t_$field as"
	." select nextval('s') as $field,$rawfield as t_$field from "
	." (select distinct $rawfield from $table order by $rawfield) as x");
    sql("drop sequence s");
    $r=sql_select("select count(*) from t_$field");
    $fldcount=$r->getvalue(0,0);
    if (($fldcount >= $count or $fldcount <=1) and $relevant and not $keep{$field}) {
      print "WARNING: $field is irrelevant, removing.\n";
      sql("drop table t_$field");
    }
    else {
      print "FOUND $fldcount different classes in $field\n";
      push @numerized,$rawfield;
    }
  }
}

if (@numerized <= 0 and @dates <= 0) {
  if ($anyway) {
    print STDERR "Nothing to do! Proceeding anyway...\n";
  }
  else {
    print STDERR "Nothing to do! quitting...\n";
    exit(-1);
  }
}

# we build the sql create table as: create table n_$table as select
# @qfields from $table join @joins. 

@qfields=();
@joins=();

# copy numeric/unprocessed fields as they are

push @qfields,@verbatim;

# convert date fields to seconds after 01-01-1970.

foreach $rawfield (@dates) {
  if (defined($rename{$rawfield})) { $field = $rename{$rawfield}; }
  else { $field = $rawfield; }
  push @qfields, "extract(epoch from $rawfield-'1970-1-1'::timestamp) as $field";
}

# insert numberic values for numerized fields by means of a join. 

foreach $rawfield (@numerized) {
  if (defined($rename{$rawfield})) { $field = $rename{$rawfield}; }
  else { $field = $rawfield; }
  push @joins,"left outer join t_$field on t_$field.t_$field = x.$rawfield"; 
  push @qfields,"t_$field.$field";
  #print $field,"\n";
}



$sql_cmd="create table n_$table as select"
  . "\n  " . join(",",@qfields)
  . "\n  from $table x\n" 
  . join("\n",@joins)
  ;

print "Creating table n_$table ...\n";
sql($sql_cmd);
sql("COMMIT;");

print "$0: Finished.\n";
