#!/usr/bin/perl
#Copyright (C) Pablo Funes, 1999-2014. pfunes@gmail.com

# TODO: support fields ending in % as numerical; also $ prefix - it requires passing a conversion function from struct_guesser to the copy command, hum.  

use strict;

use File::Basename;    	# needed for dirname below

use lib (dirname($0)); 	# add my own location to search for struct_lib.pl 

use Pg; 				# Postgres database module

my $status=do("struct_lib.pl");	# load my own set of postgres-related libraries

die "Failed to load struct_lib.pl, must reside in same directory as $0. Stopped.\n Error messages: \n(1) $@\n(2) $!"
  unless (defined($status));

exit(-1) unless ($status);		# error initializing struct_lib package

# print out usage message on tty
sub usage {
  if (defined($_[0])) { print STDERR $_[0],"\n"; }
  print STDERR "usage:\n";
  print STDERR "CREATE_FROMTXT.PL Create a postgres table from a delimited text file. 

This program creates a new table from a delimited ascii file, as a
two-pass process:

    1. Scan the database an guess the structure of the table.
    2. Create a new table with that structure and fill it in. 

The ascii input file must be in either delimited format, or CSV format
(as defined by the Text::CSV Perl library).

";

  print STDERR "Usage: $0 [options] txtfile tablename\n\n";
  print STDERR "Options:
    -nocreate     Skip structure guess & creation step, use existing table (see -trows).
    -like name    Skip structure guess step, copy structure from another table (see -trows). 
    -nosql        Send output to stdout rather than trying to talk to psql
    -nf n         Number of fields on the table involved, if different from no. of fields on the input
    -h name    Host name (defaults to PGHOST/localhost)
    -db name      Database name (defaults to PGDATABASE)
    -schema name  Schema name (defaults to public). Might not work properly yet. Try manually specifying
                  the schema in the table name: schemaname.tablename.
    -b            Remove all spaces.
    -s            Separator (default: tab). Use perl expression, e.g. '[ \t]+'
    -ns sep       Title row separator, if different from separator.
    -trows n      Number of header rows (default: 1) If more than one, column names are concatenated 
                  across all header rows. Set to zero (or -tfile /dev/null) if no headers are present 
                  in the datafile and you are using -like or -nocreate.
    -v            Verbose output.
    -csv          Input file is in csv format.
    -tfile name   Name of file to read column names from
    -skip         Skip empty lines
    -space        Clean up space (delete trailing/leading, turn double spaces into singles)
    -num          Add a line column to table and fill with sources' line number for each record
    -lower        Convert all letters to lowercase
    -infermax     Infer structure from only these many lines instead of the whole data file. 
    -dmy          Use european order for date fields (does not work yet.)
    -varchar      Use varchar(maxlen) for strings instead of text\n
";
  exit(-1);
}

# initialize variables
our $dbname=$ENV{PGDATABASE}; # database, defaults to PGDATABASE variable 
our $host=$ENV{PGHOST};       # host, defaults to PGHOST variable or localhost
$host="localhost" unless defined($host);

our $sql_verbose = 0;     # set to 1 to print out all sql interaction
  
my $separator="\t";		  # field separator
my $nameseparator=$separator;# field separator on column name row
my $cleaneol=0;              # Boolean: ignore last, empty column if present
my $noblanks=0;			  # See -b
my $lineno=0;                # See -num
my $usetext=1;               # See -varchar
my $trows=1;

my $verbose=0;

my $create=1;
my $nosql=0;
my $numfields=-1;
my $csvmode=0;
my $skipEmpty=0;
my $space=0;
my $tolower=0;
my $dmy=0;
my $infermax=undef;
my $like=undef;          # See -like
my $schema=undef;        # See -schema
my $tfile=undef;		# See -tfile

my $file = undef;       # input file
my $tablename = undef;  # name of table we're creating
my $csv;                # csv reader, created as needed


# parse command line arguments
while ($_=shift(@ARGV)) {
  if (/^-/) {
    if (/-v$/) {
       $verbose=1+$verbose;
    }
    elsif (/-csv/) {
      $csvmode=1;
      use Text::CSV_XS;
      use Text::CSV;
      #$csv=Text::CSV->new({binary => 1, allow_loose_escapes => 1, escape_char => '|' });
      $csv=Text::CSV->new({binary => 1 });
      $nameseparator="CSV";
      $separator="CSV";
    }
    elsif (/-varchar/) {
      $usetext=0;
    }
    elsif (/-num/) {
      $lineno=1;
    }
    elsif (/-space/) {
      $space=1;
    }
    elsif (/-like/) {
      $like = shift(@ARGV);
    }
    elsif (/-lower/) {
      $tolower=1;
    }
    elsif (/-infermax/) {
    	$infermax = shift(@ARGV);
    }
    elsif (/-trows/) {
      $trows=shift(@ARGV);
    }
    elsif (/-nocreate/) {
      $create=0;
    }
    elsif (/-nf$/) {
    	$numfields=shift(@ARGV);
	}
    elsif (/-s$/) {
      $separator=shift(@ARGV);
      $nameseparator=$separator;
    }
    elsif (/-b$/) {
      $noblanks = 1;
    }
    elsif (/-ns/) {
      $nameseparator=shift(@ARGV);
    }
    elsif (/-nosql/) {
      $nosql=1;
    }
    elsif (/-db/) {
      $dbname=shift(@ARGV);
    }
    elsif (/-h(ost)?$/) {
      $host=shift(@ARGV);
    }
    elsif (/-schema/) {
      $schema=shift(@ARGV);
    }
    elsif(/-tfile/) {
      $tfile=shift(@ARGV);
    }
    elsif(/-dmy/) {
     $dmy=1;
    }
    elsif(/-skip/) {
      $skipEmpty=1;
    }
    else {
      usage("$0: Bad argument ($_)");
    }
  }
  else {
    if (!defined($file)) {  $file=$_; }
    else { $tablename=$_; }
  }
}

usage() unless (defined($file));


if (!($nosql)) {
  $status=do("sqllib.pl");
  die "$0: Failed to load sqllib.pl. Sqllib.pl must reside in the same directory as $0.\nStopped"
    unless (defined($status));
  die "$0: Database not open. Stopped"
    unless ($status);
}

unless (defined($tablename)) {
  # table name not specified, use filename
  $tablename=lc($file);
  if ($tablename =~ /\/([^\/]+)$/) {
    # remove path
    $tablename=$1;
  }
  if ($tablename =~ /^([^.]+)[.].*$/) {
    # remove extension
    $tablename = $1;
  }
}
else {
  $tablename=lc($tablename);
}

if ($dmy) {
  print STDERR "Setting datestyle to DMY\n";
  sql("set datestyle to dmy");
}

print STDERR "$0: file=$file table=$tablename create=$create \n" if ($verbose);
#$tablename =~ s/[.,;:'"-]/ /g;
# do not remove . as it may be the schema !
$tablename =~ s/[,;:'"-]/ /g;
$tablename =~ s/ +$//g;
$tablename =~ s/^ +//g;
$tablename =~ s/ /_/g;



print STDERR "file=$file\ntable name=$tablename\n" if ($verbose);

print STDERR "\n" if ($verbose);


if ($create) {
  my $cmd;
  if (defined($like)) {
  	$cmd = "create table $tablename (like $like);"
  }
  else {
	  print STDERR "Guessing structure...\n";
	  my $struct=struct_guesser($verbose,$separator,$nameseparator,$file,$tfile,$usetext,$trows,$infermax,$space);
	  chomp $struct; # drop trailing newline
	  $struct =~ s/[ \t]*$//; # drop trailing blanks
	
	  if ($lineno) {
	    $struct =~ s/[)]$/, line serial)/ or die "wrong structure syntax in [$struct]";
	  }
	  die "Error: bad structure <$struct>" unless ($struct =~ /^[\(](.+)[\)]$/);
	  if ($numfields>0) {
	      my @flds = split /,/,$1;
	      if (@flds < $numfields) {
	          print STDERR ($#flds+1)," column names found, padding to $numfields\n";
	          for (my $i=$#flds+1; $i<$numfields;$i++) {
		    push(@flds,"col$i");
		  }
		  $struct="(" . join(',',@flds) . ")";
	      }
	      elsif (@flds > $numfields) {
	          print STDERR ($#flds+1)," column names found, truncating to $numfields\n";
		  $#flds = ($numfields-1);
		  $struct="(" . join(',',@flds) . ")";
	      }
	  }
	  print STDERR "Structure will be:\n$struct\n";
	  $cmd="create table $tablename $struct;";
  }
  if ($nosql) {
    print "$cmd\n";
  }
  else {
    sql_nocheck("drop table $tablename;");
    sql($cmd);
  }
}

unless (defined($file)) { usage(); }

open I,"<$file" or die "can't open $file";
# skip title rows
unless (defined($tfile)) {
  for (my $i=0;$i<$trows;$i++) {
    my $skip=<I>; # ignore title rows in reading data
  }
}

my @fieldnames=(); 
if ((not $nosql) and ($numfields<0)) {
  @fieldnames=getfields($tablename);
  if ($lineno) { pop(@fieldnames); }
  $numfields=1+$#fieldnames;
  print join(",",@fieldnames)."\n";
  print "$0: found $numfields fields in table $tablename\n";
}

if ($nosql) {
  open PIPE,">& STDOUT" or die "failed to dup stdin??";
}
else {
  open PIPE,"| psql -h $host $dbname -e -f -" or die "failed to open pipe to psql";
}

my $schemaname;
if (defined($schema)) { $schemaname="$schema.$tablename"; }
else {$schemaname = $tablename;}
my $fieldlist;
if (@fieldnames > 0) {
	$fieldlist = "(" . join(',',@fieldnames) . ")";
}
else {
	$fieldlist="";
} 
print PIPE "copy $schemaname $fieldlist from stdin delimiters '|' with null as '';\n";
$|=1;
my $count=0;
my $line=0;
my $lastFieldCount=0;
my @field_arr;
if ($csvmode) {
	
}
loop: while(1) {
  $line++;
  print STDERR "\r $line   " if (($line % 1000)==0);
  #print STDERR "$line\n";
  unless ($csvmode) {
    $_=<I>;
    unless ($_) { last loop; }	
    chomp;	
    # windows file format fix
    s/\r$//;	
    # Break into fields
    # TODO: this won't accept escaped separators. 
    s/[$separator]*$// if ($cleaneol);
    s/(^[$separator])[nN][uU][lL][lL]([$separator]$)//g; # replace null string with null field
    s/[|]/\\|/g unless ($separator eq '|');
    @field_arr=split(/[$separator]/,$_,-1);
  }
  else {
  	# CSV mode
  	my $row = $csv->getline(*I);
  	unless ($row) { 
    #unless($csv->parse($_)) 
       if ($csv->eof()) { last loop; }
      else { print STDERR "Text::CSV Says \"not in CSV format\"\n",
			"Offending line:\n",
			"<$_>\n";
      		die "Stopped";
      }
    }
    @field_arr=@$row;
    #@field_arr=$csv->fields();
    for (my $i=0;$i<@field_arr;$i++) {
      $_ = $field_arr[$i];
      s/[\r\n]//g;
      s/^[Nn][uU][lL][lL]$//;
      s/([|\\])/\\$1/g; # escape |'s and \'s
      $field_arr[$i] = $_;
    }
  }
  if ($lastFieldCount != @field_arr) {
    $lastFieldCount = (0+@field_arr);
    if ($lastFieldCount != $numfields) {
      if (@field_arr > $numfields) {
	print STDERR "WARNING line $line: ",(0+@field_arr)," fields found, chopping off to $numfields\n";
      }
      else {
	print STDERR "WARNING line $line: ",(0+@field_arr)," fields found, completing (with null) to $numfields\n";
      }
    }
  }
  $#field_arr = ($numfields-1) if ($numfields > 0);
  #while ((not defined($x=$field_arr[$#field_arr])) or ($x eq "")) {
  #	pop(@field_arr);
  #	print "$#field_arr\n";
  #  	}
  $_=join('|',@field_arr);
  #s/\t/,/g;
  s/ //g if ($noblanks);
  $_ = lc($_) if ($tolower);
  if ($space) {
    s/  +/ /g;
    s/^ +//g;
    s/\| +/\|/g;
    s/ +$//g;
    s/ +\|/\|/g;
    # print STDERR "<$_>\n";
  }
  s/\r//g;
 
  #print STDERR $_;
  unless ($skipEmpty and (length($_)<@field_arr)) {
    $count=$count+1;
    if ($verbose>1) {
      print STDERR ">>",$_,"\n";
    }
    print PIPE $_;
    print PIPE "\n";
  }
}
print PIPE "\\.\n";
close PIPE;
close I;
print STDERR " TOTAL $count records onto $schemaname\n---------------\n";
