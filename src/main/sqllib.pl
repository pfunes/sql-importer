#!/usr/bin/perl

# global variables:
#
# $dbname   Name of database
# $schema   Name of default schema, if any
# $conn     Postgres connection (see man Pg)
# $sql_verbose Print out command feedback (default: true)

use strict;

use Pg;

our $sql_verbose;

$sql_verbose = 1 unless (defined($sql_verbose));
#$sql_verbose = 1;

our $dbname=$ENV{PGDATABASE} unless defined($dbname);
our $schema=$ENV{PGSCHEMA} unless defined($schema);

our $host; 
my $conn;

if ((not defined($dbname)) and -f ".database") {
  open I,"<.database";
  $dbname=<I>;
  chomp $dbname;
  $host=<I>;
  close I;
  $schema=<I>;
}

our $host; 

if (not defined($host)) {
  $host = $ENV{PGHOST};
  $host="localhost" unless (defined($host));
}

unless (defined($dbname)) {
  print STDERR "$0: Don't know which database to connect to.\n";
  0
}
else {
  print STDERR "sql: connecting to db <$dbname> at host <$host>... "
    if ($sql_verbose);
  
  
  if (defined($dbname))
    {  $conn = Pg::connectdb("host=$host dbname=$dbname"); }
  else
    {  $conn = Pg::connectdb; }
  
  if ($conn->status eq PGRES_CONNECTION_OK) {
    print STDERR "connect.\n";
    if (defined($schema) and (length($schema)>0)) {
      $sql_verbose=1;
      sql("set search_path to $schema,public");
    }
    1;
  }
  else {
    my $current=select(STDERR); 
    $|=1;
    select($current);
    print STDERR "sql: CONNECT FAILED: ",$conn->errorMessage,"\n";
    0;
  }
}

sub sql {
  print STDERR $_[0]."...\n" if ($sql_verbose);
  my $r=$conn->exec($_[0]);
  my $status=$r->resultStatus;
  if ($status eq PGRES_TUPLES_OK) {
    print STDERR "TUPLES OK:" . $r->cmdStatus."\n" if ($sql_verbose);
    }
  elsif ($status ne PGRES_COMMAND_OK) {
    print STDERR "CMD STATUS:" . $r->cmdStatus."\n";
    print STDERR "ERROR MESSAGE: ". $conn->errorMessage . "\n" ;
    print STDERR "STATUS: " . $r->resultStatus . "\n";
    die "PGRES_FATAL_ERROR\n" if ($status eq PGRES_FATAL_ERROR);
    print STDERR "PGRES_NONFATAL_ERROR\n" if ($status eq PGRES_NONFATAL_ERROR);
    print STDERR "PGRES_BAD_RESPONSE\n" if ($status eq PGRES_BAD_RESPONSE);
  }
  else {
    print STDERR "COMMAND_OK: " . $r->cmdStatus."\n" if ($sql_verbose);
    }
  return $r;
}

sub sql_nocheck {
  print STDERR $_[0]."\n" if ($sql_verbose);
  my $r=$conn->exec($_[0]);
  return $r;
  }

sub sql_select {
  my $r = sql($_[0]);
  die "Expcted tuples ok, got other " unless ($r->resultStatus eq PGRES_TUPLES_OK);
  return $r;
}

sub getfields_v7 {
  my $t = $_[0];
  my $schname;
  if (defined($_[1])) 
    { $schname = $_[1]; }
  elsif (defined($schema)) 
    {   $schname=$schema;   }
  else 
    { $schname = 'public'; }
  my $r = $conn->exec("SELECT a.attname AS field"
		      ." FROM pg_class c,pg_attribute a, pg_namespace n"
		      ." WHERE c.relname = \'$t\'and a.attnum > 0 and a.attrelid = c.oid"
		      ." AND c.relnamespace = n.oid"
		      ." AND n.nspname = \'$schname\'"
		      ." ORDER BY a.attnum;");
  my @res=();
  for (my $i=0;$i < $r->ntuples;$i++) {
    push @res,$r->getvalue($i,0);
  }
  return @res;
}


sub getfields {
  my $t = $_[0];
  my $schname;
  if (defined($_[1])) 
    { $schname = $_[1] . '.'; }
  elsif (defined($schema)) 
    {   $schname=$schema . '.';   }
  
  my @res=();
  my $r = sql("SELECT * from $schname$t limit 0");
  
  for (my $i=0; $i< $r->nfields; $i++) {
  	push @res,$r->fname($i);
  }
  
  return @res;
}
	


sub getfield_types {
  my $t = $_[0];
  my ($f,$typ);
  my $r = $conn->exec("SELECT a.attname AS field,typname"
		   ." FROM pg_class c,pg_attribute a, pg_type u"
		   ." WHERE c.relname = \'$t\'"
		   ." and a.attnum > 0 and a.attrelid = c.oid"
		   ." and a.atttypid = u.oid"
		   ." ORDER BY a.attnum;");
  my $res;
  my $i,$f,$typ;
  for ($i=0;$i < $r->ntuples;$i++) {
    $f=$r->getvalue($i,0);
    $typ=$r->getvalue($i,1);
    $res->{$f}=$typ;
  }
  return $res;
}

sub getindexes {
  my $t = $_[0];
  my $r = sql_select("select i.relname from pg_class c, pg_class i,pg_index x ".
  	   "where (c.relkind = 'r' and c.oid=x.indrelid and x.indexrelid=i.oid ) ".
	   "and c.relname='$t'");
  my @res;
  for (my $i=0;$i < $r->ntuples;$i++) {
    push @res,$r->getvalue($i,0);
  }
  return @res;
  }

sub gettables { 
  my $schname;
  if (defined($_[0])) 
    { $schname = $_[0]; }
  elsif (defined($schema)) 
    {   $schname=$schema;   }
  else
    { $schname = 'public'; }
  my $r=sql_select("select c.relname"
		   ." from pg_class c"
		   ." left join pg_namespace n on n.oid = c.relnamespace"
		   ." where relkind='r' "
		   ." and n.nspname=\'$schname\'"
		  );
  my @res=();
  for (my $i=0;$i<$r->ntuples;$i++) {
    push @res,$r->getvalue($i,0);
  }
  return @res;
}

sub sql_copy {
  my $r = sql_nocheck(@_);
  my $status = $r->resultStatus;
  die "copy command failed to start ("
    .$conn->errorMessage .")" unless ($status == PGRES_COPY_IN);
  return $r;
}

sub sql_copyline {
  my $ret = $conn->putline(@_[0]."\n");
  die "sql copyline failed, status = $ret" 
    unless ($ret == 0);
  return $ret;
}

sub sql_endcopy {
  sql_copyline("\\.");
  my $ret=$conn->endcopy;
  die "copy command failed, sorry (status = $ret)"
    unless ($ret == 0);
}
