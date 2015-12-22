#!/usr/bin/perl

use strict;

sub struct_guesser {
  my $verbose=$_[0]; # (boolean) 
  my $separator=$_[1]; # field separator (regexp)
  my $nameseparator=$_[2]; # field separator for titles row, if different
  my $file=$_[3]; # file name
  my $tfile=$_[4]; # title row file name, if different from above
  my $usetext=$_[5]; # use varchar type for string fields (recommended; otherwise uses varchar(maxlen)) 
  my $trows=$_[6]; # column names span multiple rows (concatenate)
  my $maxcount=$_[7]; # if defined, stop parsing after $maxcount lines
  my $space=$_[8]; # trim spaces
  my $encoding=$_[9]; #encoding
  
  unless(defined($usetext)) { $usetext=0; }
  unless(defined($trows)) { $trows=1; }

  print STDERR "Encoding $encoding\n";

  open I,"<:encoding($encoding)","$file" or die "can't open $file";
  my @firstline;
  if (defined($tfile)) {
    open T,"<$tfile" or die "can't open $tfile";
    for (my $i=0;$i<$trows;$i++) {
      $firstline[$i] = <T>;
    }
    close T;
  }
  else {
    for (my $i=0;$i<$trows;$i++) {
      $firstline[$i]=<I>;
    }
  }
  my @xnames;
  my @names=();
  my $csv;
  for my $headline (@firstline) {
    chomp $headline;
    $headline =~ s/\r$//;
    if ($nameseparator eq "CSV") {
      use Text::CSV_XS;
      use Text::CSV;
      $csv=Text::CSV->new({binary => 1 });
      $csv->parse($headline) or die "Failed to CSV parse <$headline>";
      @xnames=$csv->fields();
    }
    else {
      @xnames=split /$nameseparator/,$headline;
    }
    if (@names == 0)  { @names = @xnames; }
    else {
    	for(my $i=0; $i<@names and $i<@xnames; $i++) {
		$names[$i] = $names[$i].' '.$xnames[$i];
      }
    }
  }
  print STDERR "struct_guesser: found ". (1+$#names) . " names\n";
  my %namehash={};
  my $name;
  my @type;
  my @maxlen;
  my @min;
  my @max;
  my @digits; 
  my $NULL=0;
  my $INT=1;
  my $FLOAT=2;
  my $DATE=4;
  my $TIME=5;
  my $INTERVAL=6;
  my $TIMESTAMP=7;
  my $BOOLEAN=8;
  my $STRING=10;
  
  for (my $i=0;$i<=$#names;$i++) {
    $name=lc($names[$i]);
    $name =~ s/[^a-z0-9_-]//g;
    $name =~ s/-/_/g;
    $name = "field".($i+1) unless (defined($name) and length($name)>0);
    if ($name eq "month" 
      or $name eq "MONTH") {
      $type[$i]=$STRING;
    } else {
      $type[$i] = $NULL;
    }
    {
      my $tname=$name;
      for (my $j=''; defined($namehash{$tname});$j++) {
	$tname="$name$j";
      }
      $names[$i]=$tname;
      $namehash{$tname}=1;
    }
    #print STDERR join("\n",sort(keys %namehash));
    my $j=0;
    $maxlen[$i]=0;
    $min[$i]=0;
    $max[$i]=0;
    $digits[$i]=0;
  }

 
  my @typenames=("NULL","INT","FLOAT","STRING","DATE","TIME","INTERVAL","TIMESTAMP" );

  my $count=0;
  my $nfields=$#names;
  # the following date/time formats are understood by postgres' parser
  # javadate = Fri Oct 14 00:00:00 EDT 2005, where Fri and EDT and 00:00:00 are optional 
  my $javadate="([A-Za-z][A-Za-z][A-Za-z] )? *([A-Za-z][A-Za-z][A-Za-z]) +([0-9][0-9]) +([0-9][0-9]:[0-9][0-9](:[0-9][0-9])? )? *([A-Z][A-Z][A-Z] )? *([0-9][0-9][0-9][0-9])";
  my $datefield="[0-9][0-9]([0-9][0-9])?[-\/][0-9][0-9]?[-\/][0-9][0-9]?";
  my $timefield="T?[+-]?[0-9]?[0-9]:[0-9][0-9](:[0-9][0-9]([.][0-9]+)?)?";
  my $intfield = '^[ \t]*[+-]?[0-9]+[.]?[ \t]*$';
  my $floatfield = '^[ \t]*[+-]?[0-9]*[.][0-9]+([eE][+-]?[0-9]+)?[ \t]*$';
  my $booleanfield = '^(([Tt][Rr][Uu][Ee])|([Ff][Aa][Ll][Ss][Ee]))$'; # not currently used

my @fields;
    
my $csvMode = ($separator eq "CSV"); 
my $csv;

if ($csvMode) {
      $csv=Text::CSV->new({binary => 1 });
}

 loop: while () {
    $count++;
    last loop if (defined($maxcount) and $count > $maxcount) ;
    print STDERR "\r $count   " if (($count % 1000)==0);
    if ($csvMode) {
  	my $row = $csv->getline(*I);
  	unless ($row) { 
            if ($csv->eof()) { last loop; }
            else { print STDERR "Text::CSV Says \"not in CSV format\"\n",
			"Offending (Relative) line:\n",
			"<$count>\n";
      		die "Stopped";
            }
	}
        @fields=$csv->fields();
        for (my $i=0;$i<@fields;$i++) {
	    if (lc($fields[$i]) eq "null") {
	        $fields[$i] = undef;
		}
         }
    }
    else {
      last loop unless <I>;
      chomp;
      s/\r$//;
      s/(^|[$separator])[nN][uU][lL][lL]([$separator]|$)//g;
      @fields=split /$separator/;
    }
    if ($#fields ne $nfields) {
      print STDERR "struct_guesser: WARNING line $count has ".(1+$#fields)." fields\n";
      $nfields=$#fields;
    }
    for (my $i=0;$i<=$#names;$i++) {
      my $name = $names[$i];
      my $val = $fields[$i];
      my $t = $type[$i];
      my $len = length($val);
      $maxlen[$i] = $len if ($len > $maxlen[$i]);
      my $curtyp=$type[$i];
      my $typ=undef;
      unless ($curtyp == $STRING) {
	if ($val eq "") { $typ = $NULL; }
	elsif ($curtyp <= $FLOAT) {
	  # numeric compat test
	  if ($val eq "") 
	    { $typ = $NULL; }
	  elsif ($val =~ /$intfield/ ) 
	    { $typ = $INT; }
	  elsif ($val =~ /$floatfield/) 
	    { $typ = $FLOAT ; }
	}
	if (not defined($typ) and ($curtyp >= $DATE or $curtyp <= $NULL)) { 
		if ($space) {
 	  	 	$val =~	s/^ +//g;
 	  	 	$val =~ s/ +$//g;
		  }
	  if
	    ($val =~ /^${javadate}$/)
	      {$typ=$TIMESTAMP;}
	  elsif
	    ($val =~ /^${datefield}$/) 
	      {$typ=$DATE;}
	  elsif
	    ($val =~ /^${timefield}$/) 
	      { # Time fields have hour btw 0 and 23; 
		$val =~ /^(.*):/;
		my $hours = $1;
		if ($val =~ /-/ or $hours<0 or $hours>23) {$typ = $INTERVAL; }
		else {$typ=$TIME;}
	      }
	  elsif
	    ($val =~ /^${datefield}[ \t]*${timefield}$/ or
	     $val =~ /^${timefield}[ \t]*${datefield}$/) 
	      {$typ=$TIMESTAMP;}
	}
	if (not defined($typ)) { $typ = $STRING; }
	
	print "<$val> $typ $t\n" if ($verbose > 1);
	if ($typ > $t) {
	  $type[$i] = $typ;
	  if ($typ == $STRING and $verbose > 0) {
	    print STDERR "downgraded $name to STRING on account of <$val>\n";
	  }
	}
      }
      if ($t <= $FLOAT) {
	$max[$i] = $val if ($val > $max[$i]);
	$min[$i] = $val if ($val < $min[$i]);
      }
    }
  }
  close I;
  print STDERR "struct_guesser: parsed $count lines.\n";
  if ($verbose == 1) {
    for (my $i=0;$i<=$#names;$i++) {
      my $t = $type[$i];
      print "$names[$i]: $typenames[$t]($maxlen[$i]) ";
      if ($t > $NULL and $t <= $FLOAT) {
	print "MIN $min[$i] MAX $max[$i]";
      } elsif ($t == $STRING) { 
	print "MAXLEN $maxlen[$i]";
      }
      print "\n";
    }
  }

  my $sql_struct="";

  for (my $i=0;$i<=$#names;$i++) {
    #print "$names[$i]\t";
    my $t = $type[$i];
    my $name=$names[$i];
    my $ftype;
    my $min;
    my $max;
    if ($t == $NULL) {
    	# TODO: Should have a user-definable type for empty fields
      $ftype = "text";
    	print STDERR "Warning: field \"$name\" empty, defaulting to $ftype\n";
    } elsif ($t == $INT) {
      if (0) {
	# value-based guessing
	$min=$min[$i];
	$max=$max[$i];
	if (-($min) > $max) {
	  $max = -($min);
	}
	if ($max < 32768) {
	  $ftype= "smallint";
	} elsif ($max < 2147483648) {
	  $ftype = "integer";
	} elsif ($max < 9223372036854775806) {
	  $ftype= "bigint";
	} else {
	  $ftype= "varchar";
	}
      } else {
	# length-based guessing
	if ($maxlen[$i]<= 4) {
	  $ftype = "smallint";
	} elsif ($maxlen[$i] <= 8) {
	  $ftype = "integer";
	} elsif ($maxlen[$i] <= 18) {
	  $ftype="bigint";
	} else {
	  $ftype="varchar";
	  }
      }
    } elsif ($t == $FLOAT) {
      if ($maxlen[$i] <= 6) {
	$ftype = "real";
      } else {
	$ftype = "double precision";
      }
    } elsif ($t == $TIME) {$ftype = "time"; }
    elsif ($t == $INTERVAL) {$ftype = "interval"; }
    elsif ($t == $DATE) {$ftype = "date"; }
    elsif ($t == $TIMESTAMP) { $ftype = "timestamp"; }
    else { 
      if ($name eq "month" or $name eq "MONTH") {
	$ftype = "char($maxlen[$i])"
      } else {
	if ($usetext) {
	  $ftype = "varchar";
	}
	else {
	  $ftype = "varchar($maxlen[$i])";
	}
      }
    }
    $sql_struct .= "(" if ($i == 0);
    $sql_struct .= "$names[$i] $ftype";
    if ($i<$#names) {
      $sql_struct .= ",";
    } else {
      $sql_struct .= ")";
    }
  }
  return $sql_struct;
}


1;
