1. MOTIVATION

Data ...

TYPICALLY comes in flat files
OFTEN some variant of CSV
USUALLY text files with separators (comma, tab)
SOMETIMES column names are included
RARELY we get a database dump
NEVER an XML format
MORE OFTEN THAN NOT we need to guess the column types and create the database ourselves

2. CREATE_FROMTXT.PL

Reads a flat file
Understands multiple file types
WITH SEPARATOR ONLY
Guesses database structure !!
Reads in column names (from csv file or external)
Saves to database table in Postgres format


3. USAGE

create_fromtxt.pl -csv -db DATABASE -host host.name.com FILENAME.EXT [TABLENAME]



4. MORE USAGE

create_fromtxt.pl -h

5. MORE USAGE

vim create_fromtxt.pl

6. INSTALLATION

a. Install Postgres
b. $ apt-get install libpg-perl OR  $ cpan Pg 
c. /path/to/sql-importer/src/main/create_fromtxt.pl [...]

d. Read INSTALL.txt for more details.

