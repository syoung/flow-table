# package Table::Main;
# use Moose::Role;
# use Moose::Util::TypeConstraints;

# with 'Table::App';
# with 'Table::Common';
# with 'Table::Parameter';
# with 'Table::Project';
# with 'Table::Sample';
# with 'Table::Stage';
# with 'Table::Workflow';


# 1;

use MooseX::Declare;

=head2

  PACKAGE    Table::Main
  
  PURPOSE
  
    DATABASE MANIPULATION METHODS FOR WORKFLOW OBJECTS

=cut

class Table::Main with (Util::Logger,
  Table::App,
  Table::Parameter,
  Table::Package,
  Table::Project,
  Table::Sample,
  Table::Stage,
  Table::Workflow) {

#### EXTERNAL
use Data::Dumper;
use Term::ReadKey;

#### INTERNAL
use FindBin qw($Bin);
use lib "$Bin/../../";
use Table::DBase::Factory;


has 'database'    => ( isa => 'Str|Undef', is => 'rw' );
has 'db'      =>  ( 
  is      => 'rw', 
  isa     => 'Any', 
  # lazy    =>  1,  
  # builder =>  "setDbh" 
);

has 'conf'      => ( 
  is => 'rw', 
  isa => 'Conf::Yaml', 
  lazy => 1, 
  builder => "setConf" 
);

has 'util'    =>  (
  is      =>  'rw',
  isa     =>  'Util::Main',
  lazy    =>  1,
  builder =>  "setUtil"
);

method setUtil () {
  my $util = Util::Main->new({
    conf      =>  $self->conf(),
    log       =>  $self->log(),
    printlog  =>  $self->printlog()
  });

  $self->util($util); 
}

method BUILD ($args) {
  $self->logCaller("");
  $self->logDebug("self->log()", $self->log());

  $self->initialise($args);
}

method initialise ($args) {
  $self->logNote("args", $args);
  $self->setDbh($args);
}

method setDbh ( $args ) {
  $self->logCaller("");
  # $self->logNote("args", $args);  
  
  my $database    = $args->{database} || $self->database();
  my $dbuser      = $args->{dbuser};
  my $dbpassword  = $args->{dbpassword};
  my $dbhost      = $args->{dbhost};
  my $dbtype      = $args->{dbtype};
  my $dbfile      = $args->{dbfile};

  my $logfile     = $args->{logfile} || $self->logfile();
  my $log         = $args->{log} || $self->log();
  my $printlog    = $args->{printlog} || $self->printlog();

  $self->logNote("logfile", $logfile);
  $self->logNote("ARGS database", $database);
  $self->logNote("ARGS dbtype", $dbtype);
  $self->logNote("ARGS dbuser", $dbuser);

  #### DEBUG
  $self->logDebug( "dbfile",  $dbfile);
  $dbfile   = $self->conf()->getKey("core:INSTALLDIR") . "/" . $self->conf()->getKey("database:DBFILE") if not defined $dbfile;
  $self->logDebug("dbfile", $dbfile);

  $dbtype   = $self->conf()->getKey("database:DBTYPE") if not defined $dbtype;
  $self->logNote("dbtype", $dbtype);
  $dbuser     = $self->conf()->getKey("database:USER") if not defined $dbuser;
  $dbpassword   = $self->conf()->getKey("database:PASSWORD") if not defined $dbpassword;
  $dbhost     = $self->conf()->getKey("database:HOST") if not defined $dbhost;
  $database = $self->conf()->getKey("database:DATABASE") if not defined $database or $database eq "";
  $self->logNote("CONF database", $database);
  $self->logNote("CONF dbtype", $dbtype);
  $self->logNote("CONF dbuser", $dbuser);
  
  if ( $self->can('isTestUser') and $self->isTestUser() ) {
    $dbuser     = $self->conf()->getKey("database:TESTUSER") if not defined $dbuser;
    $dbpassword   = $self->conf()->getKey("database:TESTPASSWORD") if not defined $dbpassword;
    $database = $self->conf()->getKey("database:TESTDATABASE") if not defined $database;
  }
  
  $self->logNote("AFTER database", $database);
  $self->logNote("AFTER dbtype", $dbtype);
  $self->logNote("AFTER dbuser", $dbuser);
  $self->logNote("AFTER dbpassword", $dbpassword);
  
  $self->logError("dbtype not defined") and return if not $dbtype;
  if ( not $database and $dbtype ne "SQLite" ) {
      $self->logError("database not defined");
      print "Main::Table::setDbh    database not defined and dbytype is not 'SQLite'. Exiting.\n";
      exit;
  }

  #### SET DATABASE IF PROVIDED IN JSON
  if ( $self->can('json') ) {
    my $json = $self->json();
    $database = $json->{database} if defined $json and defined $json->{database} and $json->{database};
  }

  $self->logNote("FINAL database", $database);
  $self->logNote("FINAL dbtype", $dbtype);
  $self->logNote("FINAL dbuser", $dbuser);
  $self->logNote("FINAL dbpassword", $dbpassword);

  ##### CREATE DB OBJECT USING DBASE FACTORY
  my $db =  DBase::Factory->new(
    $dbtype,
    {
      dbfile      =>  $dbfile,
      database    =>  $database,
      dbuser      =>  $dbuser,
      dbpassword  =>  $dbpassword,
      dbhost      =>  $dbhost,
      logfile     =>  $logfile,
      log         =>  $log,
      printlog    =>  $printlog,
      parent      =>  $self
    }
  ) or print qq{ error: 'Agua::Database::setDbh    Cannot create database object $database: $!' } and return;
  $self->logDebug( "db", $db );
  $self->logError("db not defined") and return if not defined $db;

  $self->db($db); 

  return $db;
}

method grantPrivileges ( $tempfile, $rootdbpassword, $database, $dbuser, $dbpassword, $privileges, $host) {
  $self->logError("tempfile not defined") and return if not defined $tempfile;
  $self->logError("rootdbpassword not defined") and return if not defined $rootdbpassword;
  $self->logError("database not defined") and return if not defined $database;
  $self->logError("dbuser not defined") and return if not defined $dbuser;
  $self->logError("dbpassword not defined") and return if not defined $dbpassword;
  $self->logError("privileges not defined") and return if not defined $privileges;
  $self->logError("host not defined") and return if not defined $host;

  #### CREATE DATABASE AND Agua USER AND PASSWORD
    $self->logNote("tempfile", $tempfile);
  my $create = qq{
USE mysql;
GRANT ALL PRIVILEGES ON $database.* TO $dbuser\@localhost IDENTIFIED BY '$dbpassword';  
FLUSH PRIVILEGES;};
  `echo "$create" > $tempfile`;
  my $command = "mysql -u root -p$rootdbpassword < $tempfile";
  $self->logNote("$command");
  print `$command`;
  `rm -fr $tempfile`;
}

method inputRootPassword {
    #### MASK TYPING FOR PASSWORD INPUT
    ReadMode 2;
  my $rootdbpassword = $self->inputValue("Root dbpassword (will not appear on screen)");

    #### UNMASK TYPING
    ReadMode 0;

  $self->rootdbpassword($rootdbpassword);

  return $rootdbpassword;
}

method inputValue ( $message, $default ) {
  $self->logError("message is not defined") and return if not defined $message;
  $default = '' if not defined $default;
  $self->logDebug("$message [$default]: ");
  print "$message [$default]: ";

  my $input = '';
    while ( $input =~ /^\s*$/ )
    {
        $input = <STDIN>;
        $input =~ s/\s+//g;
    $default = $input if $input;
    print "\n" and return $default if $default;
        $self->logDebug("$message [$default]: ");
    print "$message [$default]: ";
    }
}




method _updateTable ( $table, $hash, $required_fields, $set_hash, $set_fields ) {
=head2

  SUBROUTINE    _updateTable
  
  PURPOSE

    UPDATE ONE OR MORE ENTRIES IN A TABLE
        
  INPUTS
    
    1. NAME OF TABLE      

    2. HASH CONTAINING OBJECT TO BE UPDATED

    3. HASH CONTAINING TABLE FIELD KEY-VALUE PAIRS
        
=cut
  $self->logNote("Common::_updateTable(table, hash, required_fields, set_fields)");
    $self->logError("hash not defined") and return if not defined $hash;
    $self->logError("required_fields not defined") and return if not defined $required_fields;
    $self->logError("set_hash not defined") and return if not defined $set_hash;
    $self->logError("set_fields not defined") and return if not defined $set_fields;
    $self->logError("table not defined") and return if not defined $table;

  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined($hash, $required_fields);
    $self->logError("undefined values: @$not_defined") and return if @$not_defined;

  #### GET WHERE
  my $where = $self->db()->where($hash, $required_fields);

  #### GET SET
  my $set = $self->db()->set($set_hash, $set_fields);
  $self->logError("set values not defined") and return if not defined $set;

  ##### UPDATE TABLE
  my $query = qq{UPDATE $table $set $where};           
  $self->logNote("$query");
  my $result = $self->db()->do($query);
  $self->logNote("result", $result);
}

method _addToTable ( $table, $hash, $required_fields, $inserted_fields ) {
  
=head2

  SUBROUTINE    _addToTable
  
  PURPOSE

    ADD AN ENTRY TO A TABLE
        
  INPUTS
    
    1. NAME OF TABLE      

    2. ARRAY OF KEY FIELDS THAT MUST BE DEFINED 

    3. HASH CONTAINING TABLE FIELD KEY-VALUE PAIRS
        
=cut
  #### CHECK FOR ERRORS
    $self->logError("hash not defined for table: $table") and return if not defined $hash;
    $self->logError("required_fields not defined for table: $table") and return if not defined $required_fields;
    $self->logError("table not defined") and return if not defined $table;
  
  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined($hash, $required_fields);
    $self->logError("table '$table' undefined values: @$not_defined") and return if @$not_defined;

  #### GET ALL FIELDS BY DEFAULT IF INSERTED FIELDS NOT DEFINED
  $inserted_fields = $self->db()->fields($table) if not defined $inserted_fields;

  $self->logError("table '$table' fields not defined") and return if not defined $inserted_fields;
  my $fields_csv = join ",", @$inserted_fields;
  
  ##### INSERT INTO TABLE
  my $values_csv = $self->db()->fieldsToCsv($inserted_fields, $hash);
  my $query = qq{INSERT INTO $table ($fields_csv)
VALUES ($values_csv)};           
  $self->logNote("$query");
  my $result = $self->db()->do($query);
  $self->logNote("result", $result);
  
  return $result;
}

method _removeFromTable ( $table, $hash, $required_fields ) {
=head2

  SUBROUTINE    _removeFromTable
  
  PURPOSE

    REMOVE AN ENTRY FROM A TABLE
        
  INPUTS
    
    1. HASH CONTAINING TABLE FIELD KEY-VALUE PAIRS
    
    2. ARRAY OF KEY FIELDS THAT MUST BE DEFINED 

    3. NAME OF TABLE      
=cut
  
    #### CHECK INPUTS
    $self->logError("hash not defined") and return if not defined $hash;
    $self->logError("required_fields not defined") and return if not defined $required_fields;
    $self->logError("table not defined") and return if not defined $table;

  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined($hash, $required_fields);
    $self->logError("undefined values: @$not_defined") and return if @$not_defined;

  #### DO DELETE 
  my $where = $self->db()->where($hash, $required_fields);
  my $query = qq{DELETE FROM $table
$where};
  #$self->logDebug("\n$query");
  my $result = $self->db()->do($query);
  #$self->logDebug("result", $result);
  
  return 1 if defined $result;
  return 0;
}


method arrayToArrayhash ( $array, $key ) {
=head2

    SUBROUTINE:     arrayToArrayhash
    
    PURPOSE:

        CONVERT AN ARRAY INTO AN ARRAYHASH, E.G.:
    
    {
      key1 : [ entry1, entry2 ],
      key2 : [ ... ]
      ...
    }

=cut
  #$self->logNote("Common::arrayToArrayhash(array, key)");
  #$self->logNote("array: @$array");
  #$self->logNote("key", $key);

  my $arrayhash = {};
  for my $entry ( @$array )
  {
    if ( not defined $entry->{$key} )
    {
      $self->logNote("entry->{$key} not defined in entry. Returning.");
      return;
    }
    $arrayhash->{$entry->{$key}} = [] if not exists $arrayhash->{$entry->{$key}};
    push @{$arrayhash->{$entry->{$key}}}, $entry;   
  }
  
  #$self->logNote("returning arrayhash", $arrayhash);
  return $arrayhash;
}


}

