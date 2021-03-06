use MooseX::Declare;

use strict;
use warnings;

#### USE LIB FOR INHERITANCE
use FindBin qw($Bin);
use lib "$Bin/../";

class Table::DBase::Factory {

sub new {
    my $class          = shift;
    my $requested_type = shift;
    
    my $location    = "Table/DBase/$requested_type.pm";
    $class          = "Table::DBase::$requested_type";
    require $location;

    # print "Table::DBase::Factory    RETURNING class->new( @_ )\n";
    return $class->new(@_);
}
    
Table::DBase::Factory->meta->make_immutable(inline_constructor => 0);


} #### class


1;
