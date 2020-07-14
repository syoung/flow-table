package Table::Instance;
use Moose::Role;
use Method::Signatures::Simple;

=head2

	PACKAGE		Table::Instance
	
	PURPOSE
	
		instance TABLE METHODS
		
=cut

method addInstance ( $data ) {
=head2

	GET ALL ENTRIES FROM instance TABLE

=cut
	my $success = $self->_removeInstance( $data);
	$self->logDebug( "REMOVE success", $success );

	$success = $self->_addInstance( $data );
	$self->logDebug( "ADD success", $success );

	if ( not defined $success ) {
		$self->logCritical( "FAILED TO ADD TO instance TABLE", $data );
		return 0;
	}

	return 1;
}


method getInstance ( $data ) {
=head2

	GET ALL ENTRIES FROM instance TABLE

=cut

	my $query = qq{SELECT * FROM instance
WHERE username='$data->{ username }'
AND projectname='$data->{ projectname }'
AND workflowname='$data->{ workflowname }'
};
	$self->logDebug("query", $query);
	my $instance = $self->db()->queryhasharray($query);
	$self->logDebug( "instance", $instance );

	return $instance;
}

method getInstanceByWorkflow ( $data ) {
	$self->logNote("$$ data", $data);

	my $required = [ "username", "projectname", "workflowname" ];
	my $where = $self->db()->where( $data, $required );
	my $query = qq{SELECT * FROM instance
$where
ORDER BY projectname, workflownumber};
	$self->logNote("$$ $query");

	my $instances = $self->db()->queryhasharray($query);
	$instances = [] if not defined $instances;
	#$self->logDebug("$$ instances", $instances);
	$self->logDebug("Total instances", scalar(@$instances));
	
	return $instances;
}

#### ADD A PARAMETER TO THE instance TABLE
method _addInstance ( $data ) {
 	$self->logDebug("data", $data);

	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "instance";
	my $required_fields = ["username", "projectname", "workflowname" ];
	my $fields = $self->db()->fields( $table );

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### ADD ALL fields IN data TO TABLE
 	$self->logDebug("Doing addToTable(table, data, required_fields)");
	return $self->_addToTable( $table, $data, $required_fields, $fields );	
}

method _removeInstance ( $data ) {
 	$self->logDebug("data", $data);	
	
	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "instance";
	my $required_fields = ["username", "projectname", "workflowname" ];

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### REMOVE FROM TABLE
 	$self->logDebug("Doing _removeFromTable(table, data, required_fields)");
	return $self->_removeFromTable($table, $data, $required_fields);
}



1;