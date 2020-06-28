package Table::InstanceStatus;
use Moose::Role;
use Method::Signatures::Simple;

=head2

	PACKAGE		Table::InstanceStatus
	
	PURPOSE
	
		instancestatus TABLE METHODS
		
=cut

method addInstanceStatus ( $data ) {
=head2

	GET ALL ENTRIES FROM instancestatus TABLE

=cut
	my $success = $self->_removeInstanceStatus( $data);
	$self->logDebug( "REMOVE success", $success );

	$success = $self->_addInstanceStatus( $data );
	$self->logDebug( "ADD success", $success );

	if ( not defined $success ) {
		$self->logCritical( "FAILED TO ADD TO instancestatus TABLE", $data );
		return 0;
	}

	return 1;
}


method getInstanceStatus ( $data ) {
=head2

	GET ALL ENTRIES FROM instancestatus TABLE

=cut

	my $query = qq{SELECT * FROM instancestatus
WHERE username='$data->{ username }'
AND projectname='$data->{ projectname }'
AND workflowname='$data->{ workflowname }'
};
	$self->logDebug("query", $query);
	my $instancestatus = $self->db()->queryhasharray($query);
	$self->logDebug( "instancestatus", $instancestatus );

	return $instancestatus;
}

method getInstanceStatusByWorkflow ( $data ) {
	$self->logNote("$$ data", $data);

	my $required = [ "username", "projectname", "workflowname" ];
	my $where = $self->db()->where( $data, $required );
	my $query = qq{SELECT * FROM instancestatus
$where
ORDER BY projectname, workflownumber};
	$self->logNote("$$ $query");

	my $instancestatuss = $self->db()->queryhasharray($query);
	$instancestatuss = [] if not defined $instancestatuss;
	#$self->logDebug("$$ instancestatuss", $instancestatuss);
	$self->logDebug("Total instancestatuss", scalar(@$instancestatuss));
	
	return $instancestatuss;
}

#### ADD A PARAMETER TO THE instancestatus TABLE
method _addInstanceStatus ( $data ) {
 	$self->logDebug("data", $data);

	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "instancestatus";
	my $required_fields = ["username", "projectname", "workflowname" ];
	my $fields = $self->db()->fields( $table );

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### ADD ALL fields IN data TO TABLE
 	$self->logDebug("Doing addToTable(table, data, required_fields)");
	return $self->_addToTable( $table, $data, $required_fields, $fields );	
}

method _removeInstanceStatus ( $data ) {
 	$self->logDebug("data", $data);	
	
	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "instancestatus";
	my $required_fields = ["username", "projectname", "workflowname" ];

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### REMOVE FROM TABLE
 	$self->logDebug("Doing _removeFromTable(table, data, required_fields)");
	return $self->_removeFromTable($table, $data, $required_fields);
}



1;