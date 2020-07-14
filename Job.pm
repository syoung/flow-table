package Table::Job;
use Moose::Role;
use Method::Signatures::Simple;

=head2

	PACKAGE		Table::Job
	
	PURPOSE
	
		job TABLE METHODS
		
=cut


=head2

	SUBROUTINE		updateJob
	
	PURPOSE

		ADD A STAGE TO THE job TABLE

	INPUTS
	
		1. STAGE IDENTIFICATION - PROJECT, WORKFLOW, STAGE AND STAGE NUMBER
		
		2. INFORMATION FOR RUNNING THE STAGE (name, location, etc.)
		
	OUTPUTS
	
		1. A NEW ENTRY IN THE job TABLE
		
	NOTES
	
		THE PARAMETERS FOR THIS STAGE ARE ADDED TO THE job TABLE

		IN A SEPARATE CALL TO THE $self->addJobParametersForJob() SUBROUTINE

=cut

method updateJob ( $hash, $set_hash ) {  
  $self->logDebug( "hash", $hash, 1 );
  $self->logDebug( "set_hash", $set_hash );
  my $set_fields;
  @$set_fields = keys %$set_hash;
  $self->logDebug( "set_fields", $set_fields );
  
  #### SET TABLE AND REQUIRED FIELDS  
  my $table = "job";
  my $required_fields = [ "username", "projectname", "workflowname" ];

  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined( $hash, $required_fields);
    $self->logCritical( "undefined values: @$not_defined" ) and return 0 if @$not_defined;
  
  my $success = $self->_updateTable( $table, $hash, $required_fields, $set_hash, $set_fields );

  return $success;
}

method getJobs {
=head2

	GET ALL ENTRIES FROM job TABLE

=cut

	#### SET OWNER 
	my $username = $self->username();
	#$self->logNote("$$ owner", $owner);

	my $query = qq{SELECT * FROM job
WHERE username='$username'};
	$self->logDebug("query", $query);
	my $jobs = $self->db()->queryhasharray($query);
	$jobs = [] if not defined $jobs;
	$self->logDebug("no. jobs", scalar(@$jobs));

	return $jobs;
}

method getJobByWorkflow ( $data ) {
	$self->logNote("$$ data", $data);

	my $required = [ "username", "projectname", "workflowname" ];
	my $where = $self->db()->where( $data, $required );
	my $query = qq{SELECT * FROM job
$where
ORDER BY projectname, workflownumber};
	$self->logDebug( "query", $query );

	my $job = $self->db()->queryhash($query);
	
	return $job;
}

#### ADD A PARAMETER TO THE job TABLE
method addJob ( $data ) {
	$self->logDebug( "data", $data );

	my $success = $self->_removeJob( $data );
	$self->logDebug( "success", $success );

	$success = $self->_addJob( $data );
	$self->logDebug( "success", $success );

	return $success;
}

method _addJob ( $data ) {
 	$self->logDebug("data", $data);

	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "job";
	my $required_fields = ["username", "projectname", "workflowname" ];
	my $fields = $self->db()->fields( $table );

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### ADD ALL fields IN data TO TABLE
 	$self->logDebug("Doing addToTable(table, data, required_fields)");
	return $self->_addToTable( $table, $data, $required_fields, $fields );	
}

method _removeJob ( $data ) {
 	$self->logDebug("data", $data);	
	
	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "job";
	my $required_fields = ["username", "projectname", "workflowname" ];

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### REMOVE FROM TABLE
 	$self->logDebug("Doing _removeFromTable(table, data, required_fields)");
	return $self->_removeFromTable($table, $data, $required_fields);
}



1;