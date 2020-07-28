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

method updateJob ( $data, $set_hash ) {  
  # $self->logDebug( "data", $data, 1 );
  $self->logDebug( "set_hash", $set_hash );
  my $set_fields;
  @$set_fields = keys %$set_hash;
  $self->logDebug( "set_fields", $set_fields );
  
  #### SET TABLE AND REQUIRED FIELDS  
  my $table = "job";
  my $required_fields = [ "username", "projectname", "workflowname" ];

  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined( $data, $required_fields);
    $self->logCritical( "undefined values: @$not_defined" ) and return 0 if @$not_defined;
  
  my $success = $self->_updateTable( $table, $data, $required_fields, $set_hash, $set_fields );

  return $success;
}

method hasJob ( $data ) {

#### SET TABLE AND REQUIRED FIELDS  
  my $table = "job";
  my $required_fields = [ "username", "projectname", "workflowname" ];

  #### CHECK REQUIRED FIELDS ARE DEFINED
  my $not_defined = $self->db()->notDefined( $data, $required_fields);
    $self->logCritical( "undefined values: @$not_defined" ) and return 0 if @$not_defined;
  
  my $query = "SELECT 1 FROM job 
WHERE username='$data->{ username }'
AND projectname='$data->{ projectname }'
AND workflowname='$data->{ workflowname }'";
	$self->logDebug( "query", $query );
	my $result = $self->db()->query( $query );

	$result = 0 if not defined $result;

	return $result;
}


method getJobs {
=head2

	GET ALL ENTRIES FROM job TABLE

=cut

	my $query = qq{SELECT * FROM job};
	$self->logDebug("query", $query);
	my $jobs = $self->db()->queryhasharray($query);
	$jobs = [] if not defined $jobs;
	$self->logDebug("no. jobs", scalar(@$jobs));

	return $jobs;
}

method getJobsByUser ( $username ) {
=head2

  GET ALL ENTRIES FROM job TABLE

=cut

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
	$self->logNote( "data", $data );

	my $success = $self->_removeJob( $data );
	$self->logDebug( "_removeJob success", $success );

	$success = $self->_addJob( $data );
	$self->logDebug( "_addJob success", $success );

	return $success;
}

method _addJob ( $data ) {
 	# $self->logDebug("data", $data);

	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "job";
	my $required_fields = ["username", "projectname", "workflowname" ];
	my $fields = $self->db()->fields( $table );

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### ADD ALL fields IN data TO TABLE
	return $self->_addToTable( $table, $data, $required_fields, $fields );	
}

method _removeJob ( $data ) {
 	# $self->logDebug("data", $data);	
	
	#### SET TABLE AND REQUIRED FIELDS	
	my $table = "job";
	my $required_fields = ["username", "projectname", "workflowname" ];

	#### CHECK REQUIRED FIELDS ARE DEFINED
	my $not_defined = $self->db()->notDefined($data, $required_fields);
  $self->logError("undefined values: @$not_defined") and return if @$not_defined;

	#### REMOVE FROM TABLE
	return $self->_removeFromTable($table, $data, $required_fields);
}



1;