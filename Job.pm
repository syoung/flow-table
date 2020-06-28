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

method updateJobs ( $data ) {  
  $self->logDebug( "data", $data, 1 );

  my $query = qq{UPDATE job
SET
processid = '$data->{ processid }',
first='$data->{ first }',
last='$data->{ last }',
current='$data->{ appnumber }'
WHERE username = '$data->{ username }'
AND projectname = '$data->{ projectname }'
AND workflowname = '$data->{ workflowname }'};

  $self->logNote("$query");
  my $success = $self->db()->do($query);
  if ( not $success ) {
    $self->logError("Can't update project '$data->{ projectname }' workflow '$data->{ workflowname }' with processid: $data->{ processid }");
    
    return 0;
  }
 
 	return 1;
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

# method getJobByWorkflow ( $data ) {
# =head2

# 	GET job TABLE ENTRY FOR WORKFLOW

# =cut

# 	my $query = qq{SELECT * FROM job
# WHERE username='$data->{ username }'
# AND projectname='$data->{ projectname }'
# AND workflowname='$data->{ workflowname}'};
# 	$self->logDebug("query", $query);

# 	my $job = $self->db()->queryhash( $query );
# 	$self->logDebug( "job", $job );

# 	return $job;
# }


method getJobByWorkflow ( $data ) {
	$self->logNote("$$ data", $data);

	my $required = [ "username", "projectname", "workflowname" ];
	my $where = $self->db()->where( $data, $required );
	my $query = qq{SELECT * FROM job
$where
ORDER BY projectname, workflownumber};
	$self->logNote("$$ $query");

	my $jobs = $self->db()->queryhasharray($query);
	$jobs = [] if not defined $jobs;
	#$self->logDebug("$$ jobs", $jobs);
	$self->logDebug("Total jobs", scalar(@$jobs));
	
	return $jobs;
}

#### ADD A PARAMETER TO THE job TABLE
method addJob ( $data ) {

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