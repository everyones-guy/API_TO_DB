package MyDatabase;

use strict;
use warnings;
use DBI;
use Log::Log4perl;
use JSON;
use Text::CSV;
use Excel::Writer::XLSX;
use JSON::Validator;
use Data::Random qw(:all);

sub new {
    my ($class, $config) = @_;
    my $self = {
        dbh => undef,
        logger => Log::Log4perl->get_logger(),
        db_type => undef,
        db_name => undef,
        db_user => undef,
        db_pass => undef,
        db_host => undef,
        db_port => undef,
    };
    bless($self, $class);

    if ($config && ref($config) eq 'HASH') {
        $self->set_config($config);
    }

    return $self;
}

sub set_config {
    my ($self, $config) = @_;

    if (exists $config->{log_config}) {
        Log::Log4perl->init($config->{log_config});
        $self->{logger} = Log::Log4perl->get_logger();
    }

    if (exists $config->{db_type}) {
        $self->{db_type} = $config->{db_type};
    }

    if (exists $config->{db_name}) {
        $self->{db_name} = $config->{db_name};
    }

    if (exists $config->{db_user}) {
        $self->{db_user} = $config->{db_user};
    }

    if (exists $config->{db_pass}) {
        $self->{db_pass} = $config->{db_pass};
    }

    if (exists $config->{db_host}) {
        $self->{db_host} = $config->{db_host};
    }

    if (exists $config->{db_port}) {
        $self->{db_port} = $config->{db_port};
    }
}

# Connect to the database
sub connect {
    my ($self, $config) = @_;

    my $dsn;
    my $logger = $self->{logger};

    my $db_type = $config->{db_type};
    my $db_name = $config->{db_name};
    my $db_user = $config->{db_user};
    my $db_pass = $config->{db_pass};
    my $db_host = $config->{db_host};
    my $db_port = $config->{db_port};

    # Basic input validation
    unless ($db_type && $db_name && $db_user && $db_host && $db_port) {
        $logger->error("Missing required database connection parameters.");
        return;
    }

    if ($db_type !~ /^(mysql|postgresql|sqlite)$/) {
        $logger->error("Unsupported database type: $db_type");
        return;
    }

    # Construct DSN
	# ***Note, if encryption(SSL/TLS) is being used, it will need to be added to the connection string
    if ($db_type eq 'mysql') {
        $dsn = "DBI:mysql:database=$db_name;host=$db_host;port=$db_port";
    }
    elsif ($db_type eq 'postgresql') {
        $dsn = "DBI:Pg:dbname=$db_name;host=$db_host;port=$db_port";
    }
    elsif ($db_type eq 'sqlite') {
        $dsn = "DBI:SQLite:dbname=$db_name";
    }

    $self->{dbh} = DBI->connect($dsn, $db_user, $db_pass, {
        PrintError       => 0,
        RaiseError       => 1,
        AutoCommit       => 1,
        FetchHashKeyName => "NAME_lc",
    });

    unless ($self->{dbh}) {
        $logger->error("Database connection error: " . DBI->errstr);
    }

    return $self->{dbh};
}


sub disconnect {
    my ($self) = @_;

	my $logger = $self->{logger};
	$self->{dbh}->disconnect if $self->{dbh};
	$logger->info("Database connection closed");
	
	return;
}

sub execute_sql {
    my ($self, $sql, @params) = @_;

	my $sth = $self->{dbh}->prepare($sql);
	unless ($sth) {
		my $logger = $self->{logger};
		$logger->error("SQL prepare error: " . $self->{dbh}->errstr);
	}

	unless ($sth->execute(@params)) {
		my $logger = $self->{logger};
		$logger->error("SQL execute error: " . $sth->errstr);
	}

	return $sth;
}

sub transform_data {
    my ($self, $data) = @_;

	# Implement your data transformation logic here
	# For example, you can clean, validate, or modify data before insertion
	return $data;  # Return the transformed data
}

sub export_data {
    my ($self, $destination, $data_structure, $format) = @_;

    if ($destination eq 'file') {
        $self->export_data_to_file($data_structure, $format);
    }
    elsif ($destination eq 'another_database') {
        $self->export_data_to_generic_database($data_structure);
    }
    elsif ($destination eq 'remote_service') {
        $self->export_data_to_remote_service($data_structure);
    }
    else {
        my $logger = $self->{logger};
        $logger->error("Unsupported destination: $destination");
    }
	
	return;
}

sub export_data_to_file {
    my ($self, $data_structure, $format) = @_;

    # Check the format and call the appropriate export function
    if ($format eq 'csv') {
        $self->export_data_to_csv($data_structure);
    }
    elsif ($format eq 'json') {
        $self->export_data_to_json($data_structure);
    }
    elsif ($format eq 'db') {
        $self->export_data_to_generic_database($data_structure);
    }
    elsif ($format eq 'xlsx') {
        $self->export_data_to_excel($data_structure);
    }
	
	return;
}

sub export_data_to_csv {
    my ($self, $data_structure) = @_;

    my $filename = $data_structure->{filename};
    my $data = $data_structure->{data};

    my $csv = Text::CSV->new({ binary => 1, auto_diag => 1, eol => "\n" });

    open my $fh, '>:encoding(utf8)', $filename or die "Could not open '$filename': $!";

    # Write the header
    $csv->print($fh, [keys %{$data->[0] || {}}]);

    # Write the data
    for my $row_data (@$data) {
        $csv->print($fh, [map { $row_data->{$_} } keys %{$data->[0] || {}}]);
    }

    close $fh;
	return;
}

sub export_data_to_json {
    my ($self, $data_structure) = @_;

    my $filename = $data_structure->{filename};
    my $data = $data_structure->{data};

    open my $fh, '>:encoding(utf8)', $filename or die "Could not open '$filename': $!";
    print $fh encode_json($data);
    close $fh;
}

sub export_data_to_generic_database {
    my ($self, $data_structure) = @_;

    unless ($data_structure) {
        $self->{logger}->error("No data structure provided for export_data_to_generic_database");
        return;
    }

    my $database_name = $data_structure->{database_name};
    my $table_name = $data_structure->{table_name};
    my $data = $data_structure->{data};

    unless ($database_name && $table_name) {
        $self->{logger}->error("Database name and table name must be provided for export_data_to_generic_database");
        return;
    }

    # Create the database if it doesn't exist
    my $create_database_sql = "CREATE DATABASE IF NOT EXISTS $database_name";
    $self->execute_sql($create_database_sql);

    # Connect to the new database
    my $dbh = $self->connect({
        db_type => $self->{db_type},
        db_name => $database_name,
        db_user => $self->{db_user},
        db_pass => $self->{db_pass},
        db_host => $self->{db_host},
        db_port => $self->{db_port}
    });

    unless ($dbh) {
        $self->{logger}->error("Failed to connect to the newly created database: $database_name");
        return;
    }

    # Create the table based on the record structure
    my $create_table_sql = "CREATE TABLE IF NOT EXISTS $table_name (";
    my @fields = keys %{$data->[0]};  # Assuming the first row contains the column names

    foreach my $field (@fields) {
        my $field_type = 'TEXT';  # You can adjust the data type as needed
        $create_table_sql .= "$field $field_type, ";
    }

    $create_table_sql =~ s/, $//;  # Remove the trailing comma and space
    $create_table_sql .= ")";
    $self->execute_sql($create_table_sql);

    # Insert data into the table
    my $insert_sql = "INSERT INTO $table_name (";
    $insert_sql .= join(', ', @fields);
    $insert_sql .= ") VALUES (";
    $insert_sql .= join(', ', ('?') x scalar(@fields));
    $insert_sql .= ")";

    foreach my $row (@$data) {
        my @values = map { $row->{$_} } @fields;
        $self->execute_sql($insert_sql, @values);
    }

    $self->disconnect;  # Disconnect from the temporary database
	
	return;
}

sub export_data_to_excel {
    my ($self, $data_structure) = @_;

    my $filename = $data_structure->{filename};
    my $data = $data_structure->{data};
    my $workbook = Excel::Writer::XLSX->new($filename);
    my $worksheet = $workbook->add_worksheet();

    my $header_format = $workbook->add_format();
    $header_format->set_bold();
    $header_format->set_align('center');
    $header_format->set_bg_color('yellow');

    my @header = keys %{$data->[0] || {}};
    my $row = 0;
    my $col = 0;

    for my $header (@header) {
        $worksheet->write($row, $col, $header, $header_format);
        $col++;
    }

    $row++;
    $col = 0;

    for my $row_data (@$data) {
        for my $header (@header) {
            $worksheet->write($row, $col, $row_data->{$header});
            $col++;
        }
        $row++;
        $col = 0;
    }

    $workbook->close();
}

sub export_data_to_remote_service {
    my ($self, $data_structure) = @_;

    my $remote_service_url = 'https://example.com/api';  # Replace with the actual URL of the remote service
    my $data = $data_structure->{data};  # Data to be sent to the remote service

    my $ua = LWP::UserAgent->new;
    my $json_data = encode_json($data);

    my $response = $ua->post(
        $remote_service_url,
        Content_Type => 'application/json',
        Content      => $json_data
    );

    if ($response->is_success) {
        $self->{logger}->info("Data successfully sent to the remote service.");
    } else {
        $self->{logger}->error("Failed to send data to the remote service: " . $response->status_line);
    }
}


sub simulate_api_call {
    my ($self, $url, $api_schema) = @_;

    my $ua = LWP::UserAgent->new;
    my $response = $ua->get($url);

    unless ($response->is_success) {
        $self->{logger}->error("API call failed: " . $response->status_line);
        return;
    }

    return $response->content;
}


# Generate test data based on an API schema
sub generate_test_data {
    my ($self, $api_schema) = @_;

    # Define the structure of the data you want to generate
    my $data_structure = {
        coupons => {
            type => 'array',
            min_length => 5,  # Adjust the number of coupons as needed
            elements => {
                type => 'object',
                properties => {
                    code => {
                        type => 'string',
                        format => 'alphanumeric',
                        min_length => 6,
                        max_length => 10,
                    },
                    discount => {
                        type => 'number',
                        minimum => 0,
                        maximum => 100,
                    },
                },
            },
        },
    };

    # Generate random data based on the defined structure
    my $fake_data = random_data($data_structure);

    return $fake_data;
}


1;
