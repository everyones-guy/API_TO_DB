use strict;
use warnings;
use lib 'Modules';
use MyDatabase;
use JSON;
use LWP::UserAgent;
use Log::Log4perl;

# Initialize logging
Log::Log4perl->init(\<<CONFIG);
log4perl.logger = DEBUG, Screen, File

log4perl.appender.Screen = Log::Log4perl::Appender::Screen
log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d %p> %m %n

log4perl.appender.File = Log::Log4perl::Appender::File
log4perl.appender.File.filename = script.log
log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.File.layout.ConversionPattern = %d %p> %m %n
CONFIG

my $logger = Log::Log4perl->get_logger();

# Database settings
my $db_type = "postgresql";  # Change this to "sqlite" if you want to use SQLite
my $db_name = "coupon_data";  # For SQLite, you can keep the file name here
my $db_user = "your_postgresql_user";
my $db_pass = "your_postgresql_password";
my $db_host = "your_postgresql_host";
my $db_port = 5432;  # Change to the actual PostgreSQL port

# API endpoint (replace with your actual API URL)
my $api_url = "https://example.com/api/coupons";

# Sample API schema (replace with your actual schema)
my $api_schema = {
    type => "object",
    properties => {
        coupons => {
            type => "array",
            items => {
                type => "object",
                properties => {
                    code => { type => "string" },
                    discount => { type => "integer" },
                    expiration_date => { type => "string" },
                },
            },
        },
    },
};

# Create a new instance of the MyDatabase module
my $db = MyDatabase->new;

# Connect to the database
eval {
    $db->connect({
        db_type => $db_type,
        db_name => $db_name,
        db_user => $db_user,
        db_pass => $db_pass,
        db_host => $db_host,
        db_port => $db_port
    });
};

if ($@) {
    $logger->error("Database connection error: $@");
    exit 1;
}

# Simulate API call with error handling
my $api_data = $db->simulate_api_call($api_url, $api_schema);

if (!$api_data) {
    $logger->error("API call failed");
    $db->disconnect;
    exit 1;
}

# Parse API data with error handling
my @coupons;
eval {
    @coupons = $db->parse_api_response($api_data);
};

if ($@) {
    $logger->error("API response parsing error: $@");
    $db->disconnect;
    exit 1;
}

# Store the data in the database with error handling
eval {
    $db->store_data_in_database(\@coupons);
};

if ($@) {
    $logger->error("Database storage error: $@");
    $db->disconnect;
    exit 1;
}

# Export data to a generic database
my $data_structure = {
    database_name => "my_new_database",
    table_name    => "my_new_table",
    data          => \@coupons,
};

$db->export_data('file', $data_structure, 'db');

# Export data to the remote service
$data_structure = {
    data => \@coupons,
};

$db->export_data('remote_service', $data_structure);

# Disconnect from the database
$db->disconnect;

sub parse_api_response {
    my ($api_data) = @_;

    my $data = eval { decode_json($api_data) };

    if ($@) {
        $logger->error("API response parsing error: $@");
        return;
    }

    return @{$data->{coupons}};
}

sub store_data_in_database {
    my ($coupons) = @_;

    my $create_table_sql = <<SQL;
    CREATE TABLE IF NOT EXISTS coupons (
        id SERIAL PRIMARY KEY,
        code TEXT,
        discount INTEGER,
        expiration_date TEXT
    );
SQL

    $db->execute_sql($create_table_sql);

    my $insert_sql = "INSERT INTO coupons (code, discount, expiration_date) VALUES (?, ?, ?)";
    foreach my $coupon (@$coupons) {
        $db->execute_sql($insert_sql, $coupon->{code}, $coupon->{discount}, $coupon->{expiration_date});
    }
}

1;
