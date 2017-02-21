AgensGraph LDBC SNB Interactive Workload Implementation
=======================================================

This is an implementation of LDBC SNB interactive workload for AgensGraph. 
AgensGraph supports Cypher and SQL.
So the benchmark queries are implemented using not only Cypher but also SQL. 
Some queries could be optimized by using the features of SQL 
which cannot be expressed when using only Cypher.

All queries are passed the validation using https://github.com/ldbc/ldbc_snb_interactive_validation.

This implementation provides three template scripts: validation, data loading and running test.

## Validation ##

The validation script automatically downloads the valdation dataset,
create a temporary database for validation and run the validation. 

Copy run_validation.sh.template to run_validation.sh and edit the copied file according to your AgensGraph setting.

## Dataset Loading ##

Copy load_test.sh.template to load_test.sh and edit the copied file according to your AgensGraph setting.
Before running this script, the database to be used should be created already.

Once ths dataset loading successfully ends, you can run the LDBC running script.

## Running Benchmark Tests ##

Copy run_test.sh.template to run_test.sh and edit the copied file according to your AgensGraph setting.
And copy ldbc-test.properties.template to ldbc-test.properties and edit the copied file according to your LDBC test setting.
You need to read LDBC documentation to set the properties file correctly.
Before you run the script, be careful that the properties files are set right, especially including LDBC driver's properties files
previously set in the driver according to your dataset size.

