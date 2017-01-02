AgensGraph LDBC SNB Interactive Workload Implementation
=======================================================

This is an implementation of LDBC SNB interactive workload for AgensGraph. 
Queries are implemented using not only Cypher, but also SQL. 
Because AgensGraph can integrate SQL with Cypher, 
some queries are optimized by using features of SQL which cannot be expressed by only Cypher.

All queries are passed the validation using https://github.com/ldbc/ldbc_snb_interactive_validation.

## Dataset Loading ##

Copy ag_jdbc.properties.template to ag_jdbc.properties and edit the file according to your AgensGraph setting.
This file is for the JDBC connection information.

Copy load.sql.template to load.sql and open the file and edit the source_path variable to your LDBC dataset directory.

Run the script file.

```
ag-shell -d [DATABASE] -f load.sql
```

After loading ends, you can run LDBC driver.

## Running Benchmark Tests ##

## Validation ##

