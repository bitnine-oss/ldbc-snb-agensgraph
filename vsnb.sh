#!/bin/sh

java -cp \
target/classes/jeeves-0.3-SNAPSHOT.jar\
:target/classes/agensgraph-jdbc-0.9.0-SNAPSHOT.jar\
:target/classes \
com.ldbc.driver.Client \
-P ldbc_snb_validation.properties \
-P ag_jdbc.properties \
