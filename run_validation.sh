#!/bin/bash

echo " ========================================= "
echo " AgensGraph LDBC validation running script "
echo " ========================================= "
echo

if [ ! -d "validation_set" ];
then
  echo "=== Downloading validation set... "
  echo 
  wget https://github.com/ldbc/ldbc_snb_interactive_validation/raw/master/neo4j/neo4j--validation_set.tar.gz
  echo "=== Extract validation set into validation_set directory... "
  echo 
  mkdir validation_set
  tar zxf neo4j--validation_set.tar.gz -C validation_set
fi

if [ ! -f "ag_jdbc_validation.properties" ];
then
  echo " [Error] Cannot find 'ag_jdbc_validation.properties'. "
  echo " [Error] Copy and modify 'ag_jdbc.properties.template' to make 'ag_jdbc_validation.properties'. "
  exit 1
fi

if [ ! -f "load_validation.sql" ];
then
  echo " [Error] Cannot find 'load_validation.sql'. "
  echo " [Error] Copy and modify 'load.sql.template' to make 'load_validation.sql'. "
  exit 1
fi

source ag_jdbc_validation.properties

ag-shell -d $dbname -p $port -U $user -f load_validation.sql

java -cp target/classes/jeeves-0.3-SNAPSHOT.jar:target/classes/agensgraph-jdbc-0.9.0-SNAPSHOT.jar:target/classes com.ldbc.driver.Client \
-P ldbc_snb_validation.properties -P ag_jdbc_validation.properties \
-p ldbc.snb.interactive.parameters_dir validation_set/substitution_parameters \
-p ldbc.snb.interactive.updates_dir validation_set/social_network
