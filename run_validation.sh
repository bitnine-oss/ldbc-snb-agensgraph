#!/bin/bash
#
# Usage: run_validation.sh [PORT] [USER]
# should be run by AgensGraph's superuser

PORT=$1
USER=$2
DATAPATH=`pwd`/validation_set/social_network/num_date

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

dropdb -p $PORT agensgraph_ldbc_validation
createdb -p $PORT --lc-collate='C' --template=template0 agensgraph_ldbc_validation

cat > validation.conf <<EOF
DBNAME=agensgraph_ldbc_validation
PORT=$PORT
USER=$USER
DATAPATH=$DATAPATH
EOF

./load.sh validation.conf

java -cp target/classes/jeeves-0.3-SNAPSHOT.jar:target/classes/agensgraph-jdbc-0.9.0-SNAPSHOT.jar:target/classes com.ldbc.driver.Client \
-P validation.properties \
-p dbname agensgraph_ldbc_validation \
-p port $PORT \
-p user $USER \
-p validate_database validation_set/validation_params.csv \
-p ldbc.snb.interactive.parameters_dir validation_set/substitution_parameters \
-p ldbc.snb.interactive.updates_dir validation_set/social_network 2>ldbc_validation_err.log
