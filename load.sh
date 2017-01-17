#!/bin/bash
#
# This script loads make the LDBC schema and load the specified dataset.
# 
# ./load.sh [CONFIGURATION_FILE]

source $1

agens -d $DBNAME -p $PORT -U $USER -f load_queries/enable_fdw.sql -L ldbc_loading.log -q -o load_output.log 2>/dev/null
agens -d $DBNAME -p $PORT -U $USER -v source_path=$DATAPATH -v target_db=$DBNAME -f load_queries/schema.sql -L ldbc_loading.log -q -o load_output.log 2>/dev/null

pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/load_vertexes.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/load_messages.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/create_vertex_property_indexes.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/load_edges.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/cluster_vertexes.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/analyze_vertexes.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/analyze_edges.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/reindex.sql
pg_batch -d $DBNAME -p $PORT -U $USER -j 2 load_queries/prewarm.sql

agens -d $DBNAME -p $PORT -U $USER -f load_queries/weight_precal.sql -L ldbc_loading.log -q -o load_output.log 2>/dev/null
agens -d $DBNAME -p $PORT -U $USER -f load_queries/proc.sql -L ldbc_loading.log -q -o load_output.log 2>/dev/null

