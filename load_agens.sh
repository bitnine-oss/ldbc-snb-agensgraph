#!/bin/bash
#
# This script loads make the LDBC schema and load the specified dataset.
# 

agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/enable_fdw.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -v source_path="$LDBC_SNB_DATAGEN_HOME/social_network" -v target_db="$USER" -f load_queries/schema.sql

agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/load_vertexes2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/create_vertex_property_indexes2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/load_edges2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/analyze_vertexes2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/analyze_edges2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/reindex2.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/prewarm2.sql | grep PREWARM | grep -v oid >prewarm.sql

agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/weight_precal.sql
agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/proc.sql

agens -d "$USER" -p "$AGENS_PORT" -U "$USER" --echo-all -P pager=off -f load_queries/alter_logged.sql
