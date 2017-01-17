SELECT 'SELECT PG_PREWARM(' || c.oid || ')'  
FROM 
  pg_class c 
LEFT JOIN 
  pg_namespace n 
ON n.oid = c.relnamespace 
WHERE 
  nspname = 'ldbc' AND (relkind = 'i' OR relkind = 'r');
