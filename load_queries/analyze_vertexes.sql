SELECT queries.* FROM (
VALUES 
($$VACUUM ANALYZE ldbc.forum$$),
($$VACUUM ANALYZE ldbc.message$$),
($$VACUUM ANALYZE ldbc.post$$),
($$VACUUM ANALYZE ldbc."Comment"$$),
($$VACUUM ANALYZE ldbc.organization$$),
($$VACUUM ANALYZE ldbc.person$$),
($$VACUUM ANALYZE ldbc.place$$),
($$VACUUM ANALYZE ldbc.tag$$),
($$VACUUM ANALYZE ldbc.tagclass$$)) queries;
