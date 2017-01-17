SELECT queries.* FROM (
VALUES 
($$CLUSTER VERBOSE ldbc.message USING message_cc_idx$$),
($$CLUSTER VERBOSE ldbc.post USING post_cc_idx$$),
($$CLUSTER VERBOSE ldbc."Comment" USING comment_cc_idx$$)) queries;
