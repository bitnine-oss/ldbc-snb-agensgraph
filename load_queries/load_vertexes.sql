SELECT queries.* FROM (
VALUES
($$LOAD FROM fdwForum AS row
   CREATE (:Forum =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwOrganization AS ROW
   CREATE (:Organization =JSONB_STRIP_NULLS(row_to_json(row)::JSONB))$$),
($$LOAD FROM person_view as row
   CREATE (:Person =JSONB_STRIP_NULLS(row_to_json(row)::JSONB))$$),
($$LOAD FROM fdwPlace as row
   CREATE (:Place =JSONB_STRIP_NULLS(row_to_json(row)::JSONB))$$),
($$LOAD FROM fdwTag as row
   CREATE (:Tag =JSONB_STRIP_NULLS(row_to_json(row)::JSONB))$$),
($$LOAD FROM fdwTagClass as row
   CREATE (:TagClass =JSONB_STRIP_NULLS(row_to_json(row)::JSONB))$$)) queries;
