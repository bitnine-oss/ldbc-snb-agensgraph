SELECT queries.* FROM (
VALUES
($$LOAD FROM fdwForum AS row
   CREATE (:Forum =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwOrganization AS row
   CREATE (:Organization =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM person_view AS row
   CREATE (:Person =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwPlace AS row
   CREATE (:Place =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwPost AS row
   CREATE (:Post =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwComment AS row
   CREATE (:"Comment" =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwTag AS row
   CREATE (:Tag =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$),
($$LOAD FROM fdwTagClass AS row
   CREATE (:TagClass =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB))$$)) queries;
