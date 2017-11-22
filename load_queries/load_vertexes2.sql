LOAD FROM viewForum       AS row CREATE (:Forum        =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM fdwOrganization AS row CREATE (:Organization =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM viewPerson      AS row CREATE (:Person       =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM fdwPlace        AS row CREATE (:Place        =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM viewPost        AS row CREATE (:Post         =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM viewComment     AS row CREATE (:"Comment"    =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM fdwTag          AS row CREATE (:Tag          =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
LOAD FROM fdwTagClass     AS row CREATE (:TagClass     =JSONB_STRIP_NULLS(ROW_TO_JSON(row)::JSONB));
