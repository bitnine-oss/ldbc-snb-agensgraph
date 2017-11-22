-- pre_eval weights
DROP TABLE IF EXISTS c14_weight;
CREATE UNLOGGED TABLE c14_weight(p1 INT8, p2 INT8, weight double precision);
INSERT INTO c14_weight
    SELECT p1, p2, SUM(inc) FROM (
        SELECT
            CASE when rep_creator < org_creator
                THEN rep_creator ELSE org_creator END AS p1,
            CASE when rep_creator < org_creator
                THEN org_creator ELSE rep_creator END AS p2,
            inc
        FROM
		(
            MATCH
            (p1:Person)<-[:hasCreatorComment]-(c)-[:replyOfPost]->(m)-[:hasCreatorPost]->(p2:Person)
			, (p1:Person)-[:knows]->(p2:Person)
			RETURN p1.id::INT8 AS rep_creator, p2.id::INT8 AS org_creator, 1.0 AS inc
            UNION ALL
            MATCH
            (p1:Person)<-[:hasCreatorComment]-(c)-[:replyOfComment]->(m)-[:hasCreatorComment]->(p2:Person)
			, (p1:Person)-[:knows]->(p2:Person)
			RETURN p1.id::INT8 AS rep_creator, p2.id::INT8 AS org_creator, 0.5
        ) AS x
    ) AS x
    GROUP BY p1, p2;
CREATE UNIQUE INDEX ON c14_weight(p1, p2);
ALTER TABLE c14_weight SET LOGGED;
