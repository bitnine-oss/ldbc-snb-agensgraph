-- used in complex query 10
create or replace function c10_fc(posts vertex[])
returns vertex[] as $$
declare
	arr vertex[];
	has_interest_tag boolean;
	p vertex;
begin
	if posts is null
	then
		return arr;
	end if;

	foreach p in array posts
	loop
		execute 'match (:Post {''id'': $1})-[:hasTag]->(:Tag)<-[:hasInterest]-(:Person) '
			|| 'return count(p) > 0'
			into has_interest_tag using p.id::int8;
		if has_interest_tag
		then
			arr := array_append(arr, p);
		end if;
	end loop;

	return arr;
end
$$ language plpgsql;

create or replace function calc_weight(node_ids int8[])
returns double precision as $$
declare
	weight double precision;
	prev int8;
	curr int8;
	p1 graphpath;
	p2 graphpath;
	p3 graphpath;
begin
	weight := 0.0;
	for i in 1..array_length(node_ids, 1) - 1 loop
		prev := path_nodes[i - 1];
		curr := path_nodes[i];
		execute 'match p=(:Person {''id'': $1})<-[:hasCreator]-(:"Comment")-[:replyOf]->(:Post)-[:hasCreator]->(:Person {''id'': $2})'
			|| 'return p'
			into p1 using curr, prev;
		execute 'match p=(:Person {''id'': $1})<-[:hasCreator]-(:"Comment")-[:replyOf]->(:Post)-[:hasCreator]->(:Person {''id'': $2})'
			|| 'return p'
			into p2 using prev, curr;
		execute 'match p=(:Person {''id'': $1})-[:hasCreator]-(:"Comment")-[:replyOf]->(:Comment)-[:hasCreator]-(:Person {''id'': $2})'
			|| 'return p'
			into p3 using prev, curr;

		weight := weight + length(p1) * 1.0 + length(p2) * 1.0 + length(p3) * 0.5;
	end loop;

	return weight;
end
$$ language plpgsql;

create or replace function extract_ids(node_ids int8[])
returns jsonb as $$
declare
	id int8;
	arr int8[];
begin
	foreach id in array path_nods
	loop
		arr = array_append(arr, id);
	end loop;

	return array_to_json(arr)::jsonb;
end
$$ language plpgsql;

CREATE OR REPLACE FUNCTION knows_shortestpaths(startnode graphid, endnode graphid)
  RETURNS TABLE(paths graphid[]) AS
$BODY$
DECLARE
    rowcount int;
    iter int;
	size int;
	x graphid[]; 
	i int;
BEGIN
    -- Create a temporary table for storing the estimates as the algorithm runs
    CREATE TEMP TABLE inter_result1
    (   
        nid graphid,
        path graphid[] 
    ) ON COMMIT DROP;

    CREATE TEMP TABLE inter_result2 
    (   
        nid graphid,
        path graphid[] 
    ) ON COMMIT DROP;

    INSERT INTO inter_result1 VALUES (startnode, ARRAY[startnode]);
    iter := 0;

    -- Run the algorithm until we decide that we are finished
    LOOP
        IF iter % 2 = 0 THEN
            INSERT INTO inter_result2 
                SELECT distinct e."end", i.path || e."end"
                FROM ldbc.knows AS e, inter_result1 AS i
                WHERE nid = e.start AND e."end" = endnode;
        ELSE
            INSERT INTO inter_result1 
                SELECT distinct e."end", i.path || e."end"
                FROM ldbc.knows AS e, inter_result2 AS i
                WHERE nid = e.start AND e."end" = endnode;
        END IF; 

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        IF rowcount != 0 THEN EXIT; END IF; 

        IF iter % 2 = 0 THEN
            INSERT INTO inter_result2 
                SELECT distinct e."end", i.path || e."end"
                FROM ldbc.knows AS e, inter_result1 AS i
				WHERE nid = e.start AND NOT (i.path @> array[e."end"]);
        ELSE
            INSERT INTO inter_result1
                SELECT distinct e."end", i.path || e."end"
                FROM ldbc.knows AS e, inter_result2 AS i
                WHERE nid = e.start AND NOT (i.path @> array[e."end"]);
        END IF;

        GET DIAGNOSTICS rowcount = ROW_COUNT;
        IF rowcount = 0 THEN EXIT; END IF;

        IF iter % 2 = 0 THEN
            TRUNCATE TABLE inter_result1;
        ELSE
            TRUNCATE TABLE inter_result2;
        END IF;

        iter := iter + 1;
    END LOOP;

    IF iter % 2 = 0 THEN
        RETURN QUERY SELECT path FROM inter_result2;
    ELSE
        RETURN QUERY SELECT path FROM inter_result1;
    END IF;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

create or replace function conv_person_id(graphids graphid[])
returns int8[] as $$
declare
	gid graphid;
	pid int8;
	ret int8[];
begin
	foreach gid in array graphids 
	loop
		execute 'match (p:Person) where id(p) = $1 return p.id::int8' 
				into pid using gid;
		ret := array_append(ret, pid);
	end loop;
	return ret;
end
$$ language plpgsql;

create or replace function shortestpath_vertex_ids(p1 vertex, p2 vertex)
returns int8[] as $$
declare
	graphids graphid[];
begin
	if p1.id = p2.id then
		return array[(p1.properties->>'id')::int8];
	end if;
	select paths into graphids from knows_shortestpaths(id(p1), id(p2)) limit 1;
	if not found then
		return NULL;
	end if;
	return conv_person_id(graphids);
end
$$ language plpgsql;

create or replace function allshortestpath_vertex_ids(p1_id int8, p2_id int8)
returns table(vertex_ids int8[]) as $$
declare
	vertex_ids int8[];
	graphids graphid[];
	p1 graphid;
	p2 graphid;
begin
	if p1_id = p2_id then
		vertex_ids := array_append(vertex_ids, p1_id);
		return next;
		return;
	end if;
	execute 'match (p1:Person {''id'': $1}), (p2:Person {''id'': $2}) return id(p1), id(p2)'
		into p1, p2 using p1_id, p2_id;
	for graphids in select paths from knows_shortestpaths(p1, p2)
	loop
		vertex_ids := conv_person_id(graphids);
		return next;
	end loop;
	return;
end
$$ language plpgsql;
