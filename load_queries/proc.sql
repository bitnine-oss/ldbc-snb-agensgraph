create or replace function c7(likes jsonb[])
returns jsonb as $$
declare
	elem jsonb;
	latestLikeTime int8;
	latestLikeId int8;
	latestLike jsonb;
begin
	latestLikeTime := 0;
	foreach elem in array likes
	loop
		if (elem->>'likeTime')::int8 > latestLikeTime
		then
			latestLikeTime := (elem->>'likeTime')::int8;
			latestLikeId := (elem->>'id')::int8;
			latestLike := elem;
		elsif (elem->>'likeTime')::int8 = latestLikeTime
		then
			if (elem->>'id')::int8 < latestLikeId
			then
				latestLikeTime := (elem->>'likeTime')::int8;
				latestLikeId := (elem->>'id')::int8;
				latestLike := elem;
			end if;
		end if;
	end loop;

	return latestLike;
end
$$ language plpgsql;

-- used in complex query 10
set graph_path = ldbc;

create or replace function c10_fc(post_ids int8[], person_id int8)
returns int8 as $$
	select count(*)
	from unnest(post_ids) x (id)
	where exists (select 1
				  from (match (post:Post)-[:hasTagPost]->(:Tag)<-[:hasInterest]-(p:Person)
				  where post.id = to_jsonb(x.id) and p.id = person_id return 1) y);
$$ language sql;

create or replace function upd_reply_weight (p1id bigint, postid bigint, commentid bigint)
returns int as $$
declare
	p2id int8;
	tmp int8;
	reply int8;
  	inc double precision;
begin
	match (m:message)-[:hasCreator]->(creator:Person)
	where m.id = postid + commentid + 1
	return creator.id into p2id;
	if not found then
		return 0;
	end if;
	if postid <> -1 then
		inc := 1;
 	else
 		inc := 0.5;
	end if;
  	if p1id > p2id then
		tmp := p2id;
		p2id := p1id;
		p1id := tmp;
	end if;
  	if not exists ((:person {id: p1id})-[:knows]->(:person {id: p2id})) then
		return 0;
	end if;
	if exists (select 1 from c14_weight where p1 = p1id and p2 = p2id for update) then
    	update c14_weight set weight = weight + inc where p1 = p1id and p2 = p2id;
	else
    	insert into c14_weight values (p1id, p2id, inc);
	end if;
	return 1;
end
$$ language plpgsql;

create or replace function upd_weight(p1id int8, p2id int8)
returns double precision as $$
declare
  a double precision;
  b double precision;
  c double precision;
  d double precision;
begin
	SELECT SUM(1.0) INTO a
	FROM (SELECT 1
	FROM (
		match (p1:Person)<-[:hasCreatorComment]-(:"Comment")-[:replyOfPost]->(:Post)-[:hasCreatorPost]->(p2:Person)
		where p1.id = p1id and p2.id = p2id
		RETURN 1 AS col1) AS dummy
	FOR UPDATE) AS dummy;

	SELECT SUM(1.0) INTO b
	FROM (SELECT 1
	FROM (
		match (p1:Person)<-[:hasCreatorComment]-(:"Comment")-[:replyOfPost]->(:Post)-[:hasCreatorPost]->(p2:Person)
		where p1.id = p2id and p2.id = p1id
		RETURN 1 AS col1) AS dummy
	FOR UPDATE) AS dummy;

	SELECT SUM(0.5) INTO c
	FROM (SELECT 1
	FROM (
		match (p1:Person)<-[:hasCreatorComment]-(:"Comment")-[:replyOfComment]->(:"Comment")-[:hasCreatorComment]->(p2:Person)
		where p1.id = p1id and p2.id = p2id
		RETURN 1 AS col1) AS dummy
	FOR UPDATE) AS dummy;

	SELECT SUM(0.5) INTO d
	FROM (SELECT 1
	FROM (
		match (p1:Person)<-[:hasCreatorComment]-(:"Comment")-[:replyOfComment]->(:"Comment")-[:hasCreatorComment]->(p2:Person)
		where p1.id = p2id and p2.id = p1id
		RETURN 1 AS col1) AS dummy
	FOR UPDATE) AS dummy;

	return coalesce(a, 0) + coalesce(b, 0) + coalesce(c, 0) + coalesce(d);
end
$$ language plpgsql;

create or replace function add_weight (p1 int8, p2 int8)
returns int as $$
declare
	cw double precision;
begin
 	cw := upd_weight(p1, p2);
	if cw > 0.0 then
		if p1 < p2 then
			insert into c14_weight values (p1, p2, cw);
		else
			insert into c14_weight values (p1, p2, cw);
		end if;
	end if;
	return 1;
end
$$ language plpgsql;

create or replace function get_weight (node_ids int8[])
returns double precision as $$
declare
	all_weight double precision;
	small_ids int8[];
	big_ids int8[];
	small int8;
	big int8;
	tmp int8;
begin
	all_weight := 0.0;
	for i in 1..array_length(node_ids, 1) - 1 loop
		small = node_ids[i];
		big = node_ids[i+1];
		if small > big then
			tmp := small;
			small := big;
			big := tmp;
		end if;
		small_ids := array_append(small_ids, small);
		big_ids := array_append(big_ids, big);
	end loop;
	select sum((select weight
			from c14_weight where p1 = s and p2 = b)) into all_weight
	from unnest(small_ids, big_ids) as x (s, b);
	return all_weight;
end
$$ language plpgsql;

create or replace function extract_ids(node_ids int8[])
returns jsonb as $$
declare
	id int8;
	arr int8[];
begin
	foreach id in array node_ids
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
		match (p:Person) where id(p) = gid return p.id into pid;
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
	graphids graphid[];
	g1 graphid;
	g2 graphid;
begin
	if p1_id = p2_id then
		vertex_ids := array_append(vertex_ids, p1_id);
		return next;
		return;
	end if;

	match (p1:Person), (p2:Person)
	where p1.id = p1_id and p2.id = p2_id return id(p1), id(p2)
	into g1, g2;

	for graphids in select paths from knows_shortestpaths(g1, g2)
	loop
		vertex_ids := conv_person_id(graphids);
		return next;
	end loop;
	return;
end
$$ language plpgsql;
