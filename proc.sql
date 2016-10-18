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

create or replace function calc_weight(path_nodes vertex[])
returns double precision as $$
declare
	weight double precision;
	prev vertex;
	curr vertex;
	p1 graphpath;
	p2 graphpath;
	p3 graphpath;
begin
	weight := 0.0;
	for i in 1..array_length(path_nodes, 1) - 1 loop
		prev := path_nodes[i - 1];
		curr := path_nodes[i];
		execute 'match p=($1)<-[:hasCreator]-(:"Comment")-[:replyOf]->(:Post)-[:hasCreator]->($2)'
			|| 'return p'
			into p1 using curr, prev;
		execute 'match p=($1)<-[:hasCreator]-(:"Comment")-[:replyOf]->(:Post)-[:hasCreator]->($2)'
			|| 'return p'
			into p2 using prev, curr;
		execute 'match p=($1)-[:hasCreator]-(:"Comment")-[:replyOf]->(:Comment)-[:hasCreator]-($2)'
			|| 'return p'
			into p3 using prev, curr;

		weight := weight + length(p1) * 1.0 + length(p2) * 1.0 + length(p3) * 0.5;
	end loop;

	return weight;
end
$$ language plpgsql;

create or replace function extract_ids(path_nodes vertex[])
returns jsonb as $$
declare
	n vertex;
	arr int8[];
begin
	foreach n in array path_nods
	loop
		arr = array_append(arr, n.id);
	end loop;

	return array_to_json(arr)::jsonb;
end
$$ language plpgsql;
