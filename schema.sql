\timing on

-- Set the source path
\set source_path '/home/ktlee/tools/ldbc_snb_datagen/social_network'

drop graph ldbc cascade;
create graph ldbc;
set graph_path = ldbc;

-- Make Vertex Labels
create vlabel Forum;
create vlabel Message;
create vlabel Post inherits (Message);
create vlabel "Comment" inherits (Message);
create vlabel Organization;
create vlabel Person;
create vlabel Place;
create vlabel Tag;
create vlabel TagClass;

-- Make Edge Labels
create elabel containerOf;
create elabel hasCreator;
create elabel hasInterest;
create elabel hasMember;
create elabel hasModerator;
create elabel hasTag;
create elabel hasType;
create elabel isLocatedIn;
create elabel isPartOf;
create elabel isSubclassOf;
create elabel knows;
create elabel likes;
create elabel replyOf;
create elabel studyAt;
create elabel workAt;

-- Make Unlogged
--alter table ldbc.forum set unlogged;
--alter table ldbc.message set unlogged;
--alter table ldbc.post set unlogged;
--alter table ldbc."Comment" set unlogged;
--alter table ldbc.Organization set unlogged;
--alter table ldbc.Person set unlogged;
--alter table ldbc.Place set unlogged;
--alter table ldbc.Tag set unlogged;
--alter table ldbc.TagClass set unlogged;
--alter table ldbc.containerOf set unlogged;
--alter table ldbc.hasCreator set unlogged;
--alter table ldbc.hasInterest set unlogged;
--alter table ldbc.hasMember set unlogged;
--alter table ldbc.hasModerator set unlogged;
--alter table ldbc.hasTag set unlogged;
--alter table ldbc.hasType set unlogged;
--alter table ldbc.isLocatedIn set unlogged;
--alter table ldbc.isPartOf set unlogged;
--alter table ldbc.isSubclassOf set unlogged;
--alter table ldbc.knows set unlogged;
--alter table ldbc.likes set unlogged;
--alter table ldbc.replyOf set unlogged;
--alter table ldbc.studyAt set unlogged;
--alter table ldbc.workAt set unlogged;
--
---- Drop Indexes
--alter table ldbc.Forum drop constraint Forum_pkey;
--alter table ldbc.Message drop constraint Message_pkey;
--alter table ldbc.Post drop constraint Post_pkey;
--alter table ldbc."Comment" drop constraint "Comment"_pkey;
--alter table ldbc.Organization drop constraint Organization_pkey;
--alter table ldbc.Person drop constraint Person_pkey;
--alter table ldbc.Place drop constraint Place_pkey;
--alter table ldbc.Tag drop constraint Tag_pkey;
--alter table ldbc.TagClass drop constraint TagClass_pkey;
--
--drop index ldbc.containerOf_start_idx;
--drop index ldbc.hasCreator_start_idx;
--drop index ldbc.hasInterest_start_idx;
--drop index ldbc.hasMember_start_idx;
--drop index ldbc.hasModerator_start_idx;
--drop index ldbc.hasTag_start_idx;
--drop index ldbc.hasType_start_idx;
--drop index ldbc.isLocatedIn_start_idx;
--drop index ldbc.isPartOf_start_idx;
--drop index ldbc.isSubclassOf_start_idx;
--drop index ldbc.knows_start_idx;
--drop index ldbc.likes_start_idx;
--drop index ldbc.replyOf_start_idx;
--drop index ldbc.studyAt_start_idx;
--drop index ldbc.workAt_start_idx;
--
--drop index ldbc.containerOf_end_idx;
--drop index ldbc.hasCreator_end_idx;
--drop index ldbc.hasInterest_end_idx;
--drop index ldbc.hasMember_end_idx;
--drop index ldbc.hasModerator_end_idx;
--drop index ldbc.hasTag_end_idx;
--drop index ldbc.hasType_end_idx;
--drop index ldbc.isLocatedIn_end_idx;
--drop index ldbc.isPartOf_end_idx;
--drop index ldbc.isSubclassOf_end_idx;
--drop index ldbc.knows_end_idx;
--drop index ldbc.likes_end_idx;
--drop index ldbc.replyOf_end_idx;
--drop index ldbc.studyAt_end_idx;
--drop index ldbc.workAt_end_idx;
--
--drop index ldbc.containerOf_id_idx;
--drop index ldbc.hasCreator_id_idx;
--drop index ldbc.hasInterest_id_idx;
--drop index ldbc.hasMember_id_idx;
--drop index ldbc.hasModerator_id_idx;
--drop index ldbc.hasTag_id_idx;
--drop index ldbc.hasType_id_idx;
--drop index ldbc.isLocatedIn_id_idx;
--drop index ldbc.isPartOf_id_idx;
--drop index ldbc.isSubclassOf_id_idx;
--drop index ldbc.knows_id_idx;
--drop index ldbc.likes_id_idx;
--drop index ldbc.replyOf_id_idx;
--drop index ldbc.studyAt_id_idx;
--drop index ldbc.workAt_id_idx;

-- Forum
\set file_name :source_path/forum_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwForum;
create foreign table fdwForum 
	(
		id int8, 
		title varchar(256), 
		creationDate int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwForum as row
create (:Forum =row_to_json(row)::jsonb);

-- Message

--- Post (inherits Message)
\set file_name :source_path/post_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPost;
create foreign table fdwPost 
	(
		id int8, 
		imageFile varchar(80),
		creationDate int8,
		locationIP varchar(80),
		browserUsed varchar(80),
		lanaguage varchar(80),
		content text,
		length int4
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPost as row
create (:Post =row_to_json(row)::jsonb);

--- Comment (inherits Message)
\set file_name :source_path/comment_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwComment;
create foreign table fdwComment 
	(
		id int8, 
		creationDate int8,
		locationIP varchar(80),
		browserUsed varchar(80),
		content varchar(2000),
		length int4
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwComment as row
create (:"Comment" =row_to_json(row)::jsonb);

-- Organization
\set file_name :source_path/organisation_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwOrganization;
create foreign table fdwOrganization 
	(
		id int8, 
		type varchar(80),
		name varchar(200),
		url varchar(200)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwOrganization as row
create (:Organization =row_to_json(row)::jsonb);

-- Person
\set file_name :source_path/person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPerson;
create foreign table fdwPerson 
	(
		id int8, 
		firstName varchar(80),
		lastName varchar(80),
		gender varchar(6),
		birthday int8,
		creationDate int8,
		locationIP varchar(80),
		browserUsed varchar(80)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPerson as row
create (:Person =row_to_json(row)::jsonb);

-- Place
\set file_name :source_path/place_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPlace;
create foreign table fdwPlace 
	(
		id int8, 
		name varchar(200),
		url varchar(200),
		type varchar(80)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPlace as row
create (:Place =row_to_json(row)::jsonb);

-- Tag
\set file_name :source_path/tag_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwTag;
create foreign table fdwTag 
	(
		id int8, 
		name varchar(200),
		url varchar(200)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwTag as row
create (:Tag =row_to_json(row)::jsonb);

-- TagClass
\set file_name :source_path/tagclass_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwTagClass;
create foreign table fdwTagClass 
	(
		id int8, 
		name varchar(200),
		url varchar(200)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwTagClass as row
create (:TagClass =row_to_json(row)::jsonb);



-- Make property indexes for 'id' field of vertexes
--- should be replaced to using CREATE PROPERTY INDEX

create UNIQUE index on ldbc."Comment" (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.forum (((properties #>> '{id}'::text[])::int8));      
create UNIQUE index on ldbc.message (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.organization (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.person (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.place (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.post (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.tag (((properties #>> '{id}'::text[])::int8));
create UNIQUE index on ldbc.tagclass (((properties #>> '{id}'::text[])::int8));

analyze ldbc."Comment";
analyze ldbc.forum;
analyze ldbc.message;
analyze ldbc.organization;
analyze ldbc.person;
analyze ldbc.place;
analyze ldbc.post;
analyze ldbc.tag;
analyze ldbc.tagclass;

-- Edge
--- containerOf
\set file_name :source_path/forum_containerOf_post_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwContainerOf;
create foreign table fdwContainerOf 
	(
		forumId int8, 
		postId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwContainerOf as row
match (r:Forum), (s:Post)
where (r).id::int8 = (row).forumId and (s).id::int8 = (row).postId
create (r)-[:containerOf]->(s);

--- hasCreator (for Post)
\set file_name :source_path/post_hasCreator_person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPostHasCreator;
create foreign table fdwPostHasCreator
	(
		postId int8, 
		personId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPostHasCreator as row
match (r:Post), (s:Person)
where (r).id::int8 = (row).postId and (s).id::int8 = (row).personId
create (r)-[:hasCreator]->(s);

--- hasCreator (for comment)
\set file_name :source_path/comment_hasCreator_person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwCommentHasCreator;
create foreign table fdwCommentHasCreator
	(
		commentId int8, 
		personId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwCommentHasCreator as row
match (r:"Comment"), (s:Person)
where (r).id::int8 = (row).commentId and (s).id::int8 = (row).personId
create (r)-[:hasCreator]->(s);

--- hasInterest
\set file_name :source_path/person_hasInterest_tag_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwHasInterest;
create foreign table fdwHasInterest
	(
		personId int8, 
		tagId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwHasInterest as row
match (r:Person), (s:Tag)
where (r).id::int8 = (row).personId and (s).id::int8 = (row).tagId
create (r)-[:hasInterest]->(s);

-- hasMember
\set file_name :source_path/forum_hasMember_person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwHasMember;
create foreign table fdwHasMember
	(
		forumId int8, 
		personId int8,
		joinDate int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwHasMember as row
match (r:Forum), (s:Person)
where (r).id::int8 = (row).forumId and (s).id::int8 = (row).personId
create (r)-[:hasMember {'joinDate': (row).joinDate}]->(s);

--- hasModerator
\set file_name :source_path/forum_hasModerator_person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwHasModerator;
create foreign table fdwHasModerator
	(
		forumId int8, 
		personId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwHasModerator as row
match (r:Forum), (s:Person)
where (r).id::int8 = (row).forumId and (s).id::int8 = (row).personId
create (r)-[:hasModerator]->(s);

--- hasTag (post)
\set file_name :source_path/post_hasTag_tag_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPostHasTag;
create foreign table fdwPostHasTag
	(
		postId int8, 
		tagId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPostHasTag as row
match (r:Post), (s:Tag)
where (r).id::int8 = (row).postId and (s).id::int8 = (row).tagId
create (r)-[:hasTag]->(s);

--- hasTag (comment)
\set file_name :source_path/comment_hasTag_tag_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwCommentHasTag;
create foreign table fdwCommentHasTag
	(
		commentId int8, 
		tagId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwCommentHasTag as row
match (r:"Comment"), (s:Tag)
where (r).id::int8 = (row).commentId and (s).id::int8 = (row).tagId
create (r)-[:hasTag]->(s);

--- hasTag (forum)
\set file_name :source_path/forum_hasTag_tag_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwForumHasTag;
create foreign table fdwForumHasTag
	(
		forumId int8, 
		tagId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwForumHasTag as row
match (r:Forum), (s:Tag)
where (r).id::int8 = (row).forumId and (s).id::int8 = (row).tagId
create (r)-[:hasTag]->(s);

--- hasType
\set file_name :source_path/tag_hasType_tagclass_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwHasType;
create foreign table fdwHasType
	(
		tagId int8, 
		tagclassId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwHasType as row
match (r:Tag), (s:TagClass)
where (r).id::int8 = (row).tagId and (s).id::int8 = (row).tagclassId
create (r)-[:hasType]->(s);

--- isLocatedIn (organisation)
\set file_name :source_path/organisation_isLocatedIn_place_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwOrganIsLocatedIn;
create foreign table fdwOrganIsLocatedIn
	(
		organId int8, 
		placeId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwOrganIsLocatedIn as row
match (r:Organization), (s:Place)
where (r).id::int8 = (row).organId and (s).id::int8 = (row).placeId
create (r)-[:isLocatedIn]->(s);

--- isLocatedIn (post)
\set file_name :source_path/post_isLocatedIn_place_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwPostIsLocatedIn;
create foreign table fdwPostIsLocatedIn
	(
		postId int8, 
		placeId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPostIsLocatedIn as row
match (r:Post), (s:Place)
where (r).id::int8 = (row).postId and (s).id::int8 = (row).placeId
create (r)-[:isLocatedIn]->(s);

--- isLocatedIn (comment)
\set file_name :source_path/comment_isLocatedIn_place_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwCommentIsLocatedIn;
create foreign table fdwCommentIsLocatedIn
	(
		commentId int8, 
		placeId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwCommentIsLocatedIn as row
match (r:"Comment"), (s:Place)
where (r).id::int8 = (row).commentId and (s).id::int8 = (row).placeId

--- isLocatedIn (person)
\set file_name :source_path/person_isLocatedIn_place_0_0.csv
\echo Start Loading :file_name
create (r)-[:isLocatedIn]->(s);
drop foreign table fdwPersonIsLocatedIn;
create foreign table fdwPersonIsLocatedIn
	(
		personId int8, 
		placeId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwPersonIsLocatedIn as row
match (r:Person), (s:Place)
where (r).id::int8 = (row).personId and (s).id::int8 = (row).placeId
create (r)-[:isLocatedIn]->(s);

--- isPartOf
\set file_name :source_path/place_isPartOf_place_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwIsPartOf;
create foreign table fdwIsPartOf
	(
		place1Id int8, 
		place2Id int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwIsPartOf as row
match (r:Place), (s:Place)
where (r).id::int8 = (row).place1Id and (s).id::int8 = (row).place2Id
create (r)-[:isPartOf]->(s);

--- isSubclassOf
\set file_name :source_path/tagclass_isSubclassOf_tagclass_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwIsSubclassOf;
create foreign table fdwIsSubclassOf
	(
		tagclass1Id int8, 
		tagclass2Id int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwIsSubclassOf as row
match (r:TagClass), (s:TagClass)
where (r).id::int8 = (row).tagclass1Id and (s).id::int8 = (row).tagclass2Id
create (r)-[:isSubclassOf]->(s);

--- knows
\set file_name :source_path/person_knows_person_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwKnows;
create foreign table fdwKnows
	(
		person1Id int8, 
		person2Id int8,
		creationDate int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwKnows as row
match (r:Person), (s:Person)
where (r).id::int8 = (row).person1Id and (s).id::int8 = (row).person2Id
create (r)-[:knows {'creationDate': (row).creationDate}]->(s);

--- likes (post)
\set file_name :source_path/person_likes_post_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwLikesPost;
create foreign table fdwLikesPost
	(
		personId int8, 
		postId int8,
		creationDate int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwLikesPost as row
match (r:Person), (s:Post)
where (r).id::int8 = (row).personId and (s).id::int8 = (row).postId
create (r)-[:likes {'creationDate': (row).creationDate}]->(s);

--- likes (post)
\set file_name :source_path/person_likes_comment_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwLikesComment;
create foreign table fdwLikesComment
	(
		personId int8, 
		commentId int8,
		creationDate int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwLikesComment as row
match (r:Person), (s:"Comment")
where (r).id::int8 = (row).personId and (s).id::int8 = (row).commentId
create (r)-[:likes {'creationDate': (row).creationDate}]->(s);

--- replyOf (post)
\set file_name :source_path/comment_replyOf_post_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwReplyOfPost;
create foreign table fdwReplyOfPost
	(
		commentId int8, 
		postId int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwReplyOfPost as row
match (r:"Comment"), (s:Post)
where (r).id::int8 = (row).commentId and (s).id::int8 = (row).postId
create (r)-[:replyOf]->(s);

--- replyOf (comment)
\set file_name :source_path/comment_replyOf_comment_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwReplyOfComment;
create foreign table fdwReplyOfComment
	(
		comment1Id int8, 
		comment2Id int8
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwReplyOfComment as row
match (r:"Comment"), (s:"Comment")
where (r).id::int8 = (row).comment1Id and (s).id::int8 = (row).comment2Id
create (r)-[:replyOf]->(s);

--- studyAt
\set file_name :source_path/person_studyAt_organisation_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwStudyAt;
create foreign table fdwStudyAt
	(
		personId int8, 
		organId int8,
		classYear char(4)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwStudyAt as row
match (r:Person), (s:Organization)
where (r).id::int8 = (row).personId and (s).id::int8 = (row).organId
create (r)-[:studyAt {'classYear': (row).classYear}]->(s);

--- workAt
\set file_name :source_path/person_workAt_organisation_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwWorkAt;
create foreign table fdwWorkAt
	(
		personId int8, 
		organId int8,
		workFrom char(4)
	)
	server graph_import
	options 
	(
		 FORMAT 'csv',
		 HEADER 'true',
		 DELIMITER '|',
		 NULL '',
		 FILENAME :'file_name'
	);
load from fdwWorkAt as row
match (r:Person), (s:Organization)
where (r).id::int8 = (row).personId and (s).id::int8 = (row).organId
create (r)-[:workAt {'workFrom': (row).workFrom}]->(s);

-- create index
---create index on ldbc.containerOf (start, "end", id);
---create index on ldbc.hasCreator (start, "end", id);
---create index on ldbc.hasInterest (start, "end", id);
---create index on ldbc.hasMember (start, "end", id);
---create index on ldbc.hasModerator (start, "end", id);
---create index on ldbc.hasTag (start, "end", id);
---create index on ldbc.hasType (start, "end", id);
---create index on ldbc.isLocatedIn (start, "end", id);
---create index on ldbc.isPartOf (start, "end", id);
---create index on ldbc.isSubclassOf (start, "end", id);
---create index on ldbc.knows (start, "end", id);
---create index on ldbc.likes (start, "end", id);
---create index on ldbc.replyOf (start, "end", id);
---create index on ldbc.studyAt (start, "end", id);
---create index on ldbc.workAt (start, "end", id);
---
---create index on ldbc.containerOf ("end", start, id);
---create index on ldbc.hasCreator ("end", start, id);
---create index on ldbc.hasInterest ("end", start, id);
---create index on ldbc.hasMember ("end", start, id);
---create index on ldbc.hasModerator ("end", start, id);
---create index on ldbc.hasTag ("end", start, id);
---create index on ldbc.hasType ("end", start, id);
---create index on ldbc.isLocatedIn ("end", start, id);
---create index on ldbc.isPartOf ("end", start, id);
---create index on ldbc.isSubclassOf ("end", start, id);
---create index on ldbc.knows ("end", start, id);
---create index on ldbc.likes ("end", start, id);
---create index on ldbc.replyOf ("end", start, id);
---create index on ldbc.studyAt ("end", start, id);
---create index on ldbc.workAt ("end", start, id);
---
---create unique index on ldbc.Forum (id);
---create unique index on ldbc.Message (id);
---create unique index on ldbc.Post (id);
---create unique index on ldbc."Comment" (id);
---create unique index on ldbc.Organization (id);
---create unique index on ldbc.Person (id);
---create unique index on ldbc.Place (id);
---create unique index on ldbc.Tag (id);
---create unique index on ldbc.TagClass (id);

-- Analyze edges 
analyze ldbc.containerOf;
analyze ldbc.hasCreator;
analyze ldbc.hasInterest;
analyze ldbc.hasMember;
analyze ldbc.hasModerator;
analyze ldbc.hasTag;
analyze ldbc.hasType;
analyze ldbc.isLocatedIn;
analyze ldbc.isPartOf;
analyze ldbc.isSubclassOf;
analyze ldbc.knows;
analyze ldbc.likes;
analyze ldbc.replyOf;
analyze ldbc.studyAt;
analyze ldbc.workAt;

