drop graph ldbc cascade;
create graph ldbc;
set graph_path = ldbc;

-- Make Vertex Labels
CREATE VLABEL Forum;
CREATE VLABEL Message;
CREATE VLABEL Post inherits (Message);
CREATE VLABEL "Comment" inherits (Message);
CREATE VLABEL Organization;
CREATE VLABEL Person;
CREATE VLABEL Place;
CREATE VLABEL Tag;
CREATE VLABEL TagClass;

-- Make Edge Labels
CREATE ELABEL containerOf;
CREATE ELABEL hasCreator;
CREATE ELABEL hasCreatorPost inherits (hasCreator);
CREATE ELABEL hasCreatorComment inherits (hasCreator);
CREATE ELABEL hasInterest;
CREATE ELABEL hasMember;
CREATE ELABEL hasModerator;
CREATE ELABEL hasTag;
CREATE ELABEL hasTagPost inherits (hasTag);
CREATE ELABEL hasTagComment inherits (hasTag);
CREATE ELABEL hasTagForum inherits (hasTag);
CREATE ELABEL hasType;
CREATE ELABEL isLocatedIn;
CREATE ELABEL isLocatedInOrgan inherits (isLocatedIn);
CREATE ELABEL isLocatedInPerson inherits (isLocatedIn);
CREATE ELABEL isLocatedInMsg inherits (isLocatedIn);
CREATE ELABEL isLocatedInPost inherits (isLocatedInMsg);
CREATE ELABEL isLocatedInComment inherits (isLocatedInMsg);
CREATE ELABEL isPartOf;
CREATE ELABEL isSubclassOf;
CREATE ELABEL knows;
CREATE ELABEL likes;
CREATE ELABEL likesPost inherits (likes);
CREATE ELABEL likesComment inherits (likes);
CREATE ELABEL replyOf;
CREATE ELABEL replyOfPost inherits (replyOf);
CREATE ELABEL replyOfComment inherits (replyOf);
CREATE ELABEL studyAt;
CREATE ELABEL workAt;

-- Make Unlogged
alter vlabel forum set unlogged;
alter vlabel message set unlogged;
alter vlabel post set unlogged;
alter vlabel "Comment" set unlogged;
alter vlabel Organization set unlogged;
alter vlabel Person set unlogged;
alter vlabel Place set unlogged;
alter vlabel Tag set unlogged;
alter vlabel TagClass set unlogged;
alter elabel containerOf set unlogged;
alter elabel hasCreator set unlogged;
alter elabel hasCreatorPost set unlogged;
alter elabel hasCreatorComment set unlogged;
alter elabel hasInterest set unlogged;
alter elabel hasMember set unlogged;
alter elabel hasModerator set unlogged;
alter elabel hasTag set unlogged;
alter elabel hasTagPost set unlogged;
alter elabel hasTagComment set unlogged;
alter elabel hasTagForum set unlogged;
alter elabel hasType set unlogged;
alter elabel isLocatedIn set unlogged;
alter elabel isLocatedInOrgan set unlogged;
alter elabel isLocatedInPost set unlogged;
alter elabel isLocatedInComment set unlogged;
alter elabel isLocatedInPerson set unlogged;
alter elabel isPartOf set unlogged;
alter elabel isSubclassOf set unlogged;
alter elabel knows set unlogged;
alter elabel likes set unlogged;
alter elabel likesPost set unlogged;
alter elabel likesComment set unlogged;
alter elabel replyOf set unlogged;
alter elabel replyOfPost set unlogged;
alter elabel replyOfComment set unlogged;
alter elabel studyAt set unlogged;
alter elabel workAt set unlogged;

-- Drop Indexes
drop constraint Forum_pkey on Forum;
drop constraint Message_pkey on Message;
drop constraint Post_pkey on Post;
drop constraint "Comment_pkey" on "Comment";
drop constraint Organization_pkey on Organization;
drop constraint Person_pkey on Person;
drop constraint Place_pkey on Place;
drop constraint Tag_pkey on Tag;
drop constraint TagClass_pkey on TagClass;

drop index ldbc.forum_properties_idx;
drop index ldbc.message_properties_idx;
drop index ldbc.post_properties_idx;
drop index ldbc."Comment_properties_idx";
drop index ldbc.organization_properties_idx;
drop index ldbc.person_properties_idx;
drop index ldbc.place_properties_idx;
drop index ldbc.tag_properties_idx;
drop index ldbc.tagclass_properties_idx;

drop index ldbc.containerOf_start_idx;
drop index ldbc.hasCreator_start_idx;
drop index ldbc.hasCreatorPost_start_idx;
drop index ldbc.hasCreatorComment_start_idx;
drop index ldbc.hasInterest_start_idx;
drop index ldbc.hasMember_start_idx;
drop index ldbc.hasModerator_start_idx;
drop index ldbc.hasTag_start_idx;
drop index ldbc.hasTagPost_start_idx;
drop index ldbc.hasTagComment_start_idx;
drop index ldbc.hasTagForum_start_idx;
drop index ldbc.hasType_start_idx;
drop index ldbc.isLocatedIn_start_idx;
drop index ldbc.isLocatedInOrgan_start_idx;
drop index ldbc.isLocatedInPost_start_idx;
drop index ldbc.isLocatedInComment_start_idx;
drop index ldbc.isLocatedInPerson_start_idx;
drop index ldbc.isPartOf_start_idx;
drop index ldbc.isSubclassOf_start_idx;
drop index ldbc.knows_start_idx;
drop index ldbc.likes_start_idx;
drop index ldbc.likesPost_start_idx;
drop index ldbc.likesComment_start_idx;
drop index ldbc.replyOf_start_idx;
drop index ldbc.replyOfPost_start_idx;
drop index ldbc.replyOfComment_start_idx;
drop index ldbc.studyAt_start_idx;
drop index ldbc.workAt_start_idx;

drop index ldbc.containerOf_end_idx;
drop index ldbc.hasCreator_end_idx;
drop index ldbc.hasCreatorPost_end_idx;
drop index ldbc.hasCreatorComment_end_idx;
drop index ldbc.hasInterest_end_idx;
drop index ldbc.hasMember_end_idx;
drop index ldbc.hasModerator_end_idx;
drop index ldbc.hasTag_end_idx;
drop index ldbc.hasTagPost_end_idx;
drop index ldbc.hasTagComment_end_idx;
drop index ldbc.hasTagForum_end_idx;
drop index ldbc.hasType_end_idx;
drop index ldbc.isLocatedIn_end_idx;
drop index ldbc.isLocatedInOrgan_end_idx;
drop index ldbc.isLocatedInPost_end_idx;
drop index ldbc.isLocatedInComment_end_idx;
drop index ldbc.isLocatedInPerson_end_idx;
drop index ldbc.isPartOf_end_idx;
drop index ldbc.isSubclassOf_end_idx;
drop index ldbc.knows_end_idx;
drop index ldbc.likes_end_idx;
drop index ldbc.likesPost_end_idx;
drop index ldbc.likesComment_end_idx;
drop index ldbc.replyOf_end_idx;
drop index ldbc.replyOfPost_end_idx;
drop index ldbc.replyOfComment_end_idx;
drop index ldbc.studyAt_end_idx;
drop index ldbc.workAt_end_idx;

drop index ldbc.containerOf_id_idx;
drop index ldbc.hasCreator_id_idx;
drop index ldbc.hasCreatorPost_id_idx;
drop index ldbc.hasCreatorComment_id_idx;
drop index ldbc.hasInterest_id_idx;
drop index ldbc.hasMember_id_idx;
drop index ldbc.hasModerator_id_idx;
drop index ldbc.hasTag_id_idx;
drop index ldbc.hasTagPost_id_idx;
drop index ldbc.hasTagComment_id_idx;
drop index ldbc.hasTagForum_id_idx;
drop index ldbc.hasType_id_idx;
drop index ldbc.isLocatedIn_id_idx;
drop index ldbc.isLocatedInOrgan_id_idx;
drop index ldbc.isLocatedInPost_id_idx;
drop index ldbc.isLocatedInComment_id_idx;
drop index ldbc.isLocatedInPerson_id_idx;
drop index ldbc.isPartOf_id_idx;
drop index ldbc.isSubclassOf_id_idx;
drop index ldbc.knows_id_idx;
drop index ldbc.likes_id_idx;
drop index ldbc.likesPost_id_idx;
drop index ldbc.likesComment_id_idx;
drop index ldbc.replyOf_id_idx;
drop index ldbc.replyOfPost_id_idx;
drop index ldbc.replyOfComment_id_idx;
drop index ldbc.studyAt_id_idx;
drop index ldbc.workAt_id_idx;

drop index ldbc.containerOf_properties_idx;
drop index ldbc.hasCreator_properties_idx;
drop index ldbc.hasCreatorPost_properties_idx;
drop index ldbc.hasCreatorComment_properties_idx;
drop index ldbc.hasInterest_properties_idx;
drop index ldbc.hasMember_properties_idx;
drop index ldbc.hasModerator_properties_idx;
drop index ldbc.hasTag_properties_idx;
drop index ldbc.hasTagPost_properties_idx;
drop index ldbc.hasTagComment_properties_idx;
drop index ldbc.hasTagForum_properties_idx;
drop index ldbc.hasType_properties_idx;
drop index ldbc.isLocatedIn_properties_idx;
drop index ldbc.isLocatedInOrgan_properties_idx;
drop index ldbc.isLocatedInPost_properties_idx;
drop index ldbc.isLocatedInComment_properties_idx;
drop index ldbc.isLocatedInPerson_properties_idx;
drop index ldbc.isPartOf_properties_idx;
drop index ldbc.isSubclassOf_properties_idx;
drop index ldbc.knows_properties_idx;
drop index ldbc.likes_properties_idx;
drop index ldbc.likesPost_properties_idx;
drop index ldbc.likesComment_properties_idx;
drop index ldbc.replyOf_properties_idx;
drop index ldbc.replyOfPost_properties_idx;
drop index ldbc.replyOfComment_properties_idx;
drop index ldbc.studyAt_properties_idx;
drop index ldbc.workAt_properties_idx;

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
create (:Forum =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:Post =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:"Comment" =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:Organization =jsonb_strip_nulls(row_to_json(row)::jsonb));

-- Person
drop view person_view;

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

-- Person's email
\set file_name :source_path/person_email_emailaddress_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwEmail;
create foreign table fdwEmail
(
	id int8,
	email varchar(200)
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

-- Person's language
\set file_name :source_path/person_speaks_language_0_0.csv
\echo Start Loading :file_name

drop foreign table fdwLanguage;
create foreign table fdwLanguage
(
	id int8,
	language varchar(200)
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

create view person_view as (
    SELECT p.*, email, speaks
    FROM
        fdwPerson p LEFT OUTER JOIN
	(SELECT id, array_agg(email) email FROM fdwEmail GROUP BY id) e ON p.id = e.id LEFT OUTER JOIN
	(SELECT id, array_agg(language) speaks FROM fdwLanguage GROUP BY id) l ON p.id = l.id
);

load from person_view as row
create (:Person =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:Place =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:Tag =jsonb_strip_nulls(row_to_json(row)::jsonb));

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
create (:TagClass =jsonb_strip_nulls(row_to_json(row)::jsonb));

-- Make property indexes for 'id' field of vertexes
CREATE UNIQUE PROPERTY INDEX ON forum ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON message ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON post ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON "Comment" ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON organization ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON person ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON place ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON tag ((id::int8));
CREATE UNIQUE PROPERTY INDEX ON tagclass ((id::int8));

-- Analyze vertices
ANALYZE ldbc.forum;
ANALYZE ldbc.message;
ANALYZE ldbc.post;
ANALYZE ldbc."Comment";
ANALYZE ldbc.organization;
ANALYZE ldbc.person;
ANALYZE ldbc.place;
ANALYZE ldbc.tag;
ANALYZE ldbc.tagclass;

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
create (r)-[:hasCreatorPost]->(s);

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
create (r)-[:hasCreatorComment]->(s);

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
create (r)-[:hasTagPost]->(s);

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
create (r)-[:hasTagComment]->(s);

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
create (r)-[:hasTagForum]->(s);

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
create (r)-[:isLocatedInOrgan]->(s);

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
create (r)-[:isLocatedInPost]->(s);

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
create (r)-[:isLocatedInComment]->(s);

--- isLocatedIn (person)
\set file_name :source_path/person_isLocatedIn_place_0_0.csv
\echo Start Loading :file_name

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
create (r)-[:isLocatedInPerson]->(s);

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
create (r)-[:knows {'creationDate': (row).creationDate}]->(s)
create (s)-[:knows {'creationDate': (row).creationDate}]->(r);

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
create (r)-[:likesPost {'creationDate': (row).creationDate}]->(s);

--- likes (comment)
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
create (r)-[:likesComment {'creationDate': (row).creationDate}]->(s);

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
create (r)-[:replyOfPost]->(s);

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
create (r)-[:replyOfComment]->(s);

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

-- vertices
create unique index on ldbc.Forum (id);
create unique index on ldbc.Message (id);
create unique index on ldbc.Post (id);
create unique index on ldbc."Comment" (id);
create unique index on ldbc.Organization (id);
create unique index on ldbc.Person (id);
create unique index on ldbc.Place (id);
create unique index on ldbc.Tag (id);
create unique index on ldbc.TagClass (id);

-- edges
create index on ldbc.containerOf using ein(start, "end", id);
create index on ldbc.hasCreator using ein(start, "end", id);
create index on ldbc.hasCreatorPost using ein(start, "end", id);
create index on ldbc.hasCreatorComment using ein(start, "end", id);
create index on ldbc.hasInterest using ein(start, "end", id);
create index on ldbc.hasMember using ein(start, "end", id);
create index on ldbc.hasModerator using ein(start, "end", id);
create index on ldbc.hasTag using ein(start, "end", id);
create index on ldbc.hasTagPost using ein(start, "end", id);
create index on ldbc.hasTagComment using ein(start, "end", id);
create index on ldbc.hasTagForum using ein(start, "end", id);
create index on ldbc.hasType using ein(start, "end", id);
create index on ldbc.isLocatedIn using ein(start, "end", id);
create index on ldbc.isLocatedInOrgan using ein(start, "end", id);
create index on ldbc.isLocatedInPost using ein(start, "end", id);
create index on ldbc.isLocatedInComment using ein(start, "end", id);
create index on ldbc.isLocatedInPerson using ein(start, "end", id);
create index on ldbc.isPartOf using ein(start, "end", id);
create index on ldbc.isSubclassOf using ein(start, "end", id);
create index on ldbc.knows using ein(start, "end", id);
create index on ldbc.likes using ein(start, "end", id);
create index on ldbc.likesPost using ein(start, "end", id);
create index on ldbc.likesComment using ein(start, "end", id);
create index on ldbc.replyOf using ein(start, "end", id);
create index on ldbc.replyOfPost using ein(start, "end", id);
create index on ldbc.replyOfComment using ein(start, "end", id);
create index on ldbc.studyAt using ein(start, "end", id);
create index on ldbc.workAt using ein(start, "end", id);

create index on ldbc.containerOf using ein("end", start, id);
create index on ldbc.hasCreator using ein("end", start, id);
create index on ldbc.hasCreatorPost using ein("end", start, id);
create index on ldbc.hasCreatorComment using ein("end", start, id);
create index on ldbc.hasInterest using ein("end", start, id);
create index on ldbc.hasMember using ein("end", start, id);
create index on ldbc.hasModerator using ein("end", start, id);
create index on ldbc.hasTag using ein("end", start, id);
create index on ldbc.hasTagPost using ein("end", start, id);
create index on ldbc.hasTagComment using ein("end", start, id);
create index on ldbc.hasTagForum using ein("end", start, id);
create index on ldbc.hasType using ein("end", start, id);
create index on ldbc.isLocatedIn using ein("end", start, id);
create index on ldbc.isLocatedInOrgan using ein("end", start, id);
create index on ldbc.isLocatedInPost using ein("end", start, id);
create index on ldbc.isLocatedInComment using ein("end", start, id);
create index on ldbc.isLocatedInPerson using ein("end", start, id);
create index on ldbc.isPartOf using ein("end", start, id);
create index on ldbc.isSubclassOf using ein("end", start, id);
create index on ldbc.knows using ein("end", start, id);
create index on ldbc.likes using ein("end", start, id);
create index on ldbc.likesPost using ein("end", start, id);
create index on ldbc.likesComment using ein("end", start, id);
create index on ldbc.replyOf using ein("end", start, id);
create index on ldbc.replyOfPost using ein("end", start, id);
create index on ldbc.replyOfComment using ein("end", start, id);
create index on ldbc.studyAt using ein("end", start, id);
create index on ldbc.workAt using ein("end", start, id);

-- Property Index
CREATE INDEX ON ldbc.person USING gin (properties jsonb_path_ops);
CREATE INDEX ON ldbc.tag USING gin (properties jsonb_path_ops);
CREATE INDEX ON ldbc.place USING gin (properties jsonb_path_ops);

-- make indexes on message.creationDate, message.id
CREATE PROPERTY INDEX ON message ( (creationDate::int8) DESC, (id::int8) ASC );
CREATE PROPERTY INDEX ON post ( (creationDate::int8) DESC, (id::int8) ASC );
CREATE PROPERTY INDEX ON "Comment" ( (creationDate::int8) DESC, (id::int8) ASC );

-- Analyze vertices
VACUUM ANALYZE ldbc.forum;
VACUUM ANALYZE ldbc.message;
VACUUM ANALYZE ldbc.post;
VACUUM ANALYZE ldbc."Comment";
VACUUM ANALYZE ldbc.organization;
VACUUM ANALYZE ldbc.person;
VACUUM ANALYZE ldbc.place;
VACUUM ANALYZE ldbc.tag;
VACUUM ANALYZE ldbc.tagclass;

-- Analyze edges
VACUUM ANALYZE ldbc.containerOf;
VACUUM ANALYZE ldbc.hasCreator;
VACUUM ANALYZE ldbc.hasCreatorPost;
VACUUM ANALYZE ldbc.hasCreatorComment;
VACUUM ANALYZE ldbc.hasInterest;
VACUUM ANALYZE ldbc.hasMember;
VACUUM ANALYZE ldbc.hasModerator;
VACUUM ANALYZE ldbc.hasTag;
VACUUM ANALYZE ldbc.hasTagPost;
VACUUM ANALYZE ldbc.hasTagComment;
VACUUM ANALYZE ldbc.hasTagForum;
VACUUM ANALYZE ldbc.hasType;
VACUUM ANALYZE ldbc.isLocatedIn;
VACUUM ANALYZE ldbc.isLocatedInOrgan;
VACUUM ANALYZE ldbc.isLocatedInPost;
VACUUM ANALYZE ldbc.isLocatedInComment;
VACUUM ANALYZE ldbc.isLocatedInPerson;
VACUUM ANALYZE ldbc.isPartOf;
VACUUM ANALYZE ldbc.isSubclassOf;
VACUUM ANALYZE ldbc.knows;
VACUUM ANALYZE ldbc.likes;
VACUUM ANALYZE ldbc.likesPost;
VACUUM ANALYZE ldbc.likesComment;
VACUUM ANALYZE ldbc.replyOf;
VACUUM ANALYZE ldbc.replyOfPost;
VACUUM ANALYZE ldbc.replyOfComment;
VACUUM ANALYZE ldbc.studyAt;
VACUUM ANALYZE ldbc.workAt;

CREATE EXTENSION pg_prewarm;
SELECT count(*) FROM (SELECT PG_PREWARM(c.oid) from pg_class c left join pg_namespace n on n.oid = c.relnamespace where nspname = 'ldbc' and (relkind = 'i' OR relkind = 'r')) A;

-- pre_eval weights
DROP TABLE c14_weight;
CREATE UNLOGGED TABLE c14_weight(p1 int8, p2 int8, weight double precision);
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
			RETURN p1.id::int8 AS rep_creator, p2.id::int8 AS org_creator, 1.0 AS inc
            UNION ALL
            MATCH
            (p1:Person)<-[:hasCreatorComment]-(c)-[:replyOfComment]->(m)-[:hasCreatorComment]->(p2:Person)
			, (p1:Person)-[:knows]->(p2:Person)
			RETURN p1.id::int8 AS rep_creator, p2.id::int8 AS org_creator, 0.5
        ) AS x
    ) AS x
    GROUP BY p1, p2;
CREATE UNIQUE INDEX ON c14_weight(p1, p2);
ALTER TABLE c14_weight SET LOGGED;

-- Make logged
alter vlabel forum set logged;
alter vlabel message set logged;
alter vlabel post set logged;
alter vlabel "Comment" set logged;
alter vlabel Organization set logged;
alter vlabel Person set logged;
alter vlabel Place set logged;
alter vlabel Tag set logged;
alter vlabel TagClass set logged;
alter elabel containerOf set logged;
alter elabel hasCreator set logged;
alter elabel hasCreatorPost set logged;
alter elabel hasCreatorComment set logged;
alter elabel hasInterest set logged;
alter elabel hasMember set logged;
alter elabel hasModerator set logged;
alter elabel hasTag set logged;
alter elabel hasTagPost set logged;
alter elabel hasTagComment set logged;
alter elabel hasTagForum set logged;
alter elabel hasType set logged;
alter elabel isLocatedIn set logged;
alter elabel isLocatedInOrgan set logged;
alter elabel isLocatedInPost set logged;
alter elabel isLocatedInComment set logged;
alter elabel isLocatedInPerson set logged;
alter elabel isPartOf set logged;
alter elabel isSubclassOf set logged;
alter elabel knows set logged;
alter elabel likes set logged;
alter elabel likesPost set logged;
alter elabel likesComment set logged;
alter elabel replyOf set logged;
alter elabel replyOfPost set logged;
alter elabel replyOfComment set logged;
alter elabel studyAt set logged;
alter elabel workAt set logged;
