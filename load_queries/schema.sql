\timing on

DROP GRAPH ldbc CASCADE;
CREATE GRAPH ldbc;
SET GRAPH_PATH = ldbc;
ALTER DATABASE :target_db SET graph_path = ldbc ;

-- Make Vertex Labels
CREATE VLABEL Forum DISABLE INDEX;
CREATE VLABEL Message DISABLE INDEX;
CREATE VLABEL Post DISABLE INDEX INHERITS (Message);
CREATE VLABEL "Comment" DISABLE INDEX INHERITS (Message);
CREATE VLABEL Organization DISABLE INDEX;
CREATE VLABEL Person DISABLE INDEX;
CREATE VLABEL Place DISABLE INDEX;
CREATE VLABEL Tag DISABLE INDEX;
CREATE VLABEL TagClass DISABLE INDEX;

-- Make Edge Labels
CREATE ELABEL containerOf DISABLE INDEX;
CREATE ELABEL hasCreator DISABLE INDEX;
CREATE ELABEL hasCreatorPost DISABLE INDEX INHERITS (hasCreator);
CREATE ELABEL hasCreatorComment DISABLE INDEX INHERITS (hasCreator);
CREATE ELABEL hasInterest DISABLE INDEX;
CREATE ELABEL hasMember DISABLE INDEX;
CREATE ELABEL hasModerator DISABLE INDEX;
CREATE ELABEL hasTag DISABLE INDEX;
CREATE ELABEL hasTagPost DISABLE INDEX INHERITS (hasTag);
CREATE ELABEL hasTagComment DISABLE INDEX INHERITS (hasTag);
CREATE ELABEL hasTagForum DISABLE INDEX INHERITS (hasTag);
CREATE ELABEL hasType DISABLE INDEX;
CREATE ELABEL isLocatedIn DISABLE INDEX;
CREATE ELABEL isLocatedInOrgan DISABLE INDEX INHERITS (isLocatedIn);
CREATE ELABEL isLocatedInPerson DISABLE INDEX INHERITS (isLocatedIn);
CREATE ELABEL isLocatedInMsg DISABLE INDEX INHERITS (isLocatedIn);
CREATE ELABEL isLocatedInPost DISABLE INDEX INHERITS (isLocatedInMsg);
CREATE ELABEL isLocatedInComment DISABLE INDEX INHERITS (isLocatedInMsg);
CREATE ELABEL isPartOf DISABLE INDEX;
CREATE ELABEL isSubclassOf DISABLE INDEX;
CREATE ELABEL knows DISABLE INDEX;
CREATE ELABEL likes DISABLE INDEX;
CREATE ELABEL likesPost DISABLE INDEX INHERITS (likes);
CREATE ELABEL likesComment DISABLE INDEX INHERITS (likes);
CREATE ELABEL replyOf DISABLE INDEX;
CREATE ELABEL replyOfPost DISABLE INDEX INHERITS (replyOf);
CREATE ELABEL replyOfComment DISABLE INDEX INHERITS (replyOf);
CREATE ELABEL studyAt DISABLE INDEX;
CREATE ELABEL workAt DISABLE INDEX;

-- Make Unlogged
ALTER VLABEL forum SET UNLOGGED;
ALTER VLABEL message SET UNLOGGED;
ALTER VLABEL post SET UNLOGGED;
ALTER VLABEL "Comment" SET UNLOGGED;
ALTER VLABEL Organization SET UNLOGGED;
ALTER VLABEL Person SET UNLOGGED;
ALTER VLABEL Place SET UNLOGGED;
ALTER VLABEL Tag SET UNLOGGED;
ALTER VLABEL TagClass SET UNLOGGED;
ALTER ELABEL containerOf SET UNLOGGED;
ALTER ELABEL hasCreator SET UNLOGGED;
ALTER ELABEL hasCreatorPost SET UNLOGGED;
ALTER ELABEL hasCreatorComment SET UNLOGGED;
ALTER ELABEL hasInterest SET UNLOGGED;
ALTER ELABEL hasMember SET UNLOGGED;
ALTER ELABEL hasModerator SET UNLOGGED;
ALTER ELABEL hasTag SET UNLOGGED;
ALTER ELABEL hasTagPost SET UNLOGGED;
ALTER ELABEL hasTagComment SET UNLOGGED;
ALTER ELABEL hasTagForum SET UNLOGGED;
ALTER ELABEL hasType SET UNLOGGED;
ALTER ELABEL isLocatedIn SET UNLOGGED;
ALTER ELABEL isLocatedInOrgan SET UNLOGGED;
ALTER ELABEL isLocatedInPost SET UNLOGGED;
ALTER ELABEL isLocatedInComment SET UNLOGGED;
ALTER ELABEL isLocatedInPerson SET UNLOGGED;
ALTER ELABEL isPartOf SET UNLOGGED;
ALTER ELABEL isSubclassOf SET UNLOGGED;
ALTER ELABEL knows SET UNLOGGED;
ALTER ELABEL likes SET UNLOGGED;
ALTER ELABEL likesPost SET UNLOGGED;
ALTER ELABEL likesComment SET UNLOGGED;
ALTER ELABEL replyOf SET UNLOGGED;
ALTER ELABEL replyOfPost SET UNLOGGED;
ALTER ELABEL replyOfComment SET UNLOGGED;
ALTER ELABEL studyAt SET UNLOGGED;
ALTER ELABEL workAt SET UNLOGGED;

-- Make Foreign Tables for CSV files

-- Forum
\set file_name :source_path/forum_0_0.csv
DROP FOREIGN TABLE IF EXISTS fdwForum CASCADE;
DROP VIEW viewForum;
CREATE FOREIGN TABLE fdwForum
(
	id INT8,
	title VARCHAR(256),
	creationDate TIMESTAMPTZ
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewForum AS (
SELECT 
	id,
	title,
	EXTRACT(EPOCH FROM creationDate) * 1000 AS creationDate
FROM fdwForum
);

-- Organization
\set file_name :source_path/organisation_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwOrganization CASCADE;
CREATE FOREIGN TABLE fdwOrganization
(
	id INT8,
	type VARCHAR(80),
	name VARCHAR(200),
	url VARCHAR(200)
)
SERVER graph_import
OPTIONS
(
	FORMAT 'csv',
	HEADER 'true',
	DELIMITER '|',
	NULL '',
	FILENAME :'file_name'
);

-- Person
\set file_name :source_path/person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPerson CASCADE;
CREATE FOREIGN TABLE fdwPerson
(
	id INT8,
	firstName VARCHAR(80),
	lastName VARCHAR(80),
	gender VARCHAR(6),
	birthday TIMESTAMPTZ,
	creationDate TIMESTAMPTZ,
	locationIP VARCHAR(80),
	browserUsed VARCHAR(80)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- Person's email
\set file_name :source_path/person_email_emailaddress_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwEmail CASCADE;
CREATE FOREIGN TABLE fdwEmail
(
	id INT8,
	email VARCHAR(200)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- Person's language
\set file_name :source_path/person_speaks_language_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwLanguage;
DROP VIEW viewPerson;
CREATE FOREIGN TABLE fdwLanguage
(
	id INT8,
	language VARCHAR(200)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

CREATE VIEW viewPerson AS (
    SELECT 
		p.id AS id,
		firstName,
		lastName,
		gender,
		EXTRACT(EPOCH FROM birthday) * 1000 as birthday,
		EXTRACT(EPOCH FROM creationDate) * 1000 as creationDate,
		locationIP,
		browserUsed,
        email, speaks
    FROM
        fdwPerson p 
	LEFT OUTER JOIN
	    (SELECT id, ARRAY_AGG(email) email FROM fdwEmail GROUP BY id) e 
	ON p.id = e.id 
	LEFT OUTER JOIN
	    (SELECT id, ARRAY_AGG(language) speaks FROM fdwLanguage GROUP BY id) l 
	ON p.id = l.id
);

-- Place
\set file_name :source_path/place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPlace CASCADE;
CREATE FOREIGN TABLE fdwPlace
(
	id INT8,
	name VARCHAR(200),
	url VARCHAR(200),
	type VARCHAR(80)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- Tag
\set file_name :source_path/tag_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwTag CASCADE;
CREATE FOREIGN TABLE fdwTag
(
	id INT8,
	name VARCHAR(200),
	url VARCHAR(200)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- TagClass
\set file_name :source_path/tagclass_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwTagClass CASCADE;
CREATE FOREIGN TABLE fdwTagClass
(
	id INT8,
	name VARCHAR(200),
	url VARCHAR(200)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- Message
--- Post (inherits Message)
\set file_name :source_path/post_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPost CASCADE;
DROP VIEW viewPost;
CREATE FOREIGN TABLE fdwPost
(
	id INT8,
	imageFile VARCHAR(80),
	creationDate TIMESTAMPTZ,
	locationIP VARCHAR(80),
	browserUsed VARCHAR(80),
	lanaguage VARCHAR(80),
	content text,
	length int4
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewPost AS (
SELECT
	id,
	imageFile,
	EXTRACT(EPOCH FROM creationDate) * 1000 as creationDate,
	locationIP,
	browserUsed,
	lanaguage,
	content,
	length
FROM fdwPost);

--- Comment (inherits Message)
\set file_name :source_path/comment_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwComment CASCADE;
DROP VIEW viewComment;
CREATE FOREIGN TABLE fdwComment
(
	id INT8,
	creationDate TIMESTAMPTZ,
	locationIP VARCHAR(80),
	browserUsed VARCHAR(80),
	content VARCHAR(2000),
	length int4
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewComment AS (
SELECT
	id,
	EXTRACT(EPOCH FROM creationDate) * 1000 as creationDate,
	locationIP,
	browserUsed,
	content,
	length
FROM fdwComment);

-- Edge
--- containerOf
\set file_name :source_path/forum_containerOf_post_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwContainerOf CASCADE;
CREATE FOREIGN TABLE fdwContainerOf
(
	forumId INT8,
	postId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasCreator (for Post)
\set file_name :source_path/post_hasCreator_person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPostHasCreator CASCADE;
CREATE FOREIGN TABLE fdwPostHasCreator
(
	postId INT8,
	personId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasCreator (for comment)
\set file_name :source_path/comment_hasCreator_person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwCommentHasCreator CASCADE;
CREATE FOREIGN TABLE fdwCommentHasCreator
(
	commentId INT8,
	personId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasInterest
\set file_name :source_path/person_hasInterest_tag_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwHasInterest CASCADE;
CREATE FOREIGN TABLE fdwHasInterest
(
	personId INT8,
	tagId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

-- hasMember
\set file_name :source_path/forum_hasMember_person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwHasMember CASCADE;
DROP VIEW viewHasMember;
CREATE FOREIGN TABLE fdwHasMember
(
	forumId INT8,
	personId INT8,
	joinDate TIMESTAMPTZ
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewHasMember AS (
SELECT
	forumId,
	personId,
	EXTRACT(EPOCH FROM joinDate) * 1000 AS joinDate
FROM fdwHasMember);

--- hasModerator
\set file_name :source_path/forum_hasModerator_person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwHasModerator CASCADE;
CREATE FOREIGN TABLE fdwHasModerator
(
	forumId INT8,
	personId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasTag (post)
\set file_name :source_path/post_hasTag_tag_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPostHasTag CASCADE;
CREATE FOREIGN TABLE fdwPostHasTag
(
	postId INT8,
	tagId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasTag (comment)
\set file_name :source_path/comment_hasTag_tag_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwCommentHasTag CASCADE;
CREATE FOREIGN TABLE fdwCommentHasTag
(
	commentId INT8,
	tagId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasTag (forum)
\set file_name :source_path/forum_hasTag_tag_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwForumHasTag CASCADE;
CREATE FOREIGN TABLE fdwForumHasTag
(
	forumId INT8,
	tagId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- hasType
\set file_name :source_path/tag_hasType_tagclass_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwHasType CASCADE;
CREATE FOREIGN TABLE fdwHasType
(
	tagId INT8,
	tagclassId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- isLocatedIn (organisation)
\set file_name :source_path/organisation_isLocatedIn_place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwOrganIsLocatedIn CASCADE;
CREATE FOREIGN TABLE fdwOrganIsLocatedIn
(
	organId INT8,
	placeId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- isLocatedIn (post)
\set file_name :source_path/post_isLocatedIn_place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPostIsLocatedIn CASCADE;
CREATE FOREIGN TABLE fdwPostIsLocatedIn
(
	postId INT8,
	placeId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- isLocatedIn (comment)
\set file_name :source_path/comment_isLocatedIn_place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwCommentIsLocatedIn CASCADE;
CREATE FOREIGN TABLE fdwCommentIsLocatedIn
(
	commentId INT8,
	placeId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);


--- isLocatedIn (person)
\set file_name :source_path/person_isLocatedIn_place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwPersonIsLocatedIn CASCADE;
CREATE FOREIGN TABLE fdwPersonIsLocatedIn
(
	personId INT8,
	placeId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- isPartOf
\set file_name :source_path/place_isPartOf_place_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwIsPartOf CASCADE;
CREATE FOREIGN TABLE fdwIsPartOf
(
	place1Id INT8,
	place2Id INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- isSubclassOf
\set file_name :source_path/tagclass_isSubclassOf_tagclass_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwIsSubclassOf CASCADE;
CREATE FOREIGN TABLE fdwIsSubclassOf
(
	tagclass1Id INT8,
	tagclass2Id INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- knows
\set file_name :source_path/person_knows_person_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwKnows CASCADE;
DROP VIEW viewKnows;
CREATE FOREIGN TABLE fdwKnows
(
	person1Id INT8,
	person2Id INT8,
	creationDate TIMESTAMPTZ 
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewKnows AS (
SELECT
	person1Id,
	person2Id,
	EXTRACT(EPOCH FROM creationDate) * 1000 AS creationDate
FROM
	fdwKnows);

--- likes (post)
\set file_name :source_path/person_likes_post_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwLikesPost CASCADE;
DROP VIEW viewLikesPost;
CREATE FOREIGN TABLE fdwLikesPost
(
	personId INT8,
	postId INT8,
	creationDate TIMESTAMPTZ 
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewLikesPost AS (
SELECT
	personId,
	postId,
	EXTRACT(EPOCH FROM creationDate) * 1000 AS creationDate
FROM
	fdwLikesPost);

--- likes (comment)
\set file_name :source_path/person_likes_comment_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwLikesComment CASCADE;
DROP VIEW viewLikesComment;
CREATE FOREIGN TABLE fdwLikesComment
(
	personId INT8,
	commentId INT8,
	creationDate TIMESTAMPTZ 
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);
CREATE VIEW viewLikesComment AS (
SELECT
	personId,
	commentId,
	EXTRACT(EPOCH FROM creationDate) * 1000 AS creationDate
FROM
	fdwLikesComment);

--- replyOf (post)
\set file_name :source_path/comment_replyOf_post_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwReplyOfPost CASCADE;
CREATE FOREIGN TABLE fdwReplyOfPost
(
	commentId INT8,
	postId INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- replyOf (comment)
\set file_name :source_path/comment_replyOf_comment_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwReplyOfComment CASCADE;
CREATE FOREIGN TABLE fdwReplyOfComment
(
	comment1Id INT8,
	comment2Id INT8
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- studyAt
\set file_name :source_path/person_studyAt_organisation_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwStudyAt CASCADE;
CREATE FOREIGN TABLE fdwStudyAt
(
	personId INT8,
	organId INT8,
	classYear CHAR(4)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

--- workAt
\set file_name :source_path/person_workAt_organisation_0_0.csv

DROP FOREIGN TABLE IF EXISTS fdwWorkAt CASCADE;
CREATE FOREIGN TABLE fdwWorkAt
(
	personId INT8,
	organId INT8,
	workFrom CHAR(4)
)
SERVER graph_import
OPTIONS
(
	 FORMAT 'csv',
	 HEADER 'true',
	 DELIMITER '|',
	 NULL '',
	 FILENAME :'file_name'
);

