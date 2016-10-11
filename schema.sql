drop graph ldbc cascade;
create graph ldbc;
set graph_path = ldbc;
create vlabel Forum;
drop foreign table fdwForum;
create foreign table fdwForum 
	(
		id int8, 
		title varchar(80), 
		createDate timestamp(6) with time zone
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/forum_0_0.csv'
	);
load from fdwForum as row
create (:Forum =row_to_json(row)::jsonb);
-- Message
create vlabel Post;
drop foreign table fdwPost;
create foreign table fdwPost 
	(
		id int8, 
		imageFile varchar(80), 
		createDate timestamp(6) with time zone,
		locationIP varchar(80), 
		browserUsed varchar(80), 
		lanaguage varchar(80),
		content text,
		length int4
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/post_0_0.csv'
	);
load from fdwPost as row
create (:Post =row_to_json(row)::jsonb);
create vlabel "Comment";
drop foreign table fdwComment;
create foreign table fdwComment 
	(
		id int8, 
		createDate timestamp(6) with time zone,
		locationIP varchar(80), 
		browserUsed varchar(80), 
		content varchar(2000),
		length int4
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_0_0.csv'
	);
load from fdwComment as row
create (:"Comment" =row_to_json(row)::jsonb);
create vlabel Organization;
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
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/organisation_0_0.csv'
	);
load from fdwOrganization as row
create (:Organization =row_to_json(row)::jsonb);
create vlabel Person;
drop foreign table fdwPerson;
create foreign table fdwPerson 
	(
		id int8, 
		firstName varchar(80),
		lastName varchar(80),
		gender varchar(6),
		birthday date,
		createDate timestamp(6) with time zone,
		locationIP varchar(80), 
		browserUsed varchar(80) 
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_0_0.csv'
	);
load from fdwPerson as row
create (:Person =row_to_json(row)::jsonb);
create vlabel Place;
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
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/place_0_0.csv'
	);
load from fdwPlace as row
create (:Place =row_to_json(row)::jsonb);
create vlabel Tag;
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
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/tag_0_0.csv'
	);
load from fdwTag as row
create (:Tag =row_to_json(row)::jsonb);
create vlabel TagClass;
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
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/tagclass_0_0.csv'
	);
load from fdwTagClass as row
create (:TagClass =row_to_json(row)::jsonb);
-- Edge
--- containerOf
create elabel containerOf;
drop foreign table fdwContainerOf;
create foreign table fdwContainerOf 
	(
		forum_id int8, 
		post_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/forum_containerOf_post_0_0.csv'
	);
load from fdwContainerOf as row
match (r:Forum), (s:Post)
where (r).id::int8 = (row).forum_id and (s).id::int8 = (row).post_id
create (r)-[:containerOf]->(s);
--- hasCreator
create elabel hasCreator;
drop foreign table fdwPostHasCreator;
create foreign table fdwPostHasCreator
	(
		post_id int8, 
		person_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/post_hasCreator_person_0_0.csv'
	);
load from fdwPostHasCreator as row
match (r:Post), (s:Person)
where (r).id::int8 = (row).post_id and (s).id::int8 = (row).person_id
create (r)-[:hasCreator]->(s);
drop foreign table fdwCommentHasCreator;
create foreign table fdwCommentHasCreator
	(
		comment_id int8, 
		person_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_hasCreator_person_0_0.csv'
	);
load from fdwCommentHasCreator as row
match (r:"Comment"), (s:Person)
where (r).id::int8 = (row).comment_id and (s).id::int8 = (row).person_id
create (r)-[:hasCreator]->(s);
--- hasInterest
create elabel hasInterest;
drop foreign table fdwHasInterest;
create foreign table fdwHasInterest
	(
		person_id int8, 
		tag_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_hasInterest_tag_0_0.csv'
	);
load from fdwHasInterest as row
match (r:Person), (s:Tag)
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).tag_id
create (r)-[:hasInterest]->(s);
-- hasMember
create elabel hasMember;
drop foreign table fdwHasMember;
create foreign table fdwHasMember
	(
		forum_id int8, 
		person_id int8,
		joinDate timestamp(6) with time zone
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/forum_hasMember_person_0_0.csv'
	);
load from fdwHasMember as row
match (r:Forum), (s:Person)
where (r).id::int8 = (row).forum_id and (s).id::int8 = (row).person_id
create (r)-[:hasMember {'joinDate': (row).joinDate}]->(s);
--- hasModerator
create elabel hasModerator;
drop foreign table fdwHasModerator;
create foreign table fdwHasModerator
	(
		forum_id int8, 
		person_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/forum_hasModerator_person_0_0.csv'
	);
load from fdwHasModerator as row
match (r:Forum), (s:Person)
where (r).id::int8 = (row).forum_id and (s).id::int8 = (row).person_id
create (r)-[:hasModerator]->(s);
--- hasTag
create elabel hasTag;
drop foreign table fdwCommentHasTag;
create foreign table fdwCommentHasTag
	(
		comment_id int8, 
		tag_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_hasTag_tag_0_0.csv'
	);
load from fdwCommentHasTag as row
match (r:"Comment"), (s:Tag)
where (r).id::int8 = (row).comment_id and (s).id::int8 = (row).tag_id
create (r)-[:hasTag]->(s);
drop foreign table fdwForumHasTag;
create foreign table fdwForumHasTag
	(
		forum_id int8, 
		tag_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/forum_hasTag_tag_0_0.csv'
	);
load from fdwForumHasTag as row
match (r:Forum), (s:Tag)
where (r).id::int8 = (row).forum_id and (s).id::int8 = (row).tag_id
create (r)-[:hasTag]->(s);
--- hsaType
create elabel hasType;
drop foreign table fdwHasType;
create foreign table fdwHasType
	(
		tag_id int8, 
		tagclass_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/tag_hasType_tagclass_0_0.csv'
	);
load from fdwHasType as row
match (r:Tag), (s:TagClass)
where (r).id::int8 = (row).tag_id and (s).id::int8 = (row).tagclass_id
create (r)-[:hasType]->(s);
--- isLocatedIn
create elabel isLocatedIn;
drop foreign table fdwOrganIsLocatedIn;
create foreign table fdwOrganIsLocatedIn
	(
		organ_id int8, 
		place_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/organisation_isLocatedIn_place_0_0.csv'
	);
load from fdwOrganIsLocatedIn as row
match (r:Organization), (s:Place)
where (r).id::int8 = (row).organ_id and (s).id::int8 = (row).place_id
create (r)-[:isLocatedIn]->(s);
drop foreign table fdwPostIsLocatedIn;
create foreign table fdwPostIsLocatedIn
	(
		post_id int8, 
		place_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/post_isLocatedIn_place_0_0.csv'
	);
load from fdwPostIsLocatedIn as row
match (r:Post), (s:Place)
where (r).id::int8 = (row).post_id and (s).id::int8 = (row).place_id
create (r)-[:isLocatedIn]->(s);
drop foreign table fdwCommentIsLocatedIn;
create foreign table fdwCommentIsLocatedIn
	(
		comment_id int8, 
		place_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_isLocatedIn_place_0_0.csv'
	);
load from fdwCommentIsLocatedIn as row
match (r:"Comment"), (s:Place)
where (r).id::int8 = (row).comment_id and (s).id::int8 = (row).place_id
create (r)-[:isLocatedIn]->(s);
drop foreign table fdwPersonIsLocatedIn;
create foreign table fdwPersonIsLocatedIn
	(
		person_id int8, 
		place_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_isLocatedIn_place_0_0.csv'
	);
load from fdwPersonIsLocatedIn as row
match (r:Person), (s:Place)
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).place_id
create (r)-[:isLocatedIn]->(s);
--- isPartOf
create elabel isPartOf;
drop foreign table fdwIsPartOf;
create foreign table fdwIsPartOf
	(
		place1_id int8, 
		place2_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/place_isPartOf_place_0_0.csv'
	);
load from fdwIsPartOf as row
match (r:Place), (s:Place)
where (r).id::int8 = (row).place1_id and (s).id::int8 = (row).place2_id
create (r)-[:isPartOf]->(s);
--- isSubclassOf
create elabel isSubclassOf;
drop foreign table fdwIsSubclassOf;
create foreign table fdwIsSubclassOf
	(
		tagclass1_id int8, 
		tagclass2_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/tagclass_isSubclassOf_tagclass_0_0.csv'
	);
load from fdwIsSubclassOf as row
match (r:TagClass), (s:TagClass)
where (r).id::int8 = (row).tagclass1_id and (s).id::int8 = (row).tagclass2_id
create (r)-[:isSubclassOf]->(s);
--- knows
create elabel knows;
drop foreign table fdwKnows;
create foreign table fdwKnows
	(
		person1_id int8, 
		person2_id int8,
		creationDate timestamp(6) with time zone
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_knows_person_0_0.csv'
	);
load from fdwKnows as row
match (r:Person), (s:Person)
where (r).id::int8 = (row).person1_id and (s).id::int8 = (row).person2_id
create (r)-[:knows {'creationDate': (row).creationDate}]->(s);
--- likes
create elabel likes;
drop foreign table fdwLikesPost;
create foreign table fdwLikesPost
	(
		person_id int8, 
		post_id int8,
		creationDate timestamp(6) with time zone
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_likes_post_0_0.csv'
	);
load from fdwLikesPost as row
match (r:Person), (s:Post)
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).post_id
create (r)-[:likes {'creationDate': (row).creationDate}]->(s);
drop foreign table fdwLikesComment;
create foreign table fdwLikesComment
	(
		person_id int8, 
		comment_id int8,
		creationDate timestamp(6) with time zone
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_likes_comment_0_0.csv'
	);
load from fdwLikesComment as row
match (r:Person), (s:"Comment")
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).comment_id
create (r)-[:likes {'creationDate': (row).creationDate}]->(s);
--- replyOf
create elabel replyOf;
drop foreign table fdwReplyOfPost;
create foreign table fdwReplyOfPost
	(
		comment_id int8, 
		post_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_replyOf_post_0_0.csv'
	);
load from fdwReplyOfPost as row
match (r:"Comment"), (s:Post)
where (r).id::int8 = (row).comment_id and (s).id::int8 = (row).post_id
create (r)-[:replyOf]->(s);
drop foreign table fdwReplyOfComment;
create foreign table fdwReplyOfComment
	(
		comment1_id int8, 
		comment2_id int8
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/comment_replyOf_comment_0_0.csv'
	);
load from fdwReplyOfComment as row
match (r:"Comment"), (s:"Comment")
where (r).id::int8 = (row).comment1_id and (s).id::int8 = (row).comment2_id
create (r)-[:replyOf]->(s);
--- studyAt
create elabel studyAt;
drop foreign table fdwStudyAt;
create foreign table fdwStudyAt
	(
		person_id int8, 
		organ_id int8,
		classYear char(4)
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_studyAt_organisation_0_0.csv'
	);
load from fdwStudyAt as row
match (r:Person), (s:Organization)
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).organ_id
create (r)-[:studyAt {'classYear': (row).classYear}]->(s);
--- workAt
create elabel workAt;
drop foreign table fdwWorkAt;
create foreign table fdwWorkAt
	(
		person_id int8, 
		organ_id int8,
		workFrom char(4)
	)
	server graph_import
	options 
	(
		 DELIMITER '|',
		 FILENAME '/home/ktlee/tools/ldbc_snb_datagen/social_network/person_workAt_organisation_0_0.csv'
	);
load from fdwWorkAt as row
match (r:Person), (s:Organization)
where (r).id::int8 = (row).person_id and (s).id::int8 = (row).organ_id
create (r)-[:workAt {'workFrom': (row).workFrom}]->(s);
