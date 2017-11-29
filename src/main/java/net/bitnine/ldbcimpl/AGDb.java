package net.bitnine.ldbcimpl;

import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import net.bitnine.agensgraph.util.Jsonb;
import net.bitnine.agensgraph.util.JsonbObjectBuilder;
import net.bitnine.agensgraph.util.JsonbUtil;
import net.bitnine.ldbcimpl.excpetions.AGClientException;
import net.bitnine.ldbcimpl.util.DateUtils;
import net.bitnine.ldbcimpl.util.JsonArrayUtils;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AGDb extends Db {

    private DbConnectionState connectionState = null;

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    @Override
    protected  void onClose() throws IOException {
        connectionState.close();
    }

    @Override
    protected void onInit(Map<String, String> properties,
                          LoggingService loggingService) throws DbException {
        connectionState = new AGDbConnectionState(properties);
        registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
    }

    public static class LdbcQuery1Handler
            implements OperationHandler<LdbcQuery1, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery1 ldbcQuery1,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState) dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery1.toString() +" \n" + 
                    "MATCH (p:Person)-[path:knows*1..3]->(friend:Person {'firstname': '" + ldbcQuery1.firstName() + "'}) " +
                    "WHERE p.id = " + ldbcQuery1.personId() + " " + 
                    "WITH friend, min(length(path)) AS distance " +
                    "ORDER BY distance ASC, friend.lastName ASC, friend.id ASC " +
                    "LIMIT " + ldbcQuery1.limit() + " " + 
                    "MATCH (friend)-[:isLocatedInPerson]->(friendCity:Place) " +
                    "OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organization)-[:isLocatedInOrgan]->(uniCity:Place) " +
                    "WITH " +
                    "  friend, " +
                    "  collect( " +
                    "    CASE uni.name is null " +
                    "      WHEN true THEN 'null' " +
                    "      ELSE [uni.name, studyAt.\"classYear\", uniCity.name] " +
                    "    END " +
                    "  ) AS unis, " +
                    "  friendCity, " +
                    "  distance " +
                    "OPTIONAL MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedInOrgan]->(companyCountry:Place) " +
                    "WITH " +
                    "  friend, " +
                    "  collect( " +
                    "    CASE company.name is null " +
                    "      WHEN true THEN 'null' " +
                    "      ELSE [company.name, worksAt.\"workFrom\", companyCountry.name] " +
                    "    END " +
                    "  ) AS companies, " +
                    "  unis, " +
                    "  friendCity, " +
                    "  distance " +
                    "RETURN " +
                    "  friend.id AS id, " +
                    "  friend.lastName AS lastName, " +
                    "  distance, " +
                    "  friend.birthday AS birthday, " +
                    "  friend.creationDate AS creationDate, " +
                    "  friend.gender AS gender, " +
                    "  friend.browserUsed AS browser, " +
                    "  friend.locationIp AS locationIp, " +
                    "  friend.email AS emails, " +
                    "  friend.speaks AS languages, " +
                    "  friendCity.name AS cityName, " +
                    "  unis, " +
                    "  companies " +
                    "ORDER BY distance ASC, lastName ASC, id ASC ";
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery1Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    Object obj = rs.getObject(9);
                    List<String> emails = new ArrayList<>();
                    if (obj != null)
                        emails = JsonArrayUtils.toStringList(((Jsonb)obj).getArray());
                    obj = rs.getObject(10);
                    List<String> languages = new ArrayList<>();
                    if (obj != null)
                        languages = JsonArrayUtils.toStringList(((Jsonb)obj).getArray());
                    List<List<Object>> universities = JsonArrayUtils.toListofList((Jsonb)(rs.getObject(12)));
                    List<List<Object>> companies = JsonArrayUtils.toListofList((Jsonb)(rs.getObject(13)));
                    resultList.add(new LdbcQuery1Result(
                            rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getLong(4),
                            rs.getLong(5), rs.getString(6), rs.getString(7), rs.getString(8),
                            emails, languages, rs.getString(11), universities, companies
                    ));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery1);
        }
    }

    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 ldbcQuery2,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = 
                    "-- " + ldbcQuery2.toString() +" \n" + 
                    "MATCH (p:Person)-[:knows]->(friend:Person)<-[:hasCreator]-(message:Message) " +
                    "WHERE p.id = " + ldbcQuery2.personId() + " AND message.creationDate <= " + ldbcQuery2.maxDate().getTime() + " " +
		    		"WITH friend, message " +
		    		"ORDER BY message.creationDate DESC, message.id ASC LIMIT " + ldbcQuery2.limit() + " " +
                    "RETURN " +
                    "  friend.id AS personId, " +
                    "  friend.firstName AS personFirstName, " +
                    "  friend.lastName AS personLastName, " +
                    "  message.id AS messageId, " +
                    "  COALESCE(message.content, message.imageFile), " +
                    "  message.creationDate AS messageCreationDate";

            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery2Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery2Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getLong(4), rs.getString(5), rs.getLong(6)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery2);
        }
    }

    public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery3 ldbcQuery3,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

	    	java.util.Date endDate = DateUtils.endDate(ldbcQuery3.startDate(), ldbcQuery3.durationDays());
            String stmt = 
              "-- " + ldbcQuery3.toString() +" \n" + 
	          "MATCH (person:Person)-[:knows*1..2]->(friend) " + 
		      "WHERE person.id = " + ldbcQuery3.personId() + " AND id(person) != id(friend) " +
		      "WITH DISTINCT friend " +
	          "MATCH (friend)<-[:hasCreator]-(messageX:Message)-[:isLocatedInMsg]->(countryX:Place {name: '" + ldbcQuery3.countryXName() + "'}) " +
		      "WHERE not exists ((friend)-[:isLocatedInPerson]->()-[:isPartOf]->(countryX)) " +
		      "  AND messageX.creationDate >= " + ldbcQuery3.startDate().getTime() + " " +
		      "  AND messageX.creationDate < " + endDate.getTime() + " " +
		      "WITH friend, count(DISTINCT messageX) AS xCount " +
		      "MATCH (friend)<-[:hasCreator]-(messageY:Message)-[:isLocatedInMsg]->(countryY:Place {name: '" + ldbcQuery3.countryYName() + "'}) " +
		      "WHERE not exists ((friend)-[:isLocatedInPerson]->()-[:isPartOf]->(countryY)) " +
		      "  AND messageY.creationDate >= " + ldbcQuery3.startDate().getTime() + " " +
		      "  AND messageY.creationDate < " + endDate.getTime() + " " +
		      "WITH " +
		      "  friend, xCount, count(DISTINCT messageY) AS yCount " +
		      "RETURN " +
		      "  friend.id AS friendId, " +
		      "  friend.firstName AS friendFirstName, " +
		      "  friend.lastName AS friendLastName, " +
		      "  xCount, " +
		      "  yCount, " +
		      "  xCount + yCount AS xyCount " +
		      "ORDER BY xyCount DESC, friendId ASC " +
		      "LIMIT " + ldbcQuery3.limit();
	    	ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery3Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery3Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getLong(4), rs.getLong(5), rs.getLong(6)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery3);
        }
    }

    public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery4 ldbcQuery4,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            java.util.Date endDate = DateUtils.endDate(ldbcQuery4.startDate(), ldbcQuery4.durationDays());
            String stmt = "-- " + ldbcQuery4.toString() +" \n" + 
                          "SELECT tagname, count(distinct postid) AS postcount FROM ( " +
                          "  SELECT T1.tag AS tagname, T1.postid AS postid, count(distinct T2.postid) AS oldPostCount " +
                          "  FROM " +
                          "  ( " +
                          "    MATCH (person:Person)-[:KNOWS]->()<-[:hasCreatorPost]-(post:Post)-[:hasTagPost]->(tag:Tag) " +
                          "    WHERE " +
                          "    person.id = " + ldbcQuery4.personId() + 
						  "    AND post.creationDate >= " + ldbcQuery4.startDate().getTime() + " AND post.creationDate < " + endDate.getTime() + 
                          "    RETURN " +
                          "    post.id AS postid, tag.name AS tag " +
                          "  ) T1 " +
                          "  LEFT JOIN " +
                          "  ( " +
                          "    MATCH (person:Person)-[:KNOWS]->()<-[:hasCreatorPost]-(post:Post)-[:hasTagPost]->(tag:Tag) " +
                          "    WHERE " +
                          "    person.id = " + ldbcQuery4.personId() + 
						  "    AND post.creationDate < " + ldbcQuery4.startDate().getTime() + 
                          "    RETURN " +
                          "    post.id AS postid, tag.name AS tag " +
                          "  ) T2 " +
                          "  ON T1.tag = T2.tag " +
                          "  GROUP BY T1.tag, T1.postid " +
                          ") A " +
                          "WHERE oldPostCount = 0 GROUP BY tagname " +
                          "ORDER BY postcount DESC, tagname ASC " +
                          "LIMIT " + ldbcQuery4.limit();
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery4Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery4Result(rs.getString(1), rs.getInt(2)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery4);
        }
    }

    public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery5 ldbcQuery5,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery5.toString() +" \n" + 
                    "MATCH (person:Person)-[:knows*1..2]->(friend) " +
                    "WHERE person.id = " + ldbcQuery5.personId() + " AND id(person) != id(friend) " +
		    		"WITH DISTINCT friend " +
		    		"MATCH (friend)<-[membership:hasMember]-(forum:Forum) " +
                    "WHERE membership.\"joinDate\" > " + ldbcQuery5.minDate().getTime() + " " +
                    "OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post:Post)<-[:containerOf]-(forum) " +
		    		"WITH forum.id AS forumid, forum.title AS forumTitle, count(id(post)) AS postcount " +
		    		"ORDER BY postCount DESC, forumid ASC " +
		    		"RETURN forumTitle, postCount " +
                    "LIMIT " + ldbcQuery5.limit();

            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery5Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery5Result(rs.getString(1), rs.getInt(2)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery5);
        }
    }

    public static class LdbcQuery6Handler implements
            OperationHandler<LdbcQuery6, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery6 ldbcQuery6,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery6.toString() +" \n" + 
                    "MATCH (person:Person)-[:knows*1..2]->(friend) " +
                    "WHERE person.id = " + ldbcQuery6.personId() + " AND id(person) != id(friend) " +
		    		"WITH DISTINCT friend " +
                    "MATCH (friend)<-[:hascreatorpost]-(friendPost) " +
		    		"MATCH (friendPost)-[:hasTagPost]->(:Tag {name: '" + ldbcQuery6.tagName() + "'}) " +
		    		"MATCH (friendPost)-[:hasTagPost]->(commonTag:Tag) " +
                    "WHERE commonTag.name <> '" + ldbcQuery6.tagName() + "' " +
                    "RETURN " +
                    "  commonTag.name AS tagName, " +
                    "  count(distinct id(friendPost)) AS postCount " +
                    "ORDER BY postCount DESC, tagName ASC " +
                    "LIMIT " + ldbcQuery6.limit();
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery6Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery6Result(rs.getString(1), rs.getInt(2)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery6);
        }
    }

    public static class LdbcQuery7Handler implements
            OperationHandler<LdbcQuery7, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery7 ldbcQuery7, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery7.toString() +" \n" + 
              "MATCH (person:Person)<-[:hasCreator]-(message:Message)<-[l:likes]-(liker:Person) " +
              "WHERE person.id = " + ldbcQuery7.personId() + " " +
              "WITH DISTINCT id(liker) AS liker_id, liker, message, l.\"creationDate\" AS likeTime, person " +
              "ORDER BY liker_id, likeTime DESC " +
              "RETURN " +
              "  liker.Id AS personId, " +
              "  liker.FirstName, " +
              "  liker.LastName, " +
              "  likeTime, " +
              "  message.id AS messageId, " +
              "  COALESCE(message.content, message.imagefile) AS messageContent, " +
              "  (likeTime - message.creationdate) / (1000 * 60) AS latency, " +
              "  not exists((person)-[:knows]->(liker)) " +
              "ORDER BY likeTime DESC, personId ASC " +
              "LIMIT " + ldbcQuery7.limit();
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery7Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery7Result(rs.getLong(1), rs.getString(2),
                            rs.getString(3), rs.getLong(4), rs.getLong(5), rs.getString(6),
                            rs.getInt(7), rs.getBoolean(8)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery7);
        }
    }

    public static class LdbcQuery8Handler implements
            OperationHandler<LdbcQuery8, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery8 ldbcQuery8,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery8.toString() +" \n" + 
                    "MATCH (p:Person)<-[:hasCreator]-()<-[:replyOf]-(c:\"Comment\")-[:hasCreatorComment]->(person:Person) " +
                    "WHERE p.id = " + ldbcQuery8.personId() + " " +
				    "WITH person, c " +
		    		"ORDER BY c.creationDate DESC, c.id ASC LIMIT " + ldbcQuery8.limit() + " " +
                    "RETURN " +
                    "  person.id AS personId, " +
                    "  person.firstName AS personFirstName, " +
                    "  person.lastName AS personLastName, " +
                    "  c.creationDate AS commentCreationDate, " +
                    "  c.id AS commentId, " +
                    "  c.content AS commentContent";
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery8Result> resultList = new ArrayList<>();
            try {
                while (rs.next())
                    resultList.add(new LdbcQuery8Result(rs.getLong(1), rs.getString(2),
                            rs.getString(3), rs.getLong(4), rs.getLong(5), rs.getString(6)));
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery8);
        }
    }

    public static class LdbcQuery9Handler implements
            OperationHandler<LdbcQuery9, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery9 ldbcQuery9,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery9.toString() +" \n" + 
	            "MATCH (person:Person)-[:knows*1..2]->(friend) " +
		    	"WHERE person.id = " + ldbcQuery9.personId() + " " +
		    	"WITH DISTINCT friend " +
		    	"MATCH (friend)<-[:hasCreator]-(message:Message) " +
		    	"WHERE message.creationDate < " + ldbcQuery9.maxDate().getTime() + " " +
		    	"WITH friend, message " +
		    	"ORDER BY message.creationDate DESC, message.id ASC LIMIT " + ldbcQuery9.limit() + " " +
		    	"RETURN " +
		    	"  friend.id AS personId, " +
		    	"  friend.firstName AS personFirstName, " +
		    	"  friend.lastName AS personLastName, " +
		    	"  message.id AS messageId, " +
		    	"  COALESCE(message.content, message.imageFile), " +
		    	"  message.creationDate AS messageCreationDate";
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery9Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery9Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getLong(4), rs.getString(5), rs.getLong(6)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery9);
        }
    }

    public static class LdbcQuery10Handler implements
            OperationHandler<LdbcQuery10, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery10 ldbcQuery10,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery10.toString() +" \n" +
              "MATCH (person:Person)-[:knows*2..2]->(friend:Person) " +
              "WHERE person.id = " + ldbcQuery10.personId() + " AND " +
              "((to_jsonb(to_char(to_timestamp(int8(friend.birthday / 1000)), \"char\"('MM'))) = " + ldbcQuery10.month() + " AND " +
              "to_jsonb(to_char(to_timestamp(int8(friend.birthday / 1000)), \"char\"('DD'))) >= 21) OR " +
              "(to_jsonb(to_char(to_timestamp(int8(friend.birthday / 1000)), \"char\"('MM'))) = (" + ldbcQuery10.month() + " % 12)+1 AND " +
              "to_jsonb(to_char(to_timestamp(int8(friend.birthday / 1000)), \"char\"('DD'))) < 22)) AND " +
              "id(friend) != id(person) AND not exists((friend)-[:knows]->(person)) " +
              "WITH DISTINCT person, friend " +
              "MATCH (friend)-[:isLocatedInPerson]->(city:Place) " +
              "OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post) " +
              "WITH   person, friend, city.name AS personCityName, post, " +
              "CASE WHEN exists((post)-[:hasTagPost]->()<-[:hasInterest]-(person)) THEN 1 ELSE 0 END AS common " +
              "WITH   friend, personCityName, count(distinct id(post)) AS postCount, sum(common) AS commonPostCount " +
              "RETURN friend.id AS personId, friend.firstName AS personFirstName, " +
              "friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, " +
              "friend.gender AS personGender, personCityName ORDER BY commonInterestScore DESC, personId ASC LIMIT " + ldbcQuery10.limit();

            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery10Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery10Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getInt(4), rs.getString(5), rs.getString(6)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery10);
        }
    }

    public static class LdbcQuery11Handler implements
            OperationHandler<LdbcQuery11, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery11 ldbcQuery11,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery11.toString() +" \n" + 
                    "MATCH (person:Person)-[:knows*1..2]->(friend) " +
                    "WHERE person.id = " + ldbcQuery11.personId() + " AND id(person) != id(friend) " +
                    "WITH DISTINCT friend " +
                    "MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedInOrgan]->(:Place {name: '" + ldbcQuery11.countryName() + "'}) " +
                    "WHERE worksAt.\"workFrom\" < " + ldbcQuery11.workFromYear() + " " +
                    "RETURN " +
                    "  friend.id AS friendId, " +
                    "  friend.firstName AS friendFirstName, " +
                    "  friend.lastName AS friendLastName, " +
                    "  company.name AS companyName, " +
                    "  worksAt.\"workFrom\" AS workFromYear " +
                    "ORDER BY workFromYear ASC, friendId ASC, companyName DESC " +
                    "LIMIT " + ldbcQuery11.limit();
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery11Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery11Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getString(4), Integer.parseInt(rs.getString(5))));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery11);
        }
    }

    public static class LdbcQuery12Handler implements
            OperationHandler<LdbcQuery12, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery12 ldbcQuery12,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery12.toString() +" \n" + 
                    "MATCH (person:Person)-[:knows]->(friend:Person) " +
                    "WHERE person.id = " + ldbcQuery12.personId() + " " +
                    "OPTIONAL MATCH " +
                    "  (friend)<-[:hasCreatorComment]-(c)-[:replyOfPost]->()-[:hasTagPost]->(tag:Tag), " +
                    "  (tag:Tag)-[:hasType]->(tagClass:TagClass)-[:isSubclassOf*0..]->(baseTagClass:TagClass) " +
                    "WHERE tagClass.name = '" + ldbcQuery12.tagClassName() + "' OR baseTagClass.name = '" + ldbcQuery12.tagClassName() + "' " +
                    "RETURN " +
                    "  friend.id AS friendId, " +
                    "  friend.firstName AS friendFirstName, " +
                    "  friend.lastName AS friendLastName, " +
                    "  collect(DISTINCT tag.name) AS tagNames, " +
                    "  count(DISTINCT id(c)) AS count " +
                    "ORDER BY count DESC, friendId ASC " +
                    "LIMIT " + ldbcQuery12.limit();
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery12Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    int tagCount = rs.getInt(5);
                    if (tagCount > 0) {
                        List<String> tags = JsonArrayUtils.toStringList((Jsonb)(rs.getObject(4)));
                        resultList.add(new LdbcQuery12Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                                tags, rs.getInt(5)));
                    }
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcQuery12);
        }
    }

    public static class LdbcQuery13Handler implements
            OperationHandler<LdbcQuery13, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery13 ldbcQuery13,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcQuery13.toString() +" \n" + 
                    "MATCH (person1:Person), (person2:Person) " +
                    "WHERE person1.id = " + ldbcQuery13.person1Id() + " AND person2.id = " + ldbcQuery13.person2Id() + " " +
                    "OPTIONAL MATCH path = shortestpath((person1)-[:knows*..15]-(person2)) " +
                    "RETURN " +
                    "CASE path IS NULL " +
                    "  WHEN true THEN -1 " +
                    "  ELSE length(path) " +
                    "END AS pathLength";
            ResultSet rs = client.executeQuery(stmt);

            LdbcQuery13Result result = null;
            try {
                if (rs.next())
                    result = new LdbcQuery13Result(rs.getInt(1));
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            client.commit();
            resultReporter.report(0, result, ldbcQuery13);
        }
    }

    public static class LdbcQuery14Handler implements
            OperationHandler<LdbcQuery14, DbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery14 ldbcQuery14,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();
            String stmt = "-- " + ldbcQuery14.toString() +" \n" + 
                    "MATCH (person1:Person), (person2:Person) " +
                    "WHERE person1.id = " + ldbcQuery14.person1Id() + " AND person2.id = " + ldbcQuery14.person2Id() +
                    "OPTIONAL MATCH path = allshortestpaths( (person1)-[:knows*..15]-(person2) ) " +
                    "RETURN extract_ids2(nodes(path)) AS pathNodes, " +
                    "get_weight2(nodes(path)) AS weight " +
                    "ORDER BY weight DESC";
            ResultSet rs = client.executeQuery(stmt);

            List<LdbcQuery14Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    List<Long> pathNodeIds = JsonArrayUtils.toLongList(((Jsonb)rs.getObject(1)).getArray());
                    resultList.add(new LdbcQuery14Result(pathNodeIds, rs.getDouble(2)));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            client.commit();
            resultReporter.report(0, resultList, ldbcQuery14);
        }
    }

    public static class LdbcShortQuery1PersonProfileHandler implements
            OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery1PersonProfile.toString() +" \n" + 
                    "MATCH (r:Person)-[:isLocatedInPerson]->(s:Place) " +
                    "WHERE r.id = ? " +
                    "RETURN " +
                    "  r.firstName AS firstName, " +
                    "  r.lastName AS lastName, " +
                    "  r.birthday AS birthday, " +
                    "  r.locationIp AS locationIP, " +
                    "  r.browserUsed AS browserUsed, " +
                    "  s.id AS placeId, " +
                    "  r.gender AS gender, " +
                    "  r.creationDate AS creationDate";

            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery1PersonProfile.personId());

            LdbcShortQuery1PersonProfileResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery1PersonProfileResult(
                            rs.getString(1), rs.getString(2),
                            rs.getLong(3), rs.getString(4),
                            rs.getString(5), rs.getLong(6),
                            rs.getString(7), rs.getLong(8));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery1PersonProfile);
        }
    }

    public static class LdbcShortQuery2PersonPostsHandler implements
            OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts ldbcShortQuery2PersonPosts,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery2PersonPosts.toString() +" \n" + 
                    "MATCH (person:Person)<-[:hasCreator]-(m:message)" +
                    "WHERE person.id = ? " +
                    "WITH m ORDER BY m.creationDate DESC LIMIT ? " + 
                    "MATCH (m)-[:replyOf*0..]->(p:Post)-[:hasCreatorPost]->(c:Person) " +
                    "RETURN " +
                    "  m.id as messageId, " +
                    "  COALESCE(m.content, m.imageFile), " +
                    "  m.creationDate AS messageCreationDate, " +
                    "  p.id AS originalPostId, " +
                    "  c.id AS originalPostAuthorId, " +
                    "  c.firstName as originalPostAuthorFirstName, " +
                    "  c.lastName as originalPostAuthorLastName " +
                    "ORDER BY messageCreationDate DESC " +
                    "LIMIT ?";

            ResultSet rs = client.executeQuery(stmt,
                    ldbcShortQuery2PersonPosts.personId(),
                    ldbcShortQuery2PersonPosts.limit(),
                    ldbcShortQuery2PersonPosts.limit());

            List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcShortQuery2PersonPostsResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getLong(5),
                            rs.getString(6), rs.getString(7)
                    ));
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcShortQuery2PersonPosts);
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler implements
            OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends ldbcShortQuery3PersonFriends,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery3PersonFriends.toString() +" \n" + 
                    "MATCH (person:Person)-[r:knows]->(friend:Person) " +
                    "WHERE person.id = ? " +
                    "RETURN " +
                    "  friend.id AS friendId, " +
                    "  friend.firstName AS firstName, " +
                    "  friend.lastName AS lastName," +
                    "  r.\"creationDate\" AS friendshipCreationDate " +
                    " ORDER BY friendshipCreationDate DESC, friendId ASC";

            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery3PersonFriends.personId());

            List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
            try {
                while (rs.next()) {
                    result.add(new LdbcShortQuery3PersonFriendsResult(
                            rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4))
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery3PersonFriends);
        }
    }

    public static class LdbcShortQuery4MessageContentHandler implements
            OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery4MessageContent.toString() +" \n" + 
                    "MATCH (m:Message) " +
                    "WHERE m.id = ? " +
                    "RETURN " +
                    "  COALESCE(m.content, m.imageFile), " +
                    "  m.creationDate as creationDate";
            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery4MessageContent.messageId());

            LdbcShortQuery4MessageContentResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery4MessageContentResult(
                            rs.getString(1), rs.getLong(2)
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery4MessageContent);
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler implements
            OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator ldbcShortQuery5MessageCreator,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery5MessageCreator.toString() +" \n" + 
                    "MATCH (m:Message)-[:hasCreator]->(p:Person) " +
                    "WHERE m.id = ? " +
                    "RETURN " +
                    "  p.id AS personId, " +
                    "  p.firstName AS firstName, " +
                    "  p.lastName AS lastName";

            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery5MessageCreator.messageId());
            LdbcShortQuery5MessageCreatorResult result = null;

            try {
                if (rs.next()) {
                    result = new LdbcShortQuery5MessageCreatorResult(
                            rs.getLong(1), rs.getString(2), rs.getString(3)
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery5MessageCreator);
        }
    }

    public static class LdbcShortQuery6MessageForumHandler implements
            OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery6MessageForum.toString() +" \n" + 
                    "MATCH (m:Message)-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f:Forum)-[:hasModerator]->(mod:Person) " +
                    "WHERE m.id = ? " +
                    "RETURN " +
                    "  f.id AS forumId, " +
                    "  f.title AS forumTitle, " +
                    "  mod.id AS moderatorId, " +
                    "  mod.firstName AS moderatorFirstName, " +
                    "  mod.lastName AS moderatorLastName";
            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery6MessageForum.messageId());

            LdbcShortQuery6MessageForumResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery6MessageForumResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3),
                            rs.getString(4), rs.getString(5)
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery6MessageForum);
        }
    }

    public static class LdbcShortQuery7MessageRepliesHandler implements
            OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "-- " + ldbcShortQuery7MessageReplies.toString() +" \n" + 
                    "MATCH (m:Message)<-[:replyOf]-(c:\"Comment\")-[:hasCreatorComment]->(p:Person) " +
                    "WHERE m.id = ? " +
                    "OPTIONAL MATCH (m)-[:hasCreator]->(a:Person)-[r:knows]->(p) " +
                    "RETURN " +
                    "  c.id AS commentId, " +
                    "  c.content AS commentContent, " +
                    "  c.creationDate AS commentCreationDate, " +
                    "  p.id AS replyAuthorId, " +
                    "  p.firstName AS replyAuthorFirstName, " +
                    "  p.lastName AS replyAuthorLastName, " +
                    "  CASE r IS NULL" +
                    "    WHEN true THEN false " +
                    "    ELSE true " +
                    "  END AS replyAuthorKnowsOriginalMessageAuthor " +
                    "ORDER BY commentCreationDate DESC, replyAuthorId ASC";
            ResultSet rs = client.executeQuery(stmt, ldbcShortQuery7MessageReplies.messageId());

            List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcShortQuery7MessageRepliesResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getString(5),
                            rs.getString(6), rs.getBoolean(7))
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, resultList, ldbcShortQuery7MessageReplies);
        }
    }

    public static class LdbcUpdate1AddPersonHandler implements
            OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate1AddPerson ldbcUpdate1AddPerson,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "create (:Person ?)";
            Jsonb prop = JsonbUtil.createObjectBuilder()
                                  .add("id", ldbcUpdate1AddPerson.personId())
                                  .add("firstname", ldbcUpdate1AddPerson.personFirstName())
                                  .add("lastname", ldbcUpdate1AddPerson.personLastName())
                                  .add("gender", ldbcUpdate1AddPerson.gender())
                                  .add("birthday", ldbcUpdate1AddPerson.birthday().getTime())
                                  .add("creationdate", ldbcUpdate1AddPerson.creationDate().getTime())
                                  .add("locationip", ldbcUpdate1AddPerson.locationIp())
                                  .add("browserused", ldbcUpdate1AddPerson.browserUsed())
                                  .add("speaks", JsonbUtil.createArray(ldbcUpdate1AddPerson.languages()))
                                  .add("email", JsonbUtil.createArray(ldbcUpdate1AddPerson.emails()))
                                  .build();
            client.execute(stmt, prop);

            stmt =  "MATCH (p:Person), (c:Place) " +
                    "WHERE p.id = ? AND c.id = ? " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE t.id IN ? " +
                    "WITH p, c, array_agg(t) AS tags " +
                    "CREATE (p)-[:isLocatedInPerson]->(c) " +
                    "WITH p, unnest(tags) AS tag " +
                    "CREATE (p)-[:hasInterest]->(tag)";

            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate1AddPerson.tagIds());
            client.execute(stmt,
                    ldbcUpdate1AddPerson.personId(),
                    ldbcUpdate1AddPerson.cityId(),
                    tagIds);

            if (ldbcUpdate1AddPerson.studyAt().size() > 0) {
                StringBuilder matchBldr = new StringBuilder();
                StringBuilder createBldr = new StringBuilder();
                List<Object> argList = new ArrayList<>();

                matchBldr.append("MATCH (p:Person {id: ?}), ");
                createBldr.append("CREATE ");
                argList.add(ldbcUpdate1AddPerson.personId());

                for (int i = 0; i < ldbcUpdate1AddPerson.studyAt().size(); i++) {
                    LdbcUpdate1AddPerson.Organization org = ldbcUpdate1AddPerson.studyAt().get(i);
                    if (i > 0) {
                        matchBldr.append(", ");
                        createBldr.append(", ");
                    }
                    matchBldr.append(
                            String.format("(u%d:Organization {id: ?})", i));
                    createBldr.append(
                            String.format("(p)-[:studyAt {'classYear': ?}]->(u%d)", i));
                    argList.add(org.organizationId());
                    argList.add(org.year());
                }

                stmt = matchBldr.toString() + " " + createBldr.toString();
                Object[] args = new Object[argList.size()];
                argList.toArray(args);
                client.execute(stmt, args);
            }

            if (ldbcUpdate1AddPerson.workAt().size() > 0) {
                StringBuilder matchBldr = new StringBuilder();
                StringBuilder createBldr = new StringBuilder();
                List<Object> argList = new ArrayList<>();

                matchBldr.append("MATCH (p:Person {id: ?}), ");
                createBldr.append("CREATE ");
                argList.add(ldbcUpdate1AddPerson.personId());

                for (int i = 0; i < ldbcUpdate1AddPerson.workAt().size(); i++) {

                    if (i > 0) {
                        matchBldr.append(", ");
                        createBldr.append(", ");
                    }
                    matchBldr.append(
                            String.format("(c%d:Organization {id: ?})", i));
                    createBldr.append(
                            String.format("(p)-[:workAt {'workFrom': ?}]->(c%d)", i));
                    LdbcUpdate1AddPerson.Organization org = ldbcUpdate1AddPerson.workAt().get(i);
                    argList.add(org.organizationId());
                    argList.add(org.year());
                }

                stmt = matchBldr.toString() + " " + createBldr.toString();
                Object[] args = new Object[argList.size()];
                argList.toArray(args);
                client.execute(stmt, args);
            }
            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate1AddPerson);
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler implements
            OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "MATCH (p:Person), (m:Post) " +
                    "WHERE p.id = ? AND m.id = ? " +
                    "CREATE (p)-[:likesPost {'creationDate': ?}]->(m)";

            client.execute(stmt,
                    ldbcUpdate2AddPostLike.personId(),
                    ldbcUpdate2AddPostLike.postId(),
                    ldbcUpdate2AddPostLike.creationDate());

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler implements
            OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike ldbcUpdate3AddCommentLike,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "MATCH (p:Person), (m:\"Comment\") " +
                    "WHERE p.id = ? AND m.id = ? " +
                    "CREATE (p)-[:likesComment {'creationDate': ?}]->(m)";

            client.execute(stmt,
                    ldbcUpdate3AddCommentLike.personId(),
                    ldbcUpdate3AddCommentLike.commentId(),
                    ldbcUpdate3AddCommentLike.creationDate());

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate3AddCommentLike);
        }
    }

    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate4AddForum ldbcUpdate4AddForum,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "CREATE (f:Forum {id: ?, title: ?, creationDate: ?})";
            client.execute(stmt,
                    ldbcUpdate4AddForum.forumId(),
                    ldbcUpdate4AddForum.forumTitle(),
                    ldbcUpdate4AddForum.creationDate());

            stmt = "MATCH (f:Forum), (p:Person) " +
                    "WHERE f.id = ? AND p.id = ? " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE t.id IN ? " +
                    "WITH f, p, array_agg(t) as tags " +
                    "CREATE (f)-[:hasModerator]->(p) " +
                    "WITH f, unnest(tags) AS tag " +
                    "CREATE (f)-[:hasTagForum]->(tag)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate4AddForum.tagIds());
            client.execute(stmt, ldbcUpdate4AddForum.forumId(), ldbcUpdate4AddForum.moderatorPersonId(), tagIds);

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler implements
            OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership ldbcUpdate5AddForumMembership,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "MATCH (f:Forum), (p:Person) " +
                    "WHERE f.id = ? AND p.id = ? " +
                    "CREATE (f)-[:hasMember {'joinDate': ?}]->(p)";

            client.execute(stmt,
                    ldbcUpdate5AddForumMembership.forumId(),
                    ldbcUpdate5AddForumMembership.personId(),
                    ldbcUpdate5AddForumMembership.joinDate());

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate5AddForumMembership);
        }
    }

    public static class LdbcUpdate6AddPostHandler implements
            OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate6AddPost ldbcUpdate6AddPost,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "CREATE (:Post ?)";
            JsonbObjectBuilder prop = JsonbUtil.createObjectBuilder()
                                               .add("id", ldbcUpdate6AddPost.postId())
                                               .add("creationdate", ldbcUpdate6AddPost.creationDate().getTime())
                                               .add("locationip", ldbcUpdate6AddPost.locationIp())
                                               .add("browserused", ldbcUpdate6AddPost.browserUsed())
                                               .add("language", ldbcUpdate6AddPost.language());
            if (ldbcUpdate6AddPost.imageFile().length() > 0) {
                prop = prop.add("imagefile", ldbcUpdate6AddPost.imageFile());
            } else {
                prop = prop.add("content", ldbcUpdate6AddPost.content());
            }
            client.execute(stmt, prop.build());

            stmt = "MATCH (m:Post), (p:Person), (f:Forum), (c:Place) " +
                    "WHERE m.id = ? AND p.id = ? AND f.id = ? AND c.id = ? " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE t.id IN ? " +
                    "WITH m, p, f, c, array_agg(t) as tagSet " +
                    "CREATE (m)-[:hasCreatorPost]->(p), " +
                    "       (m)<-[:containerOf]-(f), " +
                    "       (m)-[:isLocatedInPost]->(c) " +
                    "WITH m, unnest(tagSet) AS tag " +
                    "CREATE (m)-[:hasTagPost]->(tag)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate6AddPost.tagIds());
            client.execute(stmt, ldbcUpdate6AddPost.postId(), ldbcUpdate6AddPost.authorPersonId(),
                    ldbcUpdate6AddPost.forumId(), ldbcUpdate6AddPost.countryId(), tagIds);

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate6AddPost);
        }
    }

    public static class LdbcUpdate7AddCommentHandler implements
            OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate7AddComment ldbcUpdate7AddComment,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "CREATE (:\"Comment\" ?)";
            Jsonb prop = JsonbUtil.createObjectBuilder()
                                  .add("id",           ldbcUpdate7AddComment.commentId())
                                  .add("creationdate", ldbcUpdate7AddComment.creationDate().getTime())
                                  .add("locationip",   ldbcUpdate7AddComment.locationIp())
                                  .add("browserused",  ldbcUpdate7AddComment.browserUsed())
                                  .add("content",      ldbcUpdate7AddComment.content())
                                  .add("length",       ldbcUpdate7AddComment.length())
                                  .build();
            client.execute(stmt, prop);

            stmt = "MATCH (m:\"Comment\"), (p:Person), (r:Message), (c:Place) " +
                    "WHERE m.id = ? AND p.id = ? AND r.id = ? AND c.id = ? " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE t.id IN ? " +
                    "WITH m, p, r, c, array_agg(t) as tagSet " +
                    "CREATE (m)-[:hasCreatorComment]->(p), " +
                    "       (m)-[:replyOf]->(r), " +
                    "       (m)-[:isLocatedInComment]->(c) " +
                    "WITH m, unnest(tagSet) AS tag " +
                    "CREATE (m)-[:hasTagComment]->(tag)";
            Long replyOfId;
            if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
                replyOfId = ldbcUpdate7AddComment.replyToCommentId();
            } else {
                replyOfId = ldbcUpdate7AddComment.replyToPostId();
            }
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate7AddComment.tagIds());
            client.execute(stmt, ldbcUpdate7AddComment.commentId(), ldbcUpdate7AddComment.authorPersonId(),
                    replyOfId, ldbcUpdate7AddComment.countryId(), tagIds);

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment);
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler implements
            OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String stmt = "MATCH (p1:Person), (p2:Person) " +
                    "WHERE p1.id = ? AND p2.id = ? " +
                    "CREATE (p1)-[:knows {'creationDate': ?}]->(p2) " +
                    "CREATE (p2)-[:knows {'creationDate': ?}]->(p1)";

            client.execute(stmt,
                    ldbcUpdate8AddFriendship.person1Id(),
                    ldbcUpdate8AddFriendship.person2Id(),
                    ldbcUpdate8AddFriendship.creationDate(),
                    ldbcUpdate8AddFriendship.creationDate());

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
        }
    }
}
