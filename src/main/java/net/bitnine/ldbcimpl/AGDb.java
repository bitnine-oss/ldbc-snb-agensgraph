package net.bitnine.ldbcimpl;

import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import net.bitnine.agensgraph.graph.property.JsonArray;
import net.bitnine.agensgraph.graph.property.JsonObject;
import net.bitnine.agensgraph.graph.property.Jsonb;
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

            String query ="MATCH (p:Person)-[path:knows*1..3]-(friend:Person {'firstname': ?}) " +
                    "WHERE p.id::int8 = ? " +
                    "WITH friend, min(array_length(path, 1)) AS distance " +
                    "ORDER BY distance ASC, friend.lastName ASC, friend.id::int8 ASC " +
                    "LIMIT ? " +
                    "MATCH (friend), (friendCity:Place) " +
                    "WHERE friend.place::int8 = friendCity.id::int8 " +
                    "OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organization), (uniCity:Place) " +
                    "WHERE uni.place::int8 = uniCity.id::int8 " +
                    "WITH " +
                    "  friend, " +
                    "  jsonb_agg( " +
                    "    CASE uni.name is null " +
                    "      WHEN true THEN 'null'::jsonb " +
                    "      ELSE array_to_json(array[uni.name, studyAt.\"classYear\", uniCity.name])::jsonb " +
                    "    END " +
                    "  ) AS unis, " +
                    "  friendCity, " +
                    "  distance " +
                    "OPTIONAL MATCH (friend)-[worksAt:workAt]->(company:Organization), (companyCountry:Place) " +
                    "WHERE company.place::int8 = companyCountry.id::int8 " +
                    "WITH " +
                    "  friend, " +
                    "  jsonb_agg( " +
                    "    CASE company.name is null " +
                    "      WHEN true THEN 'null'::jsonb " +
                    "      ELSE array_to_json(array[company.name, worksAt.\"workFrom\", companyCountry.name])::jsonb " +
                    "    END " +
                    "  ) AS companies, " +
                    "  unis, " +
                    "  friendCity, " +
                    "  distance " +
                    "ORDER BY distance ASC, friend.lastName ASC, friend.id::int8 ASC " +
                    "RETURN " +
                    "  friend.id::int8 AS id, " +
                    "  friend.lastName AS lastName, " +
                    "  distance, " +
                    "  friend.birthday::int8 AS birthday, " +
                    "  friend.creationDate::int8 AS creationDate, " +
                    "  friend.gender AS gender, " +
                    "  friend.browserUsed AS browser, " +
                    "  friend.locationIp AS locationIp, " +
                    "  friend.email::jsonb AS emails, " +
                    "  friend.speaks::jsonb AS languages, " +
                    "  friendCity.name AS cityName, " +
                    "  unis, " +
                    "  companies " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(query,
                    ldbcQuery1.firstName(), ldbcQuery1.personId(), ldbcQuery1.limit(), ldbcQuery1.limit());

            List<LdbcQuery1Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    Object obj = rs.getObject(9);
                    List<String> emails = new ArrayList<>();
                    if (obj != null)
                        emails = JsonArrayUtils.toStringList(((Jsonb)obj).getJsonArray());
                    obj = rs.getObject(10);
                    List<String> languages = new ArrayList<>();
                    if (obj != null)
                        languages = JsonArrayUtils.toStringList(((Jsonb)obj).getJsonArray());
                    List<List<Object>> universities = JsonArrayUtils.toListofList(((Jsonb)rs.getObject(12)).getJsonArray());
                    List<List<Object>> companies = JsonArrayUtils.toListofList(((Jsonb)rs.getObject(13)).getJsonArray());
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

            String stmt = "MATCH (p:Person)-[:knows]-(friend:Person), (message:Message) " +
                    "WHERE p.id::int8 = ? AND message.creationDate::int8 <= ? " +
                    "  AND friend.id::int8 = message.creator::int8 " +
                    "RETURN " +
                    "  friend.id::int8 AS personId, " +
                    "  friend.firstName AS personFirstName, " +
                    "  friend.lastName AS personLastName, " +
                    "  message.id::int8 AS messageId, " +
                    "  CASE message.content is not null " +
                    "    WHEN true THEN message.content " +
                    "    ELSE message.imageFile " +
                    "  END AS messageContent, " +
                    "  message.creationDate::int8 AS messageCreationDate " +
                    "ORDER BY messageCreationDate DESC, messageId::int8 ASC " +
                    "LIMIT ?";

            ResultSet rs = client.executeQuery(stmt, ldbcQuery2.personId(), ldbcQuery2.maxDate(), ldbcQuery2.limit());

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

            String stmt = "MATCH (person:Person)-[:knows*1..2]-(friend:Person) " +
                    "WHERE " +
                    "  person.id::int8 = ?  " +
                    "  AND id(person) <> id(friend) " +
                    "WITH DISTINCT friend " +
                    "MATCH (messageX:Message), (countryX:Place) " +
                    "WHERE messageX.creator::int8 = friend.id::int8 " +
                    "  AND messageX.place::int8 = countryX.id::int8 " +
                    "  AND not exists (select 1 from ldbc.place p " +
                    "                  where (p.properties->>'id')::int8 = friend.place::int8 " +
                    "                    and (p.properties->>'ispartof')::int8 = countryX.id::int8) " +
                    "  AND countryX.name = ? " +
                    "  AND messageX.creationDate::int8 >= ? " +
                    "  AND messageX.creationDate::int8 < ? " +
                    "WITH friend, count(DISTINCT messageX) AS xCount " +
                    "MATCH (messageY:Message), (countryY:Place) " +
                    "WHERE " +
                    "  friend.id::int8 = messageY.creator::int8 " +
                    "  and countryY.id::int8 = messageY.place::int8 " +
                    "  and countryY.name = ? " +
                    "  AND not exists (select 1 from ldbc.place p " +
                    "                  where (p.properties->>'id')::int8 = friend.place::int8 " +
                    "                    and (p.properties->>'ispartof')::int8 = countryY.id::int8) " +
                    "  AND messageY.creationDate::int8 >= ? " +
                    "  AND messageY.creationDate::int8 < ? " +
                    "WITH " +
                    "  friend.id AS friendId, " +
                    "  friend.firstName AS friendFirstName, " +
                    "  friend.lastName AS friendLastName, " +
                    "  xCount, " +
                    "  count(DISTINCT messageY) AS yCount " +
                    "RETURN " +
                    "  friendId::int8, " +
                    "  friendFirstName, " +
                    "  friendLastName, " +
                    "  xCount, " +
                    "  yCount, " +
                    "  xCount + yCount AS xyCount " +
                    "ORDER BY xyCount DESC, friendId::int8 ASC " +
                    "LIMIT ?";
            java.util.Date endDate = DateUtils.endDate(ldbcQuery3.startDate(), ldbcQuery3.durationDays());
            ResultSet rs = client.executeQuery(stmt, ldbcQuery3.personId(), ldbcQuery3.countryXName(),
                    ldbcQuery3.startDate(), endDate, ldbcQuery3.countryYName(), ldbcQuery3.startDate(), endDate,
                    ldbcQuery3.limit());
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

/*            String stmt = "MATCH (person:Person)-[:knows]-(:Person)<-[:hasCreator]-(post:Post)-[:hasTag]->(tag:Tag) " +
                    "WHERE person.id::int8 = ? AND post.creationDate::int8 >= ? AND post.creationDate::int8 < ? " +
                    "OPTIONAL MATCH (tag)<-[:hasTag]-(oldPost:Post)-[:hasCreator]->(:Person)-[:knows]-(person) " +
                    "WHERE oldPost.creationDate::int8 < ? " +
                    "WITH tag, post, count(oldPost) AS oldPostCount " +
                    "WHERE oldPostCount = 0 " +
                    "RETURN " +
                    "  tag.name AS tagName, " +
                    "  count(post) AS postCount " +
                    "ORDER BY postCount DESC, tagName ASC " +
                    "LIMIT ?"; */

            String stmt = "SELECT tagname, count(distinct postid) AS postcount FROM ( " +
                          "        SELECT T1.tag AS tagname, T1.postid AS postid, count(distinct T2.postid) AS oldPostCount " +
                          "        FROM " +
                          "        ( " +
                          "                MATCH (person:Person)-[:KNOWS]-(friend:Person), (post:Post)-[:hasTagPost]->(tag:Tag) " +
                          "                WHERE " +
                          "                person.id::int8 = ? AND post.creationDate::int8 >= ? AND post.creationDate::int8 < ? " +
                          "                AND friend.id::int8 = post.creator::int8 " +
                          "                RETURN " +
                          "                post.id::int8 AS postid, tag.name AS tag " +
                          "        ) T1 " +
                          "        LEFT JOIN " +
                          "        ( " +
                          "                MATCH (person:Person)-[:KNOWS]-(friend:Person), (post:Post)-[:hasTagPost]->(tag:Tag) " +
                          "                WHERE " +
                          "                person.id::int8 = ? AND post.creationDate::int8 < ? " +
                          "                AND friend.id::int8 = post.creator::int8 " +
                          "                RETURN " +
                          "                post.id::int8 AS postid, tag.name AS tag " +
                          "        ) T2 " +
                          "        ON T1.tag = T2.tag " +
                          "        GROUP BY T1.tag, T1.postid " +
                          ") A " +
                          "WHERE oldPostCount = 0 GROUP BY tagname " +
                          "ORDER BY postcount DESC, tagname ASC " +
                          "LIMIT ?";

            java.util.Date endDate = DateUtils.endDate(ldbcQuery4.startDate(), ldbcQuery4.durationDays());
            ResultSet rs = client.executeQuery(stmt, ldbcQuery4.personId(), ldbcQuery4.startDate(), endDate,
                    ldbcQuery4.personId(), ldbcQuery4.startDate(), ldbcQuery4.limit());

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

            String stmt = "MATCH (person:Person)-[:knows*1..2]-(friend:Person)<-[membership:hasMember]-(forum:Forum) " +
                    "WHERE person.id::int8 = ? AND id(person) <> id(friend) " +
                    "AND membership.\"joinDate\"::int8 > ? " +
                    "WITH DISTINCT friend.id::int8 AS friendId, forum.id::int8 AS forumId, forum.title AS forumTitle " +
                    "OPTIONAL MATCH (post:Post) " +
                    "WHERE friendId = post.creator::int8 and post.forumID::int8 = forumId " +
                    "WITH forumTitle, forumId, count(post) AS postCount " +
                    "ORDER BY postCount DESC, forumId ASC " +
                    "RETURN " +
                    "  forumTitle, " +
                    "  postCount " +
                    "LIMIT ?";

            ResultSet rs = client.executeQuery(stmt, ldbcQuery5.personId(), ldbcQuery5.minDate(), ldbcQuery5.limit());

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

            String stmt = "MATCH (person:Person)-[:knows*1..2]-(friend:Person) " +
                    "WHERE person.id::int8 = ? AND id(person) <> id(friend) " +
                    "WITH DISTINCT friend.id::int8 AS friendId " +
                    "MATCH (friendPost:Post)-[:hasTagPost]->(knownTag:Tag) " +
                    "WHERE friendPost.creator::int8 = friendId " +
                    "AND knownTag.name <> ? " +
                    "AND exists((friendPost)-[:hasTagPost]->(:Tag {'name': ?})) " +
                    "RETURN " +
                    "  knownTag.name AS tagName, " +
                    "  count(friendPost) AS postCount " +
                    "ORDER BY postCount DESC, tagName ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery6.personId(),
                    ldbcQuery6.tagName(), ldbcQuery6.tagName(), ldbcQuery6.limit());

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

            String stmt = "MATCH (person:Person), (message:Message)<-[l:likes]-(liker:Person)  " +
                    "WHERE person.id::int8 = ? AND person.id::int8 = message.creator::int8  " +
                    "WITH  " +
                    "  liker.id::int8 AS personId,  " +
                    "  liker.firstName AS personFirstName, " +
                    "  liker.lastName AS personLastName, " +
                    "  c7(array_agg(jsonb_build_object('msg', to_jsonb(message), 'likeTime', l.\"creationDate\"::int8, 'id', message.id::int8))) AS latestLike,  " +
                    "  person.id::int8 AS otherId " +
                    "RETURN  " +
                    "  personId,  " +
                    "  personFirstName,  " +
                    "  personLastName,  " +
                    "  (latestLike->>'likeTime')::int8 AS likeTime,  " +
                    "  (latestLike->'msg'->>'id')::int8 AS messageId,  " +
                    "  CASE latestLike->'msg'->>'content' is not null  " +
                    "    WHEN true THEN latestLike->'msg'->>'content'  " +
                    "    ELSE latestLike->'msg'->>'imagefile'  " +
                    "  END AS messageContent,  " +
                    "  ((latestLike->>'likeTime')::int8 - (latestLike->'msg'->>'creationdate')::int8) / (1000 * 60) AS latency,  " +
                    "  not exists((:Person {id: personId})-[:knows]-(:Person {id: otherId}))  " +
                    "ORDER BY likeTime DESC, personId ASC  " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery7.personId(), ldbcQuery7.limit());

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

            String stmt = "MATCH (p:Person), (m:Message), (c:\"Comment\"), (person:Person) " +
                    "WHERE p.id::int8 = ? " +
                    "  AND p.id::int8 = m.creator::int8 " +
                    "  AND (m.id::int8 = c.replyOfPost::int8 OR m.id::int8 = c.replyOfComment::int8) " +
                    "  AND c.creator::int8 = person.id::int8 " +
                    "RETURN " +
                    "  person.id::int8 AS personId, " +
                    "  person.firstName AS personFirstName, " +
                    "  person.lastName AS personLastName, " +
                    "  c.creationDate::int8 AS commentCreationDate, " +
                    "  c.id::int8 AS commentId, " +
                    "  c.content AS commentContent " +
                    "ORDER BY commentCreationDate DESC, commentId ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery8.personId(), ldbcQuery8.limit());

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

            String stmt = "MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person) " +
                    "WHERE person.id::int8 = ? " +
                    "WITH DISTINCT" +
                    "   friend.id::int8 AS personId, " +
                    "   friend.firstName AS personFirstName, " +
                    "   friend.lastName AS personLastName " +
                    "MATCH (message:Message) " +
                    " WHERE message.creationDate::int8 < ? " +
                    "   AND message.creator::int8 = personId " +
                    " RETURN " +
                    "   personId, personFirstName, personLastName, " +
                    "   message.id::int8 AS messageId, " +
                    "   CASE message.content is not null " +
                    "     WHEN true THEN message.content " +
                    "     ELSE message.imageFile " +
                    "   END AS messageContent, " +
                    "   message.creationDate::int8 AS messageCreationDate " +
                    " ORDER BY messageCreationDate DESC, messageId ASC " +
                    " LIMIT ?" ;
            ResultSet rs = client.executeQuery(stmt, ldbcQuery9.personId(), ldbcQuery9.maxDate(),
                    ldbcQuery9.limit());

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

            String stmt = "MATCH (person:Person)-[:knows*2..2]-(friend:Person), (city:Place) " +
                    "WHERE person.id::int8 = ? " +
                    "  AND friend.place::int8 = city.id::int8 " +
                    "  AND ((extract(month from to_timestamp(friend.birthday::int8 / 1000)) = ? AND extract(day from to_timestamp(friend.birthday::int8 / 1000)) >= 21) OR " +
                    "       (extract(month from to_timestamp(friend.birthday::int8 / 1000)) = (? % 12)+1 AND extract(day from to_timestamp(friend.birthday::int8 / 1000)) < 22)) " +
                    "  AND id(friend) <> id(person) " +
                    "  AND not exists((friend)-[:knows]-(person)) " +
                    "WITH DISTINCT " +
                    "  friend.id::int8 AS personId, " +
                    "  friend.firstName AS personFirstName, " +
                    "  friend.lastName AS personLastName, " +
                    "  friend.gender AS personGender, " +
                    "  city.name AS personCityName " +
                    "OPTIONAL MATCH (post:Post) " +
                    "WHERE personId = post.creator::int8 " +
                    "WITH " +
                    "  personId, " +
                    "  personFirstName, " +
                    "  personLastName, " +
                    "  personGender, " +
                    "  personCityName, " +
                    "  array_remove(array_agg(post.id::int8), NULL) posts " +
                    "WITH " +
                    "  personId, " +
                    "  personFirstName, " +
                    "  personLastName, " +
                    "  personGender, " +
                    "  personCityName, " +
                    "  case posts = '{}' when true then 0 " +
                    "  else array_length(posts, 1) end AS postCount, " +
                    "  c10_fc(posts, ?) AS commonPostCount " +
                    "RETURN " +
                    "  personId, " +
                    "  personFirstName, " +
                    "  personLastName, " +
                    "  commonPostCount - (postCount - commonPostCount) AS commonInterestScore, " +
                    "  personGender, " +
                    "  personCityName " +
                    "ORDER BY commonInterestScore DESC, personId ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery10.personId(),
                    ldbcQuery10.month(), ldbcQuery10.month(), ldbcQuery10.personId(), ldbcQuery10.limit());

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

            String stmt = "MATCH (person:Person)-[:knows*1..2]-(friend:Person) " +
                    "WHERE person.id::int8 = ? AND id(person) <> id(friend) " +
                    "WITH DISTINCT friend " +
                    "MATCH (friend)-[worksAt:workAt]->(company:Organization), (place:Place {'name': ?}) " +
                    "WHERE worksAt.\"workFrom\"::int < ?" +
                    "  AND company.place::int8 = place.id::int8 " +
                    "RETURN " +
                    "  friend.id::int8 AS friendId, " +
                    "  friend.firstName AS friendFirstName, " +
                    "  friend.lastName AS friendLastName, " +
                    "  company.name AS companyName, " +
                    "  worksAt.\"workFrom\"::int AS workFromYear " +
                    "ORDER BY workFromYear ASC, friendId ASC, companyName DESC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery11.personId(), ldbcQuery11.countryName(),
                    ldbcQuery11.workFromYear(), ldbcQuery11.limit());

            List<LdbcQuery11Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcQuery11Result(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getString(4), rs.getInt(5)));
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

            String stmt = "MATCH (person:Person)-[:knows]-(friend:Person) " +
                    "WHERE person.id::int8 = ? " +
                    "OPTIONAL MATCH " +
                    "  (c:\"Comment\"), (post:Post)-[:hasTagPost]->(tag:Tag), " +
                    "  (tag:Tag)-[:hasType]->(tagClass:TagClass)-[:isSubclassOf*0..]->(baseTagClass:TagClass) " +
                    "WHERE (tagClass.name = ? OR baseTagClass.name = ?) " +
                    "  AND friend.id::int8 = c.creator::int8 " +
                    "  AND c.replyOfPost::int8 = post.id::int8 " +
                    "RETURN " +
                    "  friend.id::int8 AS friendId, " +
                    "  friend.firstName AS friendFirstName, " +
                    "  friend.lastName AS friendLastName, " +
                    "  array_to_json(array_remove(array_agg(DISTINCT tag.name), NULL))::jsonb AS tagNames, " +
                    "  count(DISTINCT c) AS count " +
                    "ORDER BY count DESC, friendId ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery12.personId(), ldbcQuery12.tagClassName(),
                    ldbcQuery12.tagClassName(), ldbcQuery12.limit());

            List<LdbcQuery12Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    int tagCount = rs.getInt(5);
                    if (tagCount > 0) {
                        List<String> tags = JsonArrayUtils.toStringList(((Jsonb) rs.getObject(4)).getJsonArray());
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

            String stmt = "MATCH (person1:Person), (person2:Person) " +
                    "WHERE person1.id::int8 = ? AND person2.id::int8 = ? " +
                    "WITH shortestpath_vertex_ids(person1, person2) as vertex_ids " +
                    "RETURN " +
                    "CASE vertex_ids IS NULL " +
                    "  WHEN true THEN -1 " +
                    "  ELSE array_length(vertex_ids, 1) - 1 " +
                    "END AS pathLength";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery13.person1Id(), ldbcQuery13.person2Id());

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
            String stmt = "SELECT " +
                    "  extract_ids(vertex_ids) AS pathNodeIds, " +
                    "  calc_weight(vertex_ids) AS weight " +
                    "FROM allshortestpath_vertex_ids(?, ?) " +
                    "ORDER BY weight DESC";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery14.person1Id(), ldbcQuery14.person2Id());

            List<LdbcQuery14Result> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    List<Long> pathNodeIds = JsonArrayUtils.toLongList(((Jsonb)rs.getObject(1)).getJsonArray());
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

            String stmt = "MATCH (r:Person) " +
                    "WHERE r.id::int8 = ? " +
                    "RETURN " +
                    "  r.firstName AS firstName, " +
                    "  r.lastName AS lastName, " +
                    "  r.birthday::int8 AS birthday, " +
                    "  r.locationIp AS locationIP, " +
                    "  r.browserUsed AS browserUsed, " +
                    "  r.place::int8 AS placeId, " +
                    "  r.gender AS gender, " +
                    "  r.creationDate::int8 AS creationDate";

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

            /*
            String stmt = "MATCH (person:Person), (m:Message) " +
                    "MATCH (m)-[:replyOf*0..]->(p:Post) " +
                    "MATCH (p), (c:Person) " +
                    "WHERE person.id::int8 = ? " +
                    "  AND person.id::int8 = m.creator::int8 " +
                    "  AND p.creator::int8 = c.id::int8 " +
                    "RETURN " +
                    "  m.id::int8 as messageId, " +
                    "  CASE m.content is not null " +
                    "    WHEN true THEN m.content " +
                    "    ELSE m.imageFile " +
                    "  END AS messageContent, " +
                    "  m.creationDate::int8 AS messageCreationDate, " +
                    "  p.id::int8 AS originalPostId, " +
                    "  c.id::int8 AS originalPostAuthorId, " +
                    "  c.firstName as originalPostAuthorFirstName, " +
                    "  c.lastName as originalPostAuthorLastName " +
                    "ORDER BY messageCreationDate DESC " +
                    "LIMIT ?";
            */
            client.execute("set enable_hashjoin = off");
            client.execute("set enable_mergejoin = off");
            String stmt ="WITH RECURSIVE replyof (id, replyOfPost, replyOfComment, content, imageFile, creationDate) AS ( " +
                    "		SELECT * " +
                    "		FROM ( " +
                    "           SELECT (p.properties->>'id')::int8," +
                    "                  (p.properties->>'id')::int8, NULL::int8, " +
                    "                  (p.properties->>'content'), " +
                    "                  (p.properties->>'imagefile'), " +
                    "                  (p.properties->>'creationdate')::int8 " +
                    "           FROM ldbc.post p " +
                    "           WHERE (p.properties #>> ARRAY['creator'::text])::int8 = ? " +
                    "			UNION ALL " +
                    "           SELECT (c.properties->>'id')::int8, " +
                    "                  (c.properties->>'replyofpost')::int8," +
                    "                  (c.properties->>'replyofcomment')::int8, " +
                    "                  (c.properties->>'content'), " +
                    "                  (c.properties->>'imagefile'), " +
                    "                  (c.properties->>'creationdate')::int8 " +
                    "			FROM ldbc.\"Comment\" c " +
                    "			WHERE (c.properties #>> ARRAY['creator'::text])::int8 = ? " +
                    "		) m " +
                    "		UNION ALL " +
                    "		SELECT r.id, (c.properties->>'replyofpost')::int8, " +
                    "              (c.properties->>'replyofcomment')::int8, " +
                    "              r.content, r.imageFile, r.creationDate " +
                    "		FROM replyof r, ldbc.\"Comment\" c " +
                    "		WHERE r.replyOfComment = (c.properties #>> '{id}'::text[])::int8 " +
                    "		  AND r.replyOfComment IS NOT NULL " +
                    ") " +
                    "SELECT " +
                    "  m.id as messageId, " +
                    "  CASE m.content is not null " +
                    "    WHEN true THEN m.content " +
                    "    ELSE m.imageFile " +
                    "  END AS messageContent, " +
                    "  m.creationDate::int8 AS messageCreationDate, " +
                    "  (p.properties->>'id')::int8 AS originalPostId, " +
                    "  (creator.properties->>'id')::int8 AS originalPostAuthorId, " +
                    "  (creator.properties->>'firstname') as originalPostAuthorFirstName, " +
                    "  (creator.properties->>'lastname') as originalPostAuthorLastName " +
                    "FROM replyof m, ldbc.post p, ldbc.person creator " +
                    "WHERE m.replyOfPost = (p.properties #>> '{id}'::text[])::int8 " +
                    "  AND (p.properties->>'creator')::int8 = (creator.properties #>> '{id}'::text[])::int8 " +
                    "ORDER BY messageCreationDate DESC " +
                    "LIMIT ?";

            ResultSet rs = client.executeQuery(stmt,
                    ldbcShortQuery2PersonPosts.personId(),
                    ldbcShortQuery2PersonPosts.personId(),
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

            client.execute("set enable_hashjoin = on");
            client.execute("set enable_mergejoin = on");

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

            String stmt = "MATCH (person:Person)-[r:knows]-(friend:Person) " +
                    "WHERE person.id::int8 = ? " +
                    "RETURN " +
                    "  friend.id::int8 AS friendId, " +
                    "  friend.firstName AS firstName, " +
                    "  friend.lastName AS lastName," +
                    "  r.\"creationDate\"::int8 AS friendshipCreationDate " +
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

            String stmt = "MATCH (m:Message) " +
                    "WHERE m.id::int8 = ? " +
                    "RETURN " +
                    "  CASE m.content is not null " +
                    "    WHEN true THEN m.content " +
                    "    ELSE m.imageFile " +
                    "  END AS content, " +
                    "  m.creationDate::int8 as creationDate";
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

            String stmt = "MATCH (m:Message), (p:Person) " +
                    "WHERE m.id::int8 = ? AND m.creator::int8 = p.id::int8 " +
                    "RETURN " +
                    "  p.id::int8 AS personId, " +
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

            String stmt = "WITH RECURSIVE replyof (id, replyOfPost, replyOfComment, content, imageFile, creationDate) AS ( " +
                    "		SELECT * " +
                    "		FROM ( " +
                    "           SELECT (p.properties->>'id')::int8," +
                    "                  (p.properties->>'id')::int8, NULL::int8, " +
                    "                  (p.properties->>'content'), " +
                    "                  (p.properties->>'imagefile'), " +
                    "                  (p.properties->>'creationdate')::int8 " +
                    "           FROM ldbc.post p " +
                    "           WHERE (p.properties #>> '{id}'::text[])::int8 = ? " +
                    "			UNION ALL " +
                    "           SELECT (c.properties->>'id')::int8, " +
                    "                  (c.properties->>'replyofpost')::int8," +
                    "                  (c.properties->>'replyofcomment')::int8, " +
                    "                  (c.properties->>'content'), " +
                    "                  (c.properties->>'imagefile'), " +
                    "                  (c.properties->>'creationdate')::int8 " +
                    "			FROM ldbc.\"Comment\" c " +
                    "			WHERE (c.properties #>>'{id}'::text[])::int8 = ? " +
                    "		) m " +
                    "		UNION ALL " +
                    "		SELECT r.id, (c.properties->>'replyofpost')::int8, " +
                    "              (c.properties->>'replyofcomment')::int8, " +
                    "              r.content, r.imageFile, r.creationDate " +
                    "		FROM replyof r, ldbc.\"Comment\" c " +
                    "		WHERE r.replyOfComment = (c.properties #>>'{id}'::text[])::int8 " +
                    "		  AND r.replyOfComment IS NOT NULL " +
                    ") " +
                    "SELECT " +
                    "  (f.properties->>'id')::int8 AS forumId, " +
                    "  (f.properties->>'title') AS forumTitle, " +
                    "  (mod.properties->>'id')::int8 AS moderatorId, " +
                    "  (mod.properties->>'firstname') AS moderatorFirstName, " +
                    "  (mod.properties->>'lastname') AS moderatorLastName " +
                    "FROM replyof m, ldbc.post p, ldbc.forum f, ldbc.person mod " +
                    "WHERE m.replyOfPost = (p.properties #>> '{id}'::text[])::int8 " +
                    "  AND (p.properties->>'forumid')::int8 = (f.properties #>> '{id}'::text[])::int8 " +
                    "  AND (f.properties->>'moderator')::int8 = (mod.properties #>> '{id}'::text[])::int8 ";

            ResultSet rs = client.executeQuery(stmt,
                    ldbcShortQuery6MessageForum.messageId(),
                    ldbcShortQuery6MessageForum.messageId());

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

            String stmt = "MATCH (m:Message), (c:\"Comment\"), (p:Person) " +
                    "WHERE m.id::int8 = ? " +
                    "  AND (m.id::int8 = c.replyOfPost::int8 OR m.id::int8 = c.replyOfComment::int8) " +
                    "  AND (c.creator::int8 = p.id::int8) " +
                    "OPTIONAL MATCH (m), (a:Person)-[r:knows]-(p) " +
                    "WHERE m.creator::int8 = a.id::int8 " +
                    "RETURN " +
                    "  c.id::int8 AS commentId, " +
                    "  c.content AS commentContent, " +
                    "  c.creationDate::int8 AS commentCreationDate, " +
                    "  p.id::int8 AS replyAuthorId, " +
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
            JsonObject prop = new JsonObject();
            prop.put("id", ldbcUpdate1AddPerson.personId());
            prop.put("firstname", ldbcUpdate1AddPerson.personFirstName());
            prop.put("lastname", ldbcUpdate1AddPerson.personLastName());
            prop.put("gender", ldbcUpdate1AddPerson.gender());
            prop.put("birthday", ldbcUpdate1AddPerson.birthday().getTime());
            prop.put("creationdate", ldbcUpdate1AddPerson.creationDate().getTime());
            prop.put("locationip", ldbcUpdate1AddPerson.locationIp());
            prop.put("browserused", ldbcUpdate1AddPerson.browserUsed());
            prop.put("speaks", JsonArray.create(ldbcUpdate1AddPerson.languages()));
            prop.put("email", JsonArray.create(ldbcUpdate1AddPerson.emails()));
            prop.put("place", ldbcUpdate1AddPerson.cityId());
            client.execute(stmt, prop);

            stmt =  "MATCH (p:Person), (t:Tag) " +
                    "WHERE p.id::int8 = ?" +
                    "  AND ? @> array[t.id::int8] " +
                    "CREATE (p)-[:hasInterest]->(t)";

            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate1AddPerson.tagIds());
            client.execute(stmt, ldbcUpdate1AddPerson.personId(), tagIds);

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
                    "WHERE p.id::int8 = ? AND m.id::int8 = ? " +
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
                    "WHERE p.id::int8 = ? AND m.id::int8 = ? " +
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

            String stmt = "CREATE (f:Forum {id: ?, title: ?, creationDate: ?, moderator: ?})";
            client.execute(stmt,
                    ldbcUpdate4AddForum.forumId(),
                    ldbcUpdate4AddForum.forumTitle(),
                    ldbcUpdate4AddForum.creationDate(),
                    ldbcUpdate4AddForum.moderatorPersonId());

            stmt = "MATCH (f:Forum), (t:Tag) " +
                    "WHERE f.id::int8 = ? " +
                    "  AND ? @> array[t.id::int8] " +
                    "CREATE (f)-[:hasTagForum]->(t)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate4AddForum.tagIds());
            client.execute(stmt, ldbcUpdate4AddForum.forumId(), tagIds);

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
                    "WHERE f.id::int8 = ? AND p.id::int8 = ? " +
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
            JsonObject prop = new JsonObject();
            prop.put("id", ldbcUpdate6AddPost.postId());
            prop.put("creationdate", ldbcUpdate6AddPost.creationDate().getTime());
            prop.put("locationip", ldbcUpdate6AddPost.locationIp());
            prop.put("browserused", ldbcUpdate6AddPost.browserUsed());
            prop.put("language", ldbcUpdate6AddPost.language());
            if (ldbcUpdate6AddPost.imageFile().length() > 0) {
                prop.put("imagefile", ldbcUpdate6AddPost.imageFile());
            } else {
                prop.put("content", ldbcUpdate6AddPost.content());
            }
            prop.put("creator", ldbcUpdate6AddPost.authorPersonId());
            prop.put("forumid", ldbcUpdate6AddPost.forumId());
            prop.put("place", ldbcUpdate6AddPost.countryId());
            client.execute(stmt, prop);

            stmt =  "MATCH (m:Post), (t:Tag) " +
                    "WHERE m.id::int8 = ? " +
                    "  AND ? @> array[t.id::int8] " +
                    "CREATE (m)-[:hasTagPost]->(t)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate6AddPost.tagIds());
            client.execute(stmt, ldbcUpdate6AddPost.postId(), tagIds);

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
            JsonObject prop = new JsonObject();
            prop.put("id", ldbcUpdate7AddComment.commentId());
            prop.put("creationdate", ldbcUpdate7AddComment.creationDate().getTime());
            prop.put("locationip", ldbcUpdate7AddComment.locationIp());
            prop.put("browserused", ldbcUpdate7AddComment.browserUsed());
            prop.put("content", ldbcUpdate7AddComment.content());
            prop.put("length", ldbcUpdate7AddComment.length());
            prop.put("creator", ldbcUpdate7AddComment.authorPersonId());
            prop.put("place", ldbcUpdate7AddComment.countryId());
            if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
                prop.put("replyofcomment", ldbcUpdate7AddComment.replyToCommentId());
            } else {
                prop.put("replyofpost", ldbcUpdate7AddComment.replyToPostId());
            }
            client.execute(stmt, prop);

            stmt = "MATCH (m:\"Comment\"), (t:Tag) " +
                    "WHERE m.id::int8 = ? " +
                    "  AND ? @> array[t.id::int8] " +
                    "CREATE (m)-[:hasTagComment]->(t)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate7AddComment.tagIds());
            client.execute(stmt, ldbcUpdate7AddComment.commentId(), tagIds);

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
                    "WHERE p1.id::int8 = ? AND p2.id::int8 = ? " +
                    "CREATE (p1)-[:knows {'creationDate': ?}]->(p2)";

            client.execute(stmt,
                    ldbcUpdate8AddFriendship.person1Id(),
                    ldbcUpdate8AddFriendship.person2Id(),
                    ldbcUpdate8AddFriendship.creationDate());

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
        }
    }
}
