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

    static class AGDbConnectionState extends DbConnectionState {

        private AGClient client;

        private AGDbConnectionState(Map<String, String> properties) {
            String server = properties.get("server");
            if (server == null)
                server = "127.0.0.1";
            String port = properties.get("port");
            if (port == null)
                port = "5432";
            String connStr = "jdbc:agensgraph://"
                    + server + ":"
                    + port + "/"
                    + properties.get("dbname");
            client = new AGClient(connStr, properties.get("user"), properties.get("password"));
        }

        AGClient getClent() {
            return client;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

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

            String query ="MATCH (:Person {'id': ?})-[path:knows*1..3]-(friend:Person) " +
                    "WHERE friend.firstName = ? " +
                    "WITH friend, min(array_length(path, 1)) AS distance " +
                    "ORDER BY distance ASC, friend.lastName ASC, friend.id::int8 ASC " +
                    "LIMIT ? " +
                    "MATCH (friend)-[:isLocatedIn]->(friendCity:Place) " +
                    "OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organization)-[:isLocatedIn]->(uniCity:Place) " +
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
                    "OPTIONAL MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedIn]->(companyCountry:Place) " +
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
                    ldbcQuery1.personId(), ldbcQuery1.firstName(), ldbcQuery1.limit(), ldbcQuery1.limit());

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

            String stmt = "MATCH (:Person {'id': ?})-[:knows]-(friend:Person)<-[:hasCreator]-(message:Message) " +
                    "WHERE message.creationDate::int8 <= ? " +
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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows*1..2]-(friend:Person)<-[:hasCreator]-(messageX:Message), " +
                    "(messageX:Message)-[:isLocatedIn]->(countryX:Place) " +
                    "WHERE " +
                    "  person.id != friend.id " +
                    "  AND not exists ((friend)-[:isLocatedIn]->()-[:isPartOf]->(countryX)) " +
                    "  AND countryX.name = ? " +
                    "  AND messageX.creationDate::int8 >= ? " +
                    "  AND messageX.creationDate::int8 < ? " +
                    "WITH friend, count(DISTINCT messageX) AS xCount " +
                    "MATCH (friend)<-[:hasCreator]-(messageY)-[:isLocatedIn]->(countryY:Place) " +
                    "WHERE " +
                    "  countryY.name= ? " +
                    "  AND not exists ((friend)-[:isLocatedIn]->()-[:isPartOf]->(countryY)) " +
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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows]-(:Person)<-[:hasCreator]-(post:Post)-[:hasTag]->(tag:Tag) " +
                    "WHERE post.creationDate::int8 >= ? AND post.creationDate::int8 < ? " +
                    "OPTIONAL MATCH (tag)<-[:hasTag]-(oldPost:Post)-[:hasCreator]->(:Person)-[:knows]-(person) " +
                    "WHERE oldPost.creationDate::int8 < ? " +
                    "WITH tag, post, count(oldPost) AS oldPostCount " +
                    "WHERE oldPostCount = 0 " +
                    "RETURN " +
                    "  tag.name AS tagName, " +
                    "  count(post) AS postCount " +
                    "ORDER BY postCount DESC, tagName ASC " +
                    "LIMIT ?";
            java.util.Date endDate = DateUtils.endDate(ldbcQuery4.startDate(), ldbcQuery4.durationDays());
            ResultSet rs = client.executeQuery(stmt, ldbcQuery4.personId(), ldbcQuery4.startDate(), endDate,
                    ldbcQuery4.startDate(), ldbcQuery4.limit());

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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows*1..2]-(friend:Person)<-[membership:hasMember]-(forum:Forum) " +
                    "WHERE membership.\"joinDate\"::int8 > ? AND person.id != friend.id " +
                    "WITH DISTINCT friend, forum " +
                    "OPTIONAL MATCH (friend)<-[:hasCreator]-(post:Post)<-[:containerOf]-(forum) " +
                    "WITH forum, count(post) AS postCount " +
                    "ORDER BY postCount DESC, forum.id::int8 ASC " +
                    "RETURN " +
                    "  forum.title AS forumTitle, " +
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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows*1..2]-(friend:Person) " +
                    ", (friend:Person)<-[:hasCreator]-(friendPost:Post)-[:hasTag]->(knownTag:Tag {'name': ?}) " +
                    "WITH person, friend, friendPost, knownTag " +
                    "WHERE person.id != friend.id " +
                    "MATCH (friendPost)-[:hasTag]->(commonTag:Tag) " +
                    "WHERE commonTag.id != knownTag.id " +
                    "WITH DISTINCT commonTag, knownTag, friend " +
                    "MATCH (commonTag)<-[:hasTag]-(commonPost:Post)-[:hasTag]->(knownTag) " +
                    "WHERE exists((commonPost)-[:hasCreator]->(friend)) " +
                    "RETURN " +
                    "  commonTag.name AS tagName, " +
                    "  count(commonPost) AS postCount " +
                    "ORDER BY postCount DESC, tagName ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery6.personId(), ldbcQuery6.tagName(),
                    ldbcQuery6.limit());

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

            String stmt = "MATCH (person:Person {'id': ?})<-[:hasCreator]-(message:Message)<-[l:likes]-(liker:Person) " +
                    "WITH liker, message, l.\"creationDate\"::int8 AS likeTime, person " +
                    "ORDER BY likeTime DESC, message.id::int8 ASC " +
                    "WITH " +
                    "  liker, " +
                    "  (array_agg(jsonb_build_object('msg', to_jsonb(message), 'likeTime', likeTime)))[1] AS latestLike, " +
                    "  person " +
                    "RETURN " +
                    "  liker.id::int8 AS personId, " +
                    "  liker.firstName AS personFirstName, " +
                    "  liker.lastName AS personLastName, " +
                    "  (latestLike->>'likeTime')::int8 AS likeTime, " +
                    "  (latestLike->'msg'->>'id')::int8 AS messageId, " +
                    "  CASE latestLike->'msg'->>'content' is not null " +
                    "    WHEN true THEN latestLike->'msg'->>'content' " +
                    "    ELSE latestLike->'msg'->>'imagefile' " +
                    "  END AS messageContent, " +
                    "  ((latestLike->>'likeTime')::int8 - (latestLike->'msg'->>'creationdate')::int8) / (1000 * 60) AS latency, " +
                    "  not exists((liker)-[:knows]->(person)) " +
                    "ORDER BY likeTime DESC, personId ASC " +
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

            String stmt = "MATCH (:Person {'id': ?})<-[:hasCreator]-()<-[:replyOf]-(c:\"Comment\")-[:hasCreator]->(person:Person) " +
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

            String stmt = "MATCH (:Person {'id': ?})-[:knows*1..2]-(friend:Person)<-[:hasCreator]-(message:Message) " +
                    "WHERE message.creationDate::int8 < ? " +
                    "RETURN DISTINCT " +
                    "  friend.id::int8 AS personId, " +
                    "  friend.firstName AS personFirstName, " +
                    "  friend.lastName AS personLastName, " +
                    "  message.id::int8 AS messageId, " +
                    "  CASE message.content is not null " +
                    "    WHEN true THEN message.content " +
                    "    ELSE message.imageFile " +
                    "  END AS messageContent, " +
                    "  message.creationDate::int8 AS messageCreationDate " +
                    "ORDER BY messageCreationDate DESC, messageId ASC " +
                    "LIMIT ?";
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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows*2..2]-(friend:Person)-[:isLocatedIn]->(city:Place) " +
                    "WITH " +
                    "  friend, " +
                    "  city, " +
                    "  person, " +
                    "  extract(month from to_timestamp(friend.birthday::int8 / 1000)) AS birthdayMonth, " +
                    "  extract(day from to_timestamp(friend.birthday::int8 / 1000)) AS birthdayDay " +
                    "WHERE " +
                    "  ((birthdayMonth = ? AND birthdayDay >= 21) OR " +
                    "   (birthdayMonth = (? % 12)+1 AND birthdayDay < 22)) " +
                    "  AND friend.id != person.id " +
                    "  AND not exists((friend)-[:knows]->(person)) " +
                    "WITH DISTINCT friend, city, person " +
                    "OPTIONAL MATCH (friend)<-[:hasCreator]-(post:Post) " +
                    "WITH friend, city, array_remove(array_agg(post), NULL) AS posts, person " +
                    "WITH  " +
                    "  friend, " +
                    "  city, " +
                    "  array_length(posts, 1) AS postCount, " +
                    "  c10_fc(posts, person.id::int8) AS commonPostCount " +
                    "RETURN " +
                    "  friend.id::int8 AS personId, " +
                    "  friend.firstName AS personFirstName, " +
                    "  friend.lastName AS personLastName, " +
                    "  commonPostCount - (postCount - commonPostCount) AS commonInterestScore, " +
                    "  friend.gender AS personGender, " +
                    "  city.name AS personCityName " +
                    "ORDER BY commonInterestScore DESC, personId ASC " +
                    "LIMIT ?";
            ResultSet rs = client.executeQuery(stmt, ldbcQuery10.personId(), ldbcQuery10.month(), ldbcQuery10.month(),
                    ldbcQuery10.limit());

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

            String stmt = "MATCH (person:Person {'id': ?})-[:knows*1..2]-(friend:Person) " +
                    "WHERE person.id != friend.id " +
                    "WITH DISTINCT friend " +
                    "MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedIn]->(:Place {'name': ?}) " +
                    "WHERE worksAt.\"workFrom\"::int < ? " +
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

            String stmt = "MATCH (:Person {'id': ?})-[:knows]-(friend:Person) " +
                    "OPTIONAL MATCH " +
                    "  (friend)<-[:hasCreator]-(c:\"Comment\")-[:replyOf]->(:Post)-[:hasTag]->(tag:Tag), " +
                    "  (tag:Tag)-[:hasType]->(tagClass:TagClass)-[:isSubclassOf*0..]->(baseTagClass:TagClass) " +
                    "WHERE tagClass.name = ? OR baseTagClass.name = ? " +
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

            String stmt = "MATCH (person1:Person {'id': ?}), (person2:Person {'id': ?}) " +
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

            String stmt = "MATCH (r:Person {'id': ?})-[:isLocatedIn]->(s:Place) " +
                    "RETURN " +
                    "  r.firstName AS firstName, " +
                    "  r.lastName AS lastName, " +
                    "  r.birthday::int8 AS birthday, " +
                    "  r.locationIp AS locationIP, " +
                    "  r.browserUsed AS browserUsed, " +
                    "  s.id::int8 AS placeId, " +
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

            String stmt = "MATCH (:Person {'id': ?})<-[:hasCreator]-(m:Message) " +
                    "MATCH (m)-[:replyOf*0..]->(p:Post) " +
                    "MATCH (p)-[:hasCreator]->(c) " +
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

            ResultSet rs = client.executeQuery(stmt,
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

            String stmt = "MATCH (:Person {'id': ?})-[r:knows]-(friend) " +
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

            String stmt = "MATCH (m:Message {'id': ?}) " +
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

            String stmt = "MATCH (:Message {'id': ?})-[:hasCreator]->(p:Person) " +
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

            String stmt = "MATCH (m:Message {'id': ?})-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f:Forum)-[:hasModerator]->(mod:Person) " +
                    "RETURN " +
                    "  f.id::int8 AS forumId, " +
                    "  f.title AS forumTitle, " +
                    "  mod.id::int8 AS moderatorId, " +
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

            String stmt = "MATCH (m:Message {'id': ?})<-[:replyOf]-(c:\"Comment\")-[:hasCreator]->(p:Person) " +
                    "OPTIONAL MATCH (m)-[:hasCreator]->(a:Person)-[r:knows]-(p) " +
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
            client.execute(stmt, prop);

            stmt =  "MATCH (p:Person {'id': ?}), (c:Place {'id': ?}) " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE ? @> array[t.id::int8] " +
                    "WITH p, c, array_remove(array_agg(t), NULL) AS tags " +
                    "CREATE (p)-[:isLocatedIn]->(c) " +
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

            String stmt = "MATCH (p:Person {'id': ?}), (m:Post {'id': ?}) " +
                    "CREATE (p)-[:likes {'creationDate': ?}]->(m)";

            client.execute(stmt,
                    ldbcUpdate2AddPostLike.personId(),
                    ldbcUpdate2AddPostLike.postId(),
                    ldbcUpdate2AddPostLike.creationDate());

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

            String stmt = "MATCH (p:Person {'id': ?}), (m:\"Comment\" {'id': ?}) " +
                    "CREATE (p)-[:likes {'creationDate': ?}]->(m)";

            client.execute(stmt,
                    ldbcUpdate3AddCommentLike.personId(),
                    ldbcUpdate3AddCommentLike.commentId(),
                    ldbcUpdate3AddCommentLike.creationDate());

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

            stmt = "MATCH (f:Forum {'id': ?}), (p:Person {'id': ?}) " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE ? @> array[t.id::int8] " +
                    "WITH f, p, array_remove(array_agg(t), NULL) as tags " +
                    "CREATE (f)-[:hasModerator]->(p) " +
                    "WITH f, unnest(tags) AS tag " +
                    "CREATE (f)-[:hasTag]->(tag)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate4AddForum.tagIds());
            client.execute(stmt, ldbcUpdate4AddForum.forumId(), ldbcUpdate4AddForum.moderatorPersonId(), tagIds);

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

            String stmt = "MATCH (f:Forum {'id': ?}), (p:Person {'id': ?}) " +
                    "CREATE (f)-[:hasMember {'joinDate': ?}]->(p)";

            client.execute(stmt,
                    ldbcUpdate5AddForumMembership.forumId(),
                    ldbcUpdate5AddForumMembership.personId(),
                    ldbcUpdate5AddForumMembership.joinDate());

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
            client.execute(stmt, prop);

            stmt = "MATCH (m:Post {'id': ?}), " +
                    "      (p:Person {'id': ?}), " +
                    "      (f:Forum {'id': ?}), " +
                    "      (c:Place {'id': ?}) " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE ? @> array[t.id::int8] " +
                    "WITH m, p, f, c, array_remove(array_agg(t), NULL) as tagSet " +
                    "CREATE (m)-[:hasCreator]->(p), " +
                    "       (m)<-[:containerOf]-(f), " +
                    "       (m)-[:isLocatedIn]->(c) " +
                    "WITH m, unnest(tagSet) AS tag " +
                    "CREATE (m)-[:hasTag]->(tag)";
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate6AddPost.tagIds());
            client.execute(stmt, ldbcUpdate6AddPost.postId(), ldbcUpdate6AddPost.authorPersonId(),
                    ldbcUpdate6AddPost.forumId(), ldbcUpdate6AddPost.countryId(), tagIds);

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
            client.execute(stmt, prop);

            stmt = "MATCH (m:\"Comment\" {'id': ?}), " +
                    "      (p:Person {'id': ?}), " +
                    "      (r:Message {'id': ?}), " +
                    "      (c:Place {'id': ?}) " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE ? @> array[t.id::int8] " +
                    "WITH m, p, r, c, array_remove(array_agg(t), NULL) as tagSet " +
                    "CREATE (m)-[:hasCreator]->(p), " +
                    "       (m)-[:replyOf]->(r), " +
                    "       (m)-[:isLocatedIn]->(c) " +
                    "WITH m, unnest(tagSet) AS tag " +
                    "CREATE (m)-[:hasTag]->(tag)";
            Long replyOfId;
            if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
                replyOfId = ldbcUpdate7AddComment.replyToCommentId();
            } else {
                replyOfId = ldbcUpdate7AddComment.replyToPostId();
            }
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate7AddComment.tagIds());
            client.execute(stmt, ldbcUpdate7AddComment.commentId(), ldbcUpdate7AddComment.authorPersonId(),
                    replyOfId, ldbcUpdate7AddComment.countryId(), tagIds);

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

            String stmt = "MATCH (p1:Person {'id': ?}), (p2:Person {'id': ?}) " +
                    "CREATE (p1)-[:knows {'creationDate': ?}]->(p2)";

            client.execute(stmt,
                    ldbcUpdate8AddFriendship.person1Id(),
                    ldbcUpdate8AddFriendship.person2Id(),
                    ldbcUpdate8AddFriendship.creationDate());

            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
        }
    }
}
