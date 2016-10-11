package net.bitnine.ldbcimpl;

import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import net.bitnine.ldbcimpl.excpetions.AGClientException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by ktlee on 16. 10. 10.
 */
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
                    + properties.get("database");
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

        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
    }

    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String query = "" +
                    "MATCH (r:Person {id: ?})-[:isLocatedIn]->(s:Place) " +
                    "RETURN " +
                    "  r.firstName AS first_name, " +
                    "  r.lastName AS last_name, " +
                    "  r.birthday::date AS birthday, " +
                    "  r.locationIP AS location_ip, " +
                    "  r.browserUsed AS browser_used, " +
                    "  s.id::int8 AS place_id, " +
                    "  r.gender AS gender, " +
                    "  r.creationDate::timestamp AS creation_date";

            ResultSet rs = client.execute(query, ldbcShortQuery1PersonProfile.personId());

            LdbcShortQuery1PersonProfileResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery1PersonProfileResult(
                            rs.getString("first_name"), rs.getString("last_name"),
                            rs.getTimestamp("birthday").getTime(), rs.getString("location_ip"),
                            rs.getString("browser_used"), rs.getLong("place_id"),
                            rs.getString("gender"), rs.getTimestamp("creation_date").getTime());
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery1PersonProfile);
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler
            implements OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends ldbcShortQuery3PersonFriends, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String query =
                    "MATCH (:Person {id: ?})-[r:knows]->(friend) " +
                    "RETURN " +
                    "  friend.id::int8 AS friend_id, " +
                    "  friend.firstName AS first_name, " +
                    "  friend.lastName AS last_name," +
                    "  r.creationDate::timestamp AS friendship_creation_date " +
                    " ORDER BY friendship_creation_date DESC, friend_id ASC";

            ResultSet rs = client.execute(query, ldbcShortQuery3PersonFriends.personId());

            List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
            try {
                while (rs.next()) {
                    result.add(new LdbcShortQuery3PersonFriendsResult(
                            rs.getLong("friend_id"), rs.getString("first_name"),
                            rs.getString("last_name"), rs.getTimestamp("friendship_creation_date").getTime())
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery3PersonFriends);
        }
    }

    public static class LdbcShortQuery4MessageContentHandler
            implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            // FIXME merge Post and Comment
            String query = "MATCH (m:Post {id: ?}) " +
                    "RETURN " +
                    "  m.creationDate as creation_date, " +
                    "  CASE m.content is not null " +
                    "    WHEN true THEN m.content " +
                    "    ELSE m.imageFile " +
                    "   END AS content";

            ResultSet rs = client.execute(query, ldbcShortQuery4MessageContent.messageId());
            LdbcShortQuery4MessageContentResult result = null;

            try {
                if (rs.next()) {
                    result = new LdbcShortQuery4MessageContentResult(
                            rs.getString("content"), rs.getTimestamp("creation_date").getTime()
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery4MessageContent);
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler
            implements OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator ldbcShortQuery5MessageCreator, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            // FIXME merge Post and Comment
            String query = "MATCH (m:Post {id: ?})-[:hasCreator]->(p:Person) " +
                    "RETURN " +
                    "  p.id AS person_id, " +
                    "  p.firstName AS first_name, " +
                    "  p.lastName AS last_name";

            ResultSet rs = client.execute(query, ldbcShortQuery5MessageCreator.messageId());
            LdbcShortQuery5MessageCreatorResult result = null;

            try {
                if (rs.next()) {
                    result = new LdbcShortQuery5MessageCreatorResult(
                            rs.getLong("person_id"), rs.getString("first_name"), rs.getString("last_name")
                    );
                }
            } catch (SQLException e) {
                throw new AGClientException(e);
            }

            resultReporter.report(0, result, ldbcShortQuery5MessageCreator);
        }
    }

    /*
    public static class LdbcShortQuery7MessageRepliesHandler
            implements OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            AGClient client = ((AGDbConnectionState)dbConnectionState).getClent();

            String query = "MATCH (m:Post {id: ?})<-[:replyOf]-(c:Comment)-[:hasCreator]->(p:Person)"
                    + " OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)"
                    + " RETURN"
                    + "   c.id AS commentId,"
                    + "   c.content AS commentContent,"
                    + "   c.creationDate AS commentCreationDate,"
                    + "   p.id AS replyAuthorId,"
                    + "   p.firstName AS replyAuthorFirstName,"
                    + "   p.lastName AS replyAuthorLastName,"
                    + "   CASE r"
                    + "     WHEN null THEN false"
                    + "     ELSE true"
                    + "   END AS replyAuthorKnowsOriginalMessageAuthor"
                    + " ORDER BY commentCreationDate DESC, toInt(replyAuthorId) ASC";
        }
    }
    */
}
