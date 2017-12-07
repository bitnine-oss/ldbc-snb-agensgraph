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
            
            ResultSet rs = client.executeLongQuery1( ldbcQuery1.firstName(),
													 ldbcQuery1.personId(),
													 ldbcQuery1.limit() );
            
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

            ResultSet rs = client.executeLongQuery2( ldbcQuery2.personId(),
													 ldbcQuery2.maxDate().getTime(),
													 ldbcQuery2.limit() );
			
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
	    	ResultSet rs = client.executeLongQuery3( ldbcQuery3.personId(),
													 ldbcQuery3.countryXName(),
													 ldbcQuery3.startDate().getTime(),
													 endDate.getTime(),
													 ldbcQuery3.countryYName(),
													 ldbcQuery3.startDate().getTime(),
													 endDate.getTime(),
													 ldbcQuery3.limit() );

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
            ResultSet rs = client.executeLongQuery4( ldbcQuery4.personId(),
													 ldbcQuery4.startDate().getTime(),
													 endDate.getTime(),
													 ldbcQuery4.personId(),
													 ldbcQuery4.startDate().getTime(),
													 ldbcQuery4.limit() );

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

            ResultSet rs = client.executeLongQuery5( ldbcQuery5.personId(),
													 ldbcQuery5.minDate().getTime(),
													 ldbcQuery5.limit() );

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

            ResultSet rs = client.executeLongQuery6( ldbcQuery6.personId(),
													 ldbcQuery6.tagName(),
													 ldbcQuery6.tagName(),
													 ldbcQuery6.limit() );

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

            ResultSet rs = client.executeLongQuery7( ldbcQuery7.personId(),
													 ldbcQuery7.limit() );

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

            ResultSet rs = client.executeLongQuery8( ldbcQuery8.personId(),
													 ldbcQuery8.limit() );

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

            ResultSet rs = client.executeLongQuery9( ldbcQuery9.personId(),
													 ldbcQuery9.maxDate().getTime(),
													 ldbcQuery9.limit() );

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

            ResultSet rs = client.executeLongQuery10( ldbcQuery10.personId(),
													  ldbcQuery10.month(),
													  ldbcQuery10.month(),
													  ldbcQuery10.limit() );

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

            ResultSet rs = client.executeLongQuery11( ldbcQuery11.personId(),
													ldbcQuery11.countryName(),
													ldbcQuery11.workFromYear(),
													ldbcQuery11.limit() );

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

            ResultSet rs = client.executeLongQuery12( ldbcQuery12.personId(),
													  ldbcQuery12.tagClassName(),
													  ldbcQuery12.tagClassName(),
													  ldbcQuery12.limit() );

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

            ResultSet rs = client.executeLongQuery13( ldbcQuery13.person1Id(),
													  ldbcQuery13.person2Id() );

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
            ResultSet rs = client.executeLongQuery14( ldbcQuery14.person1Id(),
													  ldbcQuery14.person2Id() );

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

			ResultSet rs = client.executeShortQuery1( ldbcShortQuery1PersonProfile.personId() );

            LdbcShortQuery1PersonProfileResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery1PersonProfileResult(
                            rs.getString(1), rs.getString(2),
                            rs.getLong(3), rs.getString(4),
                            rs.getString(5), rs.getLong(6),
                            rs.getString(7), rs.getLong(8));
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery2( ldbcShortQuery2PersonPosts.personId(),
													  ldbcShortQuery2PersonPosts.limit(),
													  ldbcShortQuery2PersonPosts.limit() );

            List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcShortQuery2PersonPostsResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getLong(5),
                            rs.getString(6), rs.getString(7)
                    ));
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery3( ldbcShortQuery3PersonFriends.personId() );

            List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
            try {
                while (rs.next()) {
                    result.add(new LdbcShortQuery3PersonFriendsResult(
                            rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4))
                    );
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery4( ldbcShortQuery4MessageContent.messageId() );

            LdbcShortQuery4MessageContentResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery4MessageContentResult(
                            rs.getString(1), rs.getLong(2)
                    );
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery5( ldbcShortQuery5MessageCreator.messageId() );
            LdbcShortQuery5MessageCreatorResult result = null;

            try {
                if (rs.next()) {
                    result = new LdbcShortQuery5MessageCreatorResult(
                            rs.getLong(1), rs.getString(2), rs.getString(3)
                    );
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery6( ldbcShortQuery6MessageForum.messageId() );

            LdbcShortQuery6MessageForumResult result = null;
            try {
                if (rs.next()) {
                    result = new LdbcShortQuery6MessageForumResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3),
                            rs.getString(4), rs.getString(5)
                    );
                }
				rs.close();
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

            ResultSet rs = client.executeShortQuery7( ldbcShortQuery7MessageReplies.messageId() );

            List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<>();
            try {
                while (rs.next()) {
                    resultList.add(new LdbcShortQuery7MessageRepliesResult(
                            rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getString(5),
                            rs.getString(6), rs.getBoolean(7))
                    );
                }
				rs.close();
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

            String stmt = "";
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
            client.executeUpdateQuery1_0( prop );

            stmt =  "MATCH (p:Person), (c:Place) " +
                    "WHERE p.id = ? AND c.id = ? " +
                    "OPTIONAL MATCH (t:Tag) " +
                    "WHERE t.id IN ? " +
                    "WITH p, c, array_agg(t) AS tags " +
                    "CREATE (p)-[:isLocatedInPerson]->(c) " +
                    "WITH p, unnest(tags) AS tag " +
                    "CREATE (p)-[:hasInterest]->(tag)";

            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate1AddPerson.tagIds());
            client.executeUpdateQuery1_1( ldbcUpdate1AddPerson.personId(),
										  ldbcUpdate1AddPerson.cityId(),
										  tagIds );

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

            client.executeUpdateQuery2( ldbcUpdate2AddPostLike.personId(),
										ldbcUpdate2AddPostLike.postId(),
										ldbcUpdate2AddPostLike.creationDate() );

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

            client.executeUpdateQuery3( ldbcUpdate3AddCommentLike.personId(),
										ldbcUpdate3AddCommentLike.commentId(),
										ldbcUpdate3AddCommentLike.creationDate() );

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

            client.executeUpdateQuery4_0( ldbcUpdate4AddForum.forumId(),
										  ldbcUpdate4AddForum.forumTitle(),
										  ldbcUpdate4AddForum.creationDate() );

            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate4AddForum.tagIds());
            client.executeUpdateQuery4_1( ldbcUpdate4AddForum.forumId(),
										  ldbcUpdate4AddForum.moderatorPersonId(),
										  tagIds );

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

            client.executeUpdateQuery5( ldbcUpdate5AddForumMembership.forumId(),
										ldbcUpdate5AddForumMembership.personId(),
										ldbcUpdate5AddForumMembership.joinDate() );

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
            client.executeUpdateQuery6_0( prop.build() );

            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate6AddPost.tagIds());
            client.executeUpdateQuery6_1( ldbcUpdate6AddPost.postId(),
										  ldbcUpdate6AddPost.authorPersonId(),
										  ldbcUpdate6AddPost.forumId(),
										  ldbcUpdate6AddPost.countryId(),
										  tagIds );

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

            Jsonb prop = JsonbUtil.createObjectBuilder()
                                  .add("id",           ldbcUpdate7AddComment.commentId())
                                  .add("creationdate", ldbcUpdate7AddComment.creationDate().getTime())
                                  .add("locationip",   ldbcUpdate7AddComment.locationIp())
                                  .add("browserused",  ldbcUpdate7AddComment.browserUsed())
                                  .add("content",      ldbcUpdate7AddComment.content())
                                  .add("length",       ldbcUpdate7AddComment.length())
                                  .build();
            client.executeUpdateQuery7_0( prop );

            Long replyOfId;
            if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
                replyOfId = ldbcUpdate7AddComment.replyToCommentId();
            } else {
                replyOfId = ldbcUpdate7AddComment.replyToPostId();
            }
            Array tagIds = client.createArrayOfLong("int8", ldbcUpdate7AddComment.tagIds());
            client.executeUpdateQuery7_1( ldbcUpdate7AddComment.commentId(),
										  ldbcUpdate7AddComment.authorPersonId(),
										  replyOfId,
										  ldbcUpdate7AddComment.countryId(),
										  tagIds );

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

            client.executeUpdateQuery8( ldbcUpdate8AddFriendship.person1Id(),
										ldbcUpdate8AddFriendship.person2Id(),
										ldbcUpdate8AddFriendship.creationDate(),
										ldbcUpdate8AddFriendship.creationDate() );

            client.commit();
            resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
        }
    }
}
