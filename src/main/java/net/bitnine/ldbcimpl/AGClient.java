package net.bitnine.ldbcimpl;

import net.bitnine.agensgraph.util.Jsonb;
import net.bitnine.agensgraph.util.JsonbUtil;
import net.bitnine.ldbcimpl.excpetions.AGClientException;

import java.sql.*;
import java.util.InputMismatchException;
import java.util.List;

/**
 * Created by ktlee on 16. 10. 11.
 */
public class AGClient {

    private Connection        conn;
	private PreparedStatement stmtLongQuery1;
	private PreparedStatement stmtLongQuery2;
	private PreparedStatement stmtLongQuery3;
	private PreparedStatement stmtLongQuery4;
	private PreparedStatement stmtLongQuery5;
	private PreparedStatement stmtLongQuery6;
	private PreparedStatement stmtLongQuery7;
	private PreparedStatement stmtLongQuery8;
	private PreparedStatement stmtLongQuery9;
	private PreparedStatement stmtLongQuery10;
	private PreparedStatement stmtLongQuery11;
	private PreparedStatement stmtLongQuery12;
	private PreparedStatement stmtLongQuery13;
	private PreparedStatement stmtLongQuery14;
	private PreparedStatement stmtShortQuery1;
	private PreparedStatement stmtShortQuery2;
	private PreparedStatement stmtShortQuery3;
	private PreparedStatement stmtShortQuery4;
	private PreparedStatement stmtShortQuery5;
	private PreparedStatement stmtShortQuery6;
	private PreparedStatement stmtShortQuery7;
	private PreparedStatement stmtUpdateQuery1_0;
	private PreparedStatement stmtUpdateQuery1_1;
	private PreparedStatement stmtUpdateQuery2;
	private PreparedStatement stmtUpdateQuery3;
	private PreparedStatement stmtUpdateQuery4_0;
	private PreparedStatement stmtUpdateQuery4_1;
	private PreparedStatement stmtUpdateQuery5;
	private PreparedStatement stmtUpdateQuery6_0;
	private PreparedStatement stmtUpdateQuery6_1;
	private PreparedStatement stmtUpdateQuery7_0;
	private PreparedStatement stmtUpdateQuery7_1;
	private PreparedStatement stmtUpdateQuery8;
	
    public AGClient(String connStr, String user, String password) {
        try {
            Class.forName("net.bitnine.agensgraph.Driver");
        } catch (ClassNotFoundException e) {
            throw new AGClientException(e);
        }
        try {
            conn = DriverManager.getConnection(connStr, user, password);
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute("set graph_path = ldbc");
            stmt.execute("commit");
            stmt.close();
			prepareLongQueries();
			prepareShortQueries();
			prepareUpdateQueries();
        } catch (SQLException e) {
            throw new AGClientException(e);
        }
    }
	
    ResultSet executeQuery(String query, Object ... params) {
        ResultSet rs;

        try {
            if (params == null) {
                Statement stmt = conn.createStatement();
                rs = stmt.executeQuery(query);
            } else {
                PreparedStatement pstmt = conn.prepareStatement(query);
                bind(pstmt, params);
                rs = pstmt.executeQuery();
            }
        } catch (Exception e) {
            throw new AGClientException(e);
        }

        return rs;
    }

    void execute(String sql, Object ... params) {
        try {
            if (params == null) {
                Statement stmt = conn.createStatement();
                stmt.executeUpdate(sql);
            } else {
                PreparedStatement pstmt = conn.prepareStatement(sql);
                bind(pstmt, params);
                pstmt.executeUpdate();
            }
        } catch (Exception e) {
            throw new AGClientException(e);
        }
    }

    private void bind(PreparedStatement pstmt, Object ... params) throws SQLException {
        int i = 1;
        for (Object param : params) {
            if (param instanceof Long) {
                pstmt.setLong(i, (Long) param);
            } else if (param instanceof java.util.Date) {
                pstmt.setLong(i, ((java.util.Date) param).getTime());
            } else if (param instanceof Integer) {
                pstmt.setInt(i, (Integer) param);
            } else if (param instanceof Jsonb) {
                pstmt.setObject(i, param);
            } else if (param instanceof Array) {
                pstmt.setArray(i, (Array)param);
            } else {
                pstmt.setString(i, (String)param);
            }
            ++i;
        }
    }

    Array createArrayOfLong(String typeName, List<Long> l) {
        Long[] array = new Long[l.size()];
        l.toArray(array);
        try {
            return conn.createArrayOf(typeName, array);
        } catch (SQLException e) {
            throw new AGClientException(e);
        }
    }

    void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void close() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            conn = null;
        }
    }
	
	void prepareLongQueries() {
		prepareLongQuery1();
		prepareLongQuery2();
		prepareLongQuery3();
		prepareLongQuery4();
		prepareLongQuery5();
		prepareLongQuery6();
		prepareLongQuery7();
		prepareLongQuery8();
		prepareLongQuery9();
		prepareLongQuery10();
		prepareLongQuery11();
		prepareLongQuery12();
		prepareLongQuery13();
		prepareLongQuery14();
	}
	
	void prepareShortQueries() {
		prepareShortQuery1();
		prepareShortQuery2();
		prepareShortQuery3();
		prepareShortQuery4();
		prepareShortQuery5();
		prepareShortQuery6();
		prepareShortQuery7();
	}
	
	void prepareUpdateQueries() {
		prepareUpdateQuery1();
		prepareUpdateQuery2();
		prepareUpdateQuery3();
		prepareUpdateQuery4();
		prepareUpdateQuery5();
		prepareUpdateQuery6();
		prepareUpdateQuery7();
		prepareUpdateQuery8();
	}
	
	void prepareLongQuery1() {
		
        try {
			String stmt = "MATCH (p:Person)-[path:knows*1..3]->(friend:Person {'firstname': ?}) " +
                          "WHERE p.id = ? " + 
                          "WITH friend, min(length(path)) AS distance " +
                          "ORDER BY distance ASC, friend.lastName ASC, friend.id ASC " +
                          "LIMIT ? " +
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
			stmtLongQuery1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery2() {
		
        try {
			String stmt = "MATCH (p:Person)-[:knows]->(friend:Person)<-[:hasCreator]-(message:Message) " +
                          "WHERE p.id = ? AND message.creationDate <= ? " +
			              "WITH friend, message " +
			              "ORDER BY message.creationDate DESC, message.id ASC LIMIT ? " +
                          "RETURN " +
                          "  friend.id AS personId, " +
                          "  friend.firstName AS personFirstName, " +
                          "  friend.lastName AS personLastName, " +
                          "  message.id AS messageId, " +
                          "  COALESCE(message.content, message.imageFile), " +
                          "  message.creationDate AS messageCreationDate";
			stmtLongQuery2 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery3() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*1..2]->(friend:Person) " +
		                  "WHERE person.id = ? AND id(person) != id(friend) " +
		                  "WITH DISTINCT friend " +
	                      "MATCH (friend)<-[:hasCreator]-(messageX:Message)-[:isLocatedInMsg]->(countryX:Place {name: ?}) " +
		                  "WHERE not exists ((friend)-[:isLocatedInPerson]->()-[:isPartOf]->(countryX)) " +
		                  "  AND messageX.creationDate >= ? " +
		                  "  AND messageX.creationDate < ? " +
		                  "WITH friend, count(DISTINCT messageX) AS xCount " +
		                  "MATCH (friend)<-[:hasCreator]-(messageY:Message)-[:isLocatedInMsg]->(countryY:Place {name: ?}) " +
		                  "WHERE not exists ((friend)-[:isLocatedInPerson]->()-[:isPartOf]->(countryY)) " +
		                  "  AND messageY.creationDate >= ? " +
		                  "  AND messageY.creationDate < ? " +
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
		                  "LIMIT ?";
			stmtLongQuery3 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery4() {
		
        try {
			String stmt = "SELECT tagname, count(distinct postid) AS postcount FROM ( " +
			              "  SELECT T1.tag AS tagname, T1.postid AS postid, count(distinct T2.postid) AS oldPostCount " +
			              "  FROM " +
			              "  ( " +
			              "    MATCH (person:Person)-[:KNOWS]->()<-[:hasCreatorPost]-(post:Post)-[:hasTagPost]->(tag:Tag) " +
			              "    WHERE " +
			              "    person.id = ?" +
			              "    AND post.creationDate >= ? AND post.creationDate < ?" +
			              "    RETURN " +
			              "    post.id AS postid, tag.name AS tag " +
			              "  ) T1 " +
			              "  LEFT JOIN " +
			              "  ( " +
			              "    MATCH (person:Person)-[:KNOWS]->()<-[:hasCreatorPost]-(post:Post)-[:hasTagPost]->(tag:Tag) " +
			              "    WHERE " +
			              "    person.id = ?" +
			              "    AND post.creationDate < ?" +
			              "    RETURN " +
			              "    post.id AS postid, tag.name AS tag " +
			              "  ) T2 " +
			              "  ON T1.tag = T2.tag " +
			              "  GROUP BY T1.tag, T1.postid " +
			              ") A " +
			              "WHERE oldPostCount = 0 GROUP BY tagname " +
			              "ORDER BY postcount DESC, tagname ASC " +
			              "LIMIT ?";
			stmtLongQuery4 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery5() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*1..2]->(friend:Person) " +
                          "WHERE person.id = ? AND id(person) != id(friend) " +
			              "WITH DISTINCT friend " +
			              "MATCH (friend)<-[membership:hasMember]-(forum:Forum) " +
                          "WHERE membership.\"joinDate\" > ? " +
                          "OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post:Post)<-[:containerOf]-(forum) " +
			              "WITH forum.id AS forumid, forum.title AS forumTitle, count(id(post)) AS postcount " +
			              "ORDER BY postCount DESC, forumid ASC " +
			              "RETURN forumTitle, postCount " +
                          "LIMIT ?";
			stmtLongQuery5 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery6() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*1..2]->(friend:Person) " +
                          "WHERE person.id = ? AND id(person) != id(friend) " +
			              "WITH DISTINCT friend " +
                          "MATCH (friend)<-[:hascreatorpost]-(friendPost:Post) " +
			              "MATCH (friendPost)-[:hasTagPost]->(:Tag {name: ?}) " +
			              "MATCH (friendPost)-[:hasTagPost]->(commonTag:Tag) " +
                          "WHERE commonTag.name <> ? " +
                          "RETURN " +
                          "  commonTag.name AS tagName, " +
                          "  count(distinct id(friendPost)) AS postCount " +
                          "ORDER BY postCount DESC, tagName ASC " +
                          "LIMIT ?";
			stmtLongQuery6 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery7() {
		
        try {
			String stmt = "MATCH (person:Person)<-[:hasCreator]-(message:Message)<-[l:likes]-(liker:Person) " +
			              "WHERE person.id = ? " +
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
			              "LIMIT ?";
			stmtLongQuery7 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery8() {
		
        try {
			String stmt = "MATCH (p:Person)<-[:hasCreator]-()<-[:replyOf]-(c:\"Comment\")-[:hasCreatorComment]->(person:Person) " +
                          "WHERE p.id = ? " +
				          "WITH person, c " +
			              "ORDER BY c.creationDate DESC, c.id ASC LIMIT ? " +
                          "RETURN " +
                          "  person.id AS personId, " +
                          "  person.firstName AS personFirstName, " +
                          "  person.lastName AS personLastName, " +
                          "  c.creationDate AS commentCreationDate, " +
                          "  c.id AS commentId, " +
                          "  c.content AS commentContent";
			stmtLongQuery8 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery9() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*1..2]->(friend:Person) " +
			              "WHERE person.id = ? " +
			              "WITH DISTINCT friend " +
			              "MATCH (friend)<-[:hasCreator]-(message:Message) " +
			              "WHERE message.creationDate < ? " +
			              "WITH friend, message " +
			              "ORDER BY message.creationDate DESC, message.id ASC LIMIT ? " +
			              "RETURN " +
			              "  friend.id AS personId, " +
			              "  friend.firstName AS personFirstName, " +
			              "  friend.lastName AS personLastName, " +
			              "  message.id AS messageId, " +
			              "  COALESCE(message.content, message.imageFile), " +
			              "  message.creationDate AS messageCreationDate";
			stmtLongQuery9 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery10() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*2..2]->(friend:Person) " +
			              "WHERE person.id = ? AND " +
			              "((to_jsonb(date_part(right(left(\"varchar\"('month'),\"int4\"(6)),\"int4\"(5)), to_timestamp(int8(friend.birthday / 1000)))) = ? AND " +
			              "to_jsonb(date_part(right(left(\"varchar\"('month'),\"int4\"(6)),\"int4\"(5)), to_timestamp(int8(friend.birthday / 1000)))) >= 21) OR " +
			              "(to_jsonb(date_part(right(left(\"varchar\"('month'),\"int4\"(6)),\"int4\"(5)), to_timestamp(int8(friend.birthday / 1000)))) = (? % 12)+1 AND " +
			              "to_jsonb(date_part(right(left(\"varchar\"('month'),\"int4\"(6)),\"int4\"(5)), to_timestamp(int8(friend.birthday / 1000)))) < 22)) AND " +
			              "id(friend) != id(person) AND not exists((friend)-[:knows]->(person)) " +
			              "WITH DISTINCT person, friend " +
			              "MATCH (friend)-[:isLocatedInPerson]->(city:Place) " +
			              "OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post:Post) " +
			              "WITH   person, friend, city.name AS personCityName, post, " +
			              "CASE WHEN exists((post)-[:hasTagPost]->()<-[:hasInterest]-(person)) THEN 1 ELSE 0 END AS common " +
			              "WITH   friend, personCityName, count(distinct id(post)) AS postCount, sum(common) AS commonPostCount " +
			              "RETURN friend.id AS personId, friend.firstName AS personFirstName, " +
			              "friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, " +
			              "friend.gender AS personGender, personCityName ORDER BY commonInterestScore DESC, personId ASC LIMIT ?";
			stmtLongQuery10 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery11() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows*1..2]->(friend:Person) " +
                          "WHERE person.id = ? AND id(person) != id(friend) " +
                          "WITH DISTINCT friend " +
                          "MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedInOrgan]->(:Place {name: ?}) " +
                          "WHERE worksAt.\"workFrom\" < ? " +
                          "RETURN " +
                          "  friend.id AS friendId, " +
                          "  friend.firstName AS friendFirstName, " +
                          "  friend.lastName AS friendLastName, " +
                          "  company.name AS companyName, " +
                          "  worksAt.\"workFrom\" AS workFromYear " +
                          "ORDER BY workFromYear ASC, friendId ASC, companyName DESC " +
                          "LIMIT ?";
			stmtLongQuery11 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery12() {
		
        try {
			String stmt = "MATCH (person:Person)-[:knows]->(friend:Person) " +
                          "WHERE person.id = ? " +
                          "OPTIONAL MATCH " +
                          "  (friend)<-[:hasCreatorComment]-(c:\"Comment\")-[:replyOfPost]->()-[:hasTagPost]->(tag:Tag), " +
                          "  (tag:Tag)-[:hasType]->(tagClass:TagClass)-[:isSubclassOf*0..]->(baseTagClass:TagClass) " +
                          "WHERE tagClass.name = ? OR baseTagClass.name = ? " +
                          "RETURN " +
                          "  friend.id AS friendId, " +
                          "  friend.firstName AS friendFirstName, " +
                          "  friend.lastName AS friendLastName, " +
                          "  collect(DISTINCT tag.name) AS tagNames, " +
                          "  count(DISTINCT id(c)) AS count " +
                          "ORDER BY count DESC, friendId ASC " +
                          "LIMIT ?";
			stmtLongQuery12 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery13() {
		
        try {
			String stmt = "MATCH (person1:Person), (person2:Person) " +
                          "WHERE person1.id = ? AND person2.id = ? " +
                          "OPTIONAL MATCH path = shortestpath((person1)-[:knows*..15]-(person2)) " +
                          "RETURN " +
                          "CASE path IS NULL " +
                          "  WHEN true THEN -1 " +
                          "  ELSE length(path) " +
                          "END AS pathLength";
			stmtLongQuery13 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareLongQuery14() {
		
        try {
			String stmt = "MATCH (person1:Person), (person2:Person) " +
                          "WHERE person1.id = ? AND person2.id = ? " +
                          "OPTIONAL MATCH path = allshortestpaths( (person1)-[:knows*..15]-(person2) ) " +
                          "RETURN extract_ids2(nodes(path)) AS pathNodes, " +
                          "get_weight2(nodes(path)) AS weight " +
                          "ORDER BY weight DESC";
			stmtLongQuery14 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery1() {
		
        try {
			String stmt = "MATCH (r:Person)-[:isLocatedInPerson]->(s:Place) " +
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
			stmtShortQuery1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery2() {
		
        try {
			String stmt = "MATCH (person:Person)<-[:hasCreator]-(m:message)" +
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
			stmtShortQuery2 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery3() {
		
        try {
			String stmt = "MATCH (person:Person)-[r:knows]->(friend:Person) " +
						  "WHERE person.id = ? " +
						  "RETURN " +
						  "  friend.id AS friendId, " +
						  "  friend.firstName AS firstName, " +
						  "  friend.lastName AS lastName," +
						  "  r.\"creationDate\" AS friendshipCreationDate " +
						  " ORDER BY friendshipCreationDate DESC, friendId ASC";
			stmtShortQuery3 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery4() {
		
        try {
			String stmt = "MATCH (m:Message) " +
						  "WHERE m.id = ? " +
						  "RETURN " +
						  "  COALESCE(m.content, m.imageFile), " +
						  "  m.creationDate as creationDate";
			stmtShortQuery4 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery5() {
		
        try {
			String stmt = "MATCH (m:Message)-[:hasCreator]->(p:Person) " +
						  "WHERE m.id = ? " +
						  "RETURN " +
						  "  p.id AS personId, " +
						  "  p.firstName AS firstName, " +
						  "  p.lastName AS lastName";
			stmtShortQuery5 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery6() {
		
        try {
			String stmt = "MATCH (m:Message)-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f:Forum)-[:hasModerator]->(mod:Person) " +
						  "WHERE m.id = ? " +
						  "RETURN " +
						  "  f.id AS forumId, " +
						  "  f.title AS forumTitle, " +
						  "  mod.id AS moderatorId, " +
						  "  mod.firstName AS moderatorFirstName, " +
						  "  mod.lastName AS moderatorLastName";
			stmtShortQuery6 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareShortQuery7() {
		
        try {
			String stmt = "MATCH (m:Message)<-[:replyOf]-(c:\"Comment\")-[:hasCreatorComment]->(p:Person) " +
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
			stmtShortQuery7 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery1() {
		
        try {
			String stmt = "create (:Person ?)";
			stmtUpdateQuery1_0 = conn.prepareStatement( stmt );
			stmt = "MATCH (p:Person), (c:Place) " +
                   "WHERE p.id = ? AND c.id = ? " +
                   "OPTIONAL MATCH (t:Tag) " +
                   "WHERE t.id IN ? " +
                   "WITH p, c, array_agg(t) AS tags " +
                   "CREATE (p)-[:isLocatedInPerson]->(c) " +
                   "WITH p, unnest(tags) AS tag " +
                   "CREATE (p)-[:hasInterest]->(tag)";
			stmtUpdateQuery1_1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery2() {
		
        try {
			String stmt = "MATCH (p:Person), (m:Post) " +
			              "WHERE p.id = ? AND m.id = ? " +
			              "CREATE (p)-[:likesPost {'creationDate': ?}]->(m)";
			stmtUpdateQuery2 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery3() {
		
        try {
			String stmt = "MATCH (p:Person), (m:\"Comment\") " +
			              "WHERE p.id = ? AND m.id = ? " +
                          "CREATE (p)-[:likesComment {'creationDate': ?}]->(m)";
			stmtUpdateQuery3 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery4() {
		
        try {
			String stmt = "CREATE (f:Forum {id: ?, title: ?, creationDate: ?})";
			stmtUpdateQuery4_0 = conn.prepareStatement( stmt );
			stmt = "MATCH (f:Forum), (p:Person) " +
                   "WHERE f.id = ? AND p.id = ? " +
                   "OPTIONAL MATCH (t:Tag) " +
                   "WHERE t.id IN ? " +
                   "WITH f, p, array_agg(t) as tags " +
                   "CREATE (f)-[:hasModerator]->(p) " +
                   "WITH f, unnest(tags) AS tag " +
                   "CREATE (f)-[:hasTagForum]->(tag)";
			stmtUpdateQuery4_1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery5() {
		
        try {
			String stmt = "MATCH (f:Forum), (p:Person) " +
                          "WHERE f.id = ? AND p.id = ? " +
                          "CREATE (f)-[:hasMember {'joinDate': ?}]->(p)";
			stmtUpdateQuery5 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery6() {
		
        try {
			String stmt = "CREATE (:Post ?)";
			stmtUpdateQuery6_0 = conn.prepareStatement( stmt );
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
			stmtUpdateQuery6_1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery7() {
		
        try {
			String stmt = "CREATE (:\"Comment\" ?)";
			stmtUpdateQuery7_0 = conn.prepareStatement( stmt );
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
			stmtUpdateQuery7_1 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void prepareUpdateQuery8() {
		
        try {
			String stmt = "MATCH (p1:Person), (p2:Person) " +
                          "WHERE p1.id = ? AND p2.id = ? " +
                          "CREATE (p1)-[:knows {'creationDate': ?}]->(p2) " +
                          "CREATE (p2)-[:knows {'creationDate': ?}]->(p1)";
			stmtUpdateQuery8 = conn.prepareStatement( stmt );
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery1( Object ... params ) {
		
		try {
			bind( stmtLongQuery1, params );
			return stmtLongQuery1.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery2( Object ... params ) {
		
		try {
			bind( stmtLongQuery2, params );
			return stmtLongQuery2.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery3( Object ... params ) {
		
		try {
			bind( stmtLongQuery3, params );
			return stmtLongQuery3.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery4( Object ... params ) {
		
		try {
			bind( stmtLongQuery4, params );
			return stmtLongQuery4.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery5( Object ... params ) {
		
		try {
			bind( stmtLongQuery5, params );
			return stmtLongQuery5.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery6( Object ... params ) {
		
		try {
			bind( stmtLongQuery6, params );
			return stmtLongQuery6.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery7( Object ... params ) {
		
		try {
			bind( stmtLongQuery7, params );
			return stmtLongQuery7.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery8( Object ... params ) {
		
		try {
			bind( stmtLongQuery8, params );
			return stmtLongQuery8.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery9( Object ... params ) {
		
		try {
			bind( stmtLongQuery9, params );
			return stmtLongQuery9.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery10( Object ... params ) {
		
		try {
			bind( stmtLongQuery10, params );
			return stmtLongQuery10.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery11( Object ... params ) {
		
		try {
			bind( stmtLongQuery11, params );
			return stmtLongQuery11.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery12( Object ... params ) {
		
		try {
			bind( stmtLongQuery12, params );
			return stmtLongQuery12.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery13( Object ... params ) {
		
		try {
			bind( stmtLongQuery13, params );
			return stmtLongQuery13.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeLongQuery14( Object ... params ) {
		
		try {
			bind( stmtLongQuery14, params );
			return stmtLongQuery14.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery1( Object ... params ) {
		
		try {
			bind( stmtShortQuery1, params );
			return stmtShortQuery1.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery2( Object ... params ) {
		
		try {
			bind( stmtShortQuery2, params );
			return stmtShortQuery2.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery3( Object ... params ) {
		
		try {
			bind( stmtShortQuery3, params );
			return stmtShortQuery3.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery4( Object ... params ) {
		
		try {
			bind( stmtShortQuery4, params );
			return stmtShortQuery4.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery5( Object ... params ) {
		
		try {
			bind( stmtShortQuery5, params );
			return stmtShortQuery5.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery6( Object ... params ) {
		
		try {
			bind( stmtShortQuery6, params );
			return stmtShortQuery6.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	ResultSet executeShortQuery7( Object ... params ) {
		
		try {
			bind( stmtShortQuery7, params );
			return stmtShortQuery7.executeQuery();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery1_0( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery1_0, params );
			stmtUpdateQuery1_0.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery1_1( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery1_1, params );
			stmtUpdateQuery1_1.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery2( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery2, params );
			stmtUpdateQuery2.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery3( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery3, params );
			stmtUpdateQuery3.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery4_0( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery4_0, params );
			stmtUpdateQuery4_0.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery4_1( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery4_1, params );
			stmtUpdateQuery4_1.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery5( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery5, params );
			stmtUpdateQuery5.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery6_0( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery6_0, params );
			stmtUpdateQuery6_0.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery6_1( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery6_1, params );
			stmtUpdateQuery6_1.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery7_0( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery7_0, params );
			stmtUpdateQuery7_0.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery7_1( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery7_1, params );
			stmtUpdateQuery7_1.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
	
	void executeUpdateQuery8( Object ... params ) {
		
		try {
			bind( stmtUpdateQuery8, params );
			stmtUpdateQuery8.executeUpdate();
        } catch (Exception e) {
            throw new AGClientException(e);
        }
		
	}
}
