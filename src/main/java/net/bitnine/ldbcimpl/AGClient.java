package net.bitnine.ldbcimpl;

import net.bitnine.agensgraph.graph.property.JsonObject;
import net.bitnine.agensgraph.graph.property.Jsonb;
import net.bitnine.ldbcimpl.excpetions.AGClientException;

import java.sql.*;
import java.util.InputMismatchException;
import java.util.List;

/**
 * Created by ktlee on 16. 10. 11.
 */
public class AGClient {

    private Connection conn;

    public AGClient(String connStr, String user, String password) {
        try {
            Class.forName("net.bitnine.agensgraph.Driver");
        } catch (ClassNotFoundException e) {
            throw new AGClientException(e);
        }
        try {
            conn = DriverManager.getConnection(connStr, user, password);
            Statement stmt = conn.createStatement();
            stmt.execute("set graph_path = ldbc");
            stmt.execute("commit");
            stmt.close();
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
            } else if (param instanceof JsonObject) {
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

    void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            conn = null;
        }
    }
}
