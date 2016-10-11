package net.bitnine.ldbcimpl;

import net.bitnine.ldbcimpl.excpetions.AGClientException;

import java.sql.*;

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
        } catch (SQLException e) {
            throw new AGClientException(e);
        }
    }

    ResultSet execute(String query, Object ... params) {
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

    void bind(PreparedStatement pstmt, Object ... params) throws SQLException {
        int i = 0;
        for (Object param : params) {
            if (param instanceof Integer) {
                pstmt.setInt(i, (Integer)param);
            } else {
                pstmt.setString(i, (String)param);
            }
            ++i;
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
