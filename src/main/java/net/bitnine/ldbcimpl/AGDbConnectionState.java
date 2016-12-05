package net.bitnine.ldbcimpl;

import com.ldbc.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ktlee on 16. 12. 5.
 */
public class AGDbConnectionState extends DbConnectionState {

    private ThreadLocal<AGClient> client;

    public AGDbConnectionState(Map<String, String> properties) {
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
        client = ThreadLocal.withInitial(() -> {
            return new AGClient(connStr, properties.get("user"), properties.get("password"));
        });
    }

    AGClient getClent() {
        return client.get();
    }

    @Override
    public void close() throws IOException {
        client.get().close();
    }
}
