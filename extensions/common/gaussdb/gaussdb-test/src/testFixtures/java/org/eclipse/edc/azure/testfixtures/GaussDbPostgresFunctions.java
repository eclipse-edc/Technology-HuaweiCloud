package org.eclipse.edc.azure.testfixtures;

import com.huawei.gauss200.jdbc.ds.PGPoolingDataSource;
import com.huawei.gauss200.jdbc.util.PSQLException;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import javax.sql.DataSource;

import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;

public class GaussDbPostgresFunctions {
    private static final String PG_CONNECTION_STRING = "PG_CONNECTION_STRING";

    private static final int TIMEOUT_SEC = 100;

    @NotNull
    private static DataSource createDatasource(String connectionString) {
        var ds = new PGPoolingDataSource();
        try {
            ds.setURL(connectionString);
            ds.setInitialConnections(20);
            ds.setMaxConnections(100);
            ds.setConnectTimeout(TIMEOUT_SEC);
            ds.setSocketTimeout(TIMEOUT_SEC);
            ds.setLoginTimeout(TIMEOUT_SEC);
            ds.setCancelSignalTimeout(TIMEOUT_SEC);
        } catch (PSQLException e) {
            throw new RuntimeException(e);
        }
        return ds;
    }


    public static DataSource createDataSource() {
        var connectionString = propOrEnv(PG_CONNECTION_STRING, null);
        Objects.requireNonNull(connectionString, "GaussDB Postgres connection string not found");
        return createDatasource(connectionString);
    }
}
