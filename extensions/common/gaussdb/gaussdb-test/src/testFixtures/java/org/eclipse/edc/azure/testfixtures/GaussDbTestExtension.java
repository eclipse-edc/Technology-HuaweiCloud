package org.eclipse.edc.azure.testfixtures;

import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.sql.SqlQueryExecutor;
import org.eclipse.edc.transaction.datasource.spi.DataSourceRegistry;
import org.eclipse.edc.transaction.datasource.spi.DefaultDataSourceRegistry;
import org.eclipse.edc.transaction.spi.NoopTransactionContext;
import org.eclipse.edc.transaction.spi.TransactionContext;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;

import static org.eclipse.edc.azure.testfixtures.GaussDbPostgresFunctions.createDataSource;

public class GaussDbTestExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, ParameterResolver {

    public static final String DEFAULT_DATASOURCE_NAME = "test-datasource";
    private final QueryExecutor queryExecutor = new SqlQueryExecutor();
    private final TransactionContext transactionContext = new NoopTransactionContext();
    private DataSource dataSource;
    private DataSourceRegistry registry;
    private Connection connection;

    @Override
    public void beforeAll(ExtensionContext context) {
        dataSource = createDataSource();
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        var type = parameterContext.getParameter().getParameterizedType();
        return List.of(GaussDbTestExtension.class, QueryExecutor.class, DataSource.class, DataSourceRegistry.class, TransactionContext.class, SqlHelper.class).contains(type);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        var type = parameterContext.getParameter().getParameterizedType();
        if (type.equals(GaussDbTestExtension.class)) {
            return this;
        } else if (type.equals(QueryExecutor.class)) {
            return queryExecutor;
        } else if (type.equals(TransactionContext.class)) {
            return transactionContext;
        } else if (type.equals(DataSource.class)) {
            return dataSource;
        } else if (type.equals(SqlHelper.class)) {
            return new SqlHelper();
        } else if (type.equals(DataSourceRegistry.class)) {
            return registry;
        }
        return null;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        registry = new DefaultDataSourceRegistry();
        registry.register(DEFAULT_DATASOURCE_NAME, dataSource);
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * This class provides helper methods for executing SQL statements and handling database operations.
     */
    public class SqlHelper {

        /**
         * Executes a database statement with the given SQL statement.
         *
         * @param schema the SQL statement
         */
        public void executeStatement(String schema) {
            transactionContext.execute(() -> queryExecutor.execute(connection, schema));
        }

        /**
         * Drops a table from the database with the given table name.
         *
         * @param tableName the name of the table to be dropped
         */
        public void dropTable(String tableName) {
            executeStatement("DROP TABLE " + tableName + " CASCADE");
        }

        /**
         * Truncates (empties) a table in the database with the given table name.
         *
         * @param tableName the name of the table to be truncated
         */
        public void truncateTable(String tableName) {
            executeStatement("TRUNCATE TABLE " + tableName + " CASCADE");
        }

    }
}
