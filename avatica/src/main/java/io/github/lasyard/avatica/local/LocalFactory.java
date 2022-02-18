package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class LocalFactory implements AvaticaFactory {
    public static final LocalFactory INSTANCE = new LocalFactory();

    @Override
    public int getJdbcMajorVersion() {
        return 4;
    }

    @Override
    public int getJdbcMinorVersion() {
        return 1;
    }

    @Override
    public LocalConnection newConnection(
        UnregisteredDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        return new LocalConnection(
            (LocalDriver) driver,
            factory,
            url,
            info
        );
    }

    @Override
    public AvaticaStatement newStatement(
        AvaticaConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new LocalStatement(
            (LocalConnection) connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    @Override
    public LocalPreparedStatement newPreparedStatement(
        AvaticaConnection connection,
        Meta.StatementHandle handle,
        Meta.Signature signature,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new LocalPreparedStatement(
            (LocalConnection) connection,
            handle,
            signature,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    @Override
    public AvaticaResultSet newResultSet(
        AvaticaStatement statement,
        QueryState state,
        Meta.Signature signature,
        TimeZone timeZone,
        Meta.Frame firstFrame
    ) throws SQLException {
        final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);
        return new AvaticaResultSet(
            statement,
            state,
            signature,
            metaData,
            timeZone,
            firstFrame
        );
    }

    @Override
    public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
        return new LocalDatabaseMetaData(connection);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(
        AvaticaStatement statement,
        Meta.Signature signature
    ) {
        return new AvaticaResultSetMetaData(statement, null, signature);
    }
}
