package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;

// `AvaticaStatement` is abstract, so there must be a class.
final class LocalStatement extends AvaticaStatement {
    LocalStatement(
        LocalConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        super(
            connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }
}
