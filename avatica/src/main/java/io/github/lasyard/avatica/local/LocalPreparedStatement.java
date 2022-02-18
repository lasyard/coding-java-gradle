package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;

// `AvaticaPreparedStatement` is abstract, so there must be a class.
final class LocalPreparedStatement extends AvaticaPreparedStatement {
    LocalPreparedStatement(
        LocalConnection connection,
        Meta.StatementHandle handle,
        Meta.Signature signature,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        super(
            connection,
            handle,
            signature,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }
}
