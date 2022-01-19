package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;
import java.util.Properties;
import javax.annotation.Nonnull;

public class LocalConnection extends AvaticaConnection {
    protected LocalConnection(
        LocalDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        super(driver, factory, url, info);
    }

    public LocalStatement getStatement(@Nonnull Meta.StatementHandle sh) throws SQLException {
        // `lookupStatement` is protected.
        return (LocalStatement) lookupStatement(sh);
    }
}
