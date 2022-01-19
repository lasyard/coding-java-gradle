package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

// Must inherit, the constructor of the base class is protected.
class LocalDatabaseMetaData extends AvaticaDatabaseMetaData {
    LocalDatabaseMetaData(AvaticaConnection connection) {
        super(connection);
    }
}
