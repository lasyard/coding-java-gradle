package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.UnregisteredDriver;

public class LocalDriver extends UnregisteredDriver {
    public static final LocalDriver INSTANCE = new LocalDriver();

    public static final String CONNECT_STRING_PREFIX = "jdbc:xxx:";

    static final DriverVersion DRIVER_VERSION = new DriverVersion(
        "XXX JDBC Driver",
        "1.0.0",
        "xxx",
        "1.0.0",
        true,
        1,
        0,
        1,
        0
    );

    static {
        new LocalDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        return LocalFactory.class.getCanonicalName();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DRIVER_VERSION;
    }

    @Override
    public LocalMeta createMeta(AvaticaConnection connection) {
        return new LocalMeta((LocalConnection) connection);
    }
}
