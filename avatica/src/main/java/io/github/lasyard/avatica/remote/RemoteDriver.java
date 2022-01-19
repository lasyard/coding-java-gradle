package io.github.lasyard.avatica.remote;

import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.remote.Driver;

public class RemoteDriver extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:xxx:thin";

    static final DriverVersion DRIVER_VERSION = new DriverVersion(
        "XXX JDBC Thin Driver",
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
        new RemoteDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DRIVER_VERSION;
    }
}
