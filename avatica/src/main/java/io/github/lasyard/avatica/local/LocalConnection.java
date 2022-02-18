package io.github.lasyard.avatica.local;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;

import java.util.Properties;

public class LocalConnection extends AvaticaConnection {
    protected LocalConnection(
        LocalDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        super(driver, factory, url, info);
    }
}
