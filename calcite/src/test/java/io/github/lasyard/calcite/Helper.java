package io.github.lasyard.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public final class Helper {
    private Helper() {
    }

    public static Connection getConnection(String modelFileName) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties props = new Properties();
        props.put("model", Objects.requireNonNull(Helper.class.getResource(modelFileName)).getPath());
        return DriverManager.getConnection("jdbc:calcite:", props);
    }
}
