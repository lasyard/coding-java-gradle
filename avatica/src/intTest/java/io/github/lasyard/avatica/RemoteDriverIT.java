package io.github.lasyard.avatica;

import io.github.lasyard.avatica.local.LocalMeta;
import io.github.lasyard.avatica.remote.RemoteDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDriverIT {
    private static Connection connection = null;

    @BeforeAll
    public static void setupAll() throws ClassNotFoundException, SQLException {
        Class.forName("io.github.lasyard.avatica.remote.RemoteDriver");
        connection = DriverManager.getConnection(
            RemoteDriver.CONNECT_STRING_PREFIX + "url=http://localhost:8765"
        );
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        connection.close();
    }

    @Test
    public void testConnection() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("select 'whatever'")) {
                while (resultSet.next()) {
                    assertThat(resultSet.getString(1)).isEqualTo(LocalMeta.TEST_STRING);
                }
            }
        }
    }
}
