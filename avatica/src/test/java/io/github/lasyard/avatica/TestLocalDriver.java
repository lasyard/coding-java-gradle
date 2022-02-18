package io.github.lasyard.avatica;

import io.github.lasyard.avatica.local.LocalDriver;
import io.github.lasyard.avatica.local.LocalMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalDriver {
    private static Connection connection = null;

    @BeforeAll
    public static void setupAll() throws ClassNotFoundException, SQLException {
        Class.forName("io.github.lasyard.avatica.local.LocalDriver");
        connection = DriverManager.getConnection(LocalDriver.CONNECT_STRING_PREFIX);
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        connection.close();
    }

    @Test
    public void testQuery() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("select 'whatever'")) {
                while (resultSet.next()) {
                    assertThat(resultSet.getString(1)).isEqualTo(LocalMeta.TEST_STRING);
                }
            }
        }
    }

    @Test
    public void testQuery1() throws SQLException {
        String sql = "select name from mock where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 1);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    assertThat(resultSet.getString(1)).isEqualTo(LocalMeta.TEST_STRING);
                }
            }
        }
    }
}
