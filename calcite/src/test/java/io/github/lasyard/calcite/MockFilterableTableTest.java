package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MockFilterableTableTest {
    private static Connection connection = null;

    @BeforeAll
    public static void setupAll() throws SQLException, ClassNotFoundException {
        connection = Helper.getConnection("/models/mock_filterable_table.yml");
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        connection.close();
    }

    @Test
    public void testScan() throws SQLException {
        // Names are converted to uppercase if not quoted.
        String sql = "select * from \"mock\"";
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)
        ) {
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            while (resultSet.next()) {
                log.info("id = {}, name = {}",
                    resultSet.getInt("id"),
                    resultSet.getString("name")
                );
            }
        }
    }

    @Test
    public void testProject() throws SQLException {
        // Names are converted to uppercase if not quoted.
        String sql = "select \"name\" from \"mock\"";
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)
        ) {
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            while (resultSet.next()) {
                log.info("name = {}", resultSet.getString("name"));
            }
        }
    }

    @Test
    public void testFilter() throws SQLException {
        // Names are converted to uppercase if not quoted.
        String sql = "select * from \"mock\" where \"name\" = 'Alice'";
        try (
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)
        ) {
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            while (resultSet.next()) {
                log.info("id = {}, name = {}",
                    resultSet.getInt("id"),
                    resultSet.getString("name")
                );
            }
        }
    }
}
