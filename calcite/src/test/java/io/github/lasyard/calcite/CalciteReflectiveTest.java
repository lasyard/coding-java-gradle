package io.github.lasyard.calcite;

import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class CalciteReflectiveTest {
    private static MusicSchema musicSchema = null;

    @BeforeAll
    public static void setupAll() throws ClassNotFoundException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        musicSchema = new MusicSchema(
            new Artist[]{
                new Artist(1, "Alice"),
                new Artist(2, "Betty"),
            },
            new Album[]{
                new Album(1, 1, "Sun"),
                new Album(2, 1, "Moon"),
                new Album(3, 2, "Love"),
            }
        );
    }

    @Nonnull
    private List<Map<String, Object>> queryMapResultSet(String sql, String... cols) throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        List<Map<String, Object>> result = new LinkedList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Schema schema = new ReflectiveSchema(musicSchema);
            rootSchema.add("m", schema);
            try (PreparedStatement statement = calciteConnection.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (String col : cols) {
                            row.put(col, resultSet.getObject(resultSet.findColumn(col)));
                        }
                        result.add(row);
                    }
                }
            }
        }
        return result;
    }

    @Test
    public void testSelect() throws SQLException {
        List<Map<String, Object>> result = queryMapResultSet(
            "select * from m.artists",
            "id", "name"
        );
        assertThat(result)
            .extracting("id", "name")
            .containsExactlyInAnyOrder(
                tuple(1, "Alice"),
                tuple(2, "Betty")
            );
    }

    @Test
    public void testJoin() throws SQLException {
        List<Map<String, Object>> result = queryMapResultSet(
            "select t.id, tt.name, t.title from m.albums t join m.artists tt on t.artist = tt.id",
            "id", "name", "title"
        );
        assertThat(result)
            .extracting("id", "name", "title")
            .containsExactlyInAnyOrder(
                tuple(1, "Alice", "Sun"),
                tuple(2, "Alice", "Moon"),
                tuple(3, "Betty", "Love")
            );
    }

    // Fields in the schema classes must be `public` and the class must be `public`.
    @RequiredArgsConstructor
    public static class Artist {
        public final Integer id;
        public final String name;
    }

    @RequiredArgsConstructor
    public static class Album {
        public final Integer id;
        public final Integer artist;
        public final String title;
    }

    @RequiredArgsConstructor
    public static class MusicSchema {
        public final Artist[] artists;
        public final Album[] albums;
    }
}
