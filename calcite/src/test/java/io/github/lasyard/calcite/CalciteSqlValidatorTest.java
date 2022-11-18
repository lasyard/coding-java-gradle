package io.github.lasyard.calcite;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class CalciteSqlValidatorTest {
    private static SqlValidator validator = null;

    @BeforeAll
    public static void setupAll() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        // Create CatalogReader.
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            CalciteSchema.createRootSchema(
                true,
                false,
                "",
                new MockSchema(TableFlavor.SCANNABLE)
            ),
            ImmutableList.of("mock"),
            typeFactory,
            null
        );
        validator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        );
    }

    @AfterAll
    public static void cleanUpAll() {
    }

    @Test
    public void testInsert() throws SqlParseException {
        // Names are converted to uppercase if not quoted.
        String sql = "insert into \"mock\" values"
            + "(1, 'Alice', 1.0),"
            + "(2, 'Betty', null),"
            + "(3, 'Cindy', 3.0)";
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        log.debug("sqlNode = {}", sqlNode);
        sqlNode = validator.validate(sqlNode);
        log.debug("sqlNode = {}", sqlNode);
    }
}
