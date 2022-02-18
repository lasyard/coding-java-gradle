package io.github.lasyard.avatica.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import javax.annotation.Nonnull;

@Slf4j
public class MockTable extends AbstractTable {
    public static final int PRECISION = 128;

    @Override
    public RelDataType getRowType(@Nonnull RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            Arrays.asList(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR, PRECISION)
            ),
            Arrays.asList(
                "ID",
                "NAME"
            )
        );
    }
}
