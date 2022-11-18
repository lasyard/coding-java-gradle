package io.github.lasyard.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import javax.annotation.Nonnull;

public abstract class MockTable extends AbstractTable {
    public static final int PRECISION = 128;

    @Override
    public RelDataType getRowType(@Nonnull RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            Arrays.asList(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR, PRECISION),
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
            ),
            Arrays.asList(
                "id",
                "name",
                "amount"
            )
        );
    }
}
