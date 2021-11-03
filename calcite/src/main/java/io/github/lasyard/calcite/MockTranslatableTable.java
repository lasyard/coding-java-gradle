package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;

import java.lang.reflect.Type;
import javax.annotation.Nonnull;

@Slf4j
// Must also be queryable.
public class MockTranslatableTable extends MockTable implements QueryableTable, TranslatableTable {
    @Override
    public RelNode toRel(@Nonnull RelOptTable.ToRelContext context, @Nonnull RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        log.info("table = {}", relOptTable.getQualifiedName());
        return new MockTableScan(cluster, context.getTableHints(), relOptTable);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(
        SchemaPlus schema,
        String tableName,
        Class clazz
    ) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {
        log.info("scan() called.");
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new MockEnumerator();
            }
        };
    }
}
