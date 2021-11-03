package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
public class MockTableScan extends TableScan implements EnumerableRel {
    protected MockTableScan(
        RelOptCluster cluster,
        List<RelHint> hints,
        RelOptTable table
    ) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), hints, table);
    }

    @Override
    public void register(@Nonnull RelOptPlanner planner) {
        planner.addRule(MockTableScanRule.INSTANCE);
    }

    @Override
    public Result implement(@Nonnull EnumerableRelImplementor implementor, @Nonnull Prefer pref) {
        PhysType physType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray()
        );
        final Expression expression = table.getExpression(MockTranslatableTable.class);
        assert expression != null;
        return implementor.result(
            physType,
            Blocks.toBlock(
                Expressions.call(expression, "scan",
                    implementor.getRootExpression())
            )
        );
    }
}
