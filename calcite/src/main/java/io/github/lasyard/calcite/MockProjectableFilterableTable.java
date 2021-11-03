package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class MockProjectableFilterableTable extends MockTable implements ProjectableFilterableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext, List<RexNode> list, final int[] integers) {
        log.info("scan() called.");
        log.info("columns = {}", integers);
        if (integers == null) {
            // It does not matter that filtering is not implemented here
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new MockEnumerator();
                }
            };
        }
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                // Projecting is a must.
                return new MockEnumerator() {
                    @Override
                    public Object[] current() {
                        final Object[] result = super.current();
                        return Arrays.stream(integers).mapToObj(i -> result[i]).toArray();
                    }
                };
            }
        };
    }
}
