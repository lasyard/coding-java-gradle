package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;

import java.util.List;

@Slf4j
public class MockFilterableTable extends MockTable implements FilterableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        log.info("scan() called.");
        // It does not matter that filtering is not implemented here
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new MockEnumerator();
            }
        };
    }
}
