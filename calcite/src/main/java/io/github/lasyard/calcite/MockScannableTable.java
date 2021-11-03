package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;

@Slf4j
public class MockScannableTable extends MockTable implements ScannableTable {
    @Override
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
