package io.github.lasyard.calcite;

import org.apache.calcite.linq4j.Enumerator;

public class MockEnumerator implements Enumerator<Object[]> {
    private final Object[][] datum = {
        {1, "Alice"},
        {2, "Betty"},
    };

    private int pos = -1;

    @Override
    public Object[] current() {
        return datum[pos];
    }

    @Override
    public boolean moveNext() {
        pos++;
        return pos < datum.length;
    }

    @Override
    public void reset() {
        pos = -1;
    }

    @Override
    public void close() {
    }
}
