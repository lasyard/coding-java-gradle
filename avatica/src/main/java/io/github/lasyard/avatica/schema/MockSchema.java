package io.github.lasyard.avatica.schema;

import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

@RequiredArgsConstructor
public class MockSchema extends AbstractSchema {
    @Override
    protected Map<String, Table> getTableMap() {
        MockTable table = new MockTable();
        return ImmutableMap.<String, Table>builder()
            .put("MOCK", table)
            .build();
    }
}
