package io.github.lasyard.calcite;

import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

@RequiredArgsConstructor
public class MockSchema extends AbstractSchema {
    private final TableFlavor flavor;

    @Override
    protected Map<String, Table> getTableMap() {
        MockTable table;
        switch (flavor) {
            case SCANNABLE:
            default:
                table = new MockScannableTable();
                break;
            case FILTERABLE:
                table = new MockFilterableTable();
                break;
            case PROJECTABLE_FILTERABLE:
                table = new MockProjectableFilterableTable();
                break;
            case TRANSLATABLE:
                table = new MockTranslatableTable();
                break;
        }
        return ImmutableMap.<String, Table>builder()
            .put("mock", table)
            .build();
    }
}
